"""Exclusive recovery coordination for WhatsApp session reset and pairing."""

from __future__ import annotations

import json
import logging
import os
import re
import stat
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Optional, cast

from gateway.status import (
    _get_scope_lock_path,
    _pid_exists,
    _release_file_lock,
    _scope_hash,
    _try_acquire_file_lock,
    acquire_scoped_lock,
    get_process_start_time,
    release_scoped_lock,
    terminate_pid,
)


logger = logging.getLogger(__name__)

WHATSAPP_RECOVERY_SCOPE = "whatsapp-recovery"
WHATSAPP_SESSION_SCOPE = "whatsapp-session"


class WhatsAppRecoveryError(RuntimeError):
    """Recovery could not establish exclusive ownership of the session."""


_state_lock = threading.Lock()
_local_guards: dict[str, threading.Lock] = {}
_local_session_holders: dict[str, int] = {}
_GATE_METADATA_VERSION = 1
_MAX_LOCK_METADATA_BYTES = 128 * 1024
_MAX_BRIDGE_PID_BYTES = 256


def _session_identity(session_path: Path) -> str:
    return str(Path(session_path).expanduser().resolve(strict=False))


def _local_guard(identity: str) -> threading.Lock:
    with _state_lock:
        return _local_guards.setdefault(identity, threading.Lock())


def note_whatsapp_session_lock_acquired(session_path: Path) -> None:
    """Record an adapter-owned session lock so same-process recovery waits."""
    identity = _session_identity(session_path)
    with _state_lock:
        _local_session_holders[identity] = _local_session_holders.get(identity, 0) + 1


def note_whatsapp_session_lock_released(session_path: Path) -> None:
    """Forget one adapter-owned session lock after the scoped lock is released."""
    identity = _session_identity(session_path)
    with _state_lock:
        count = _local_session_holders.get(identity, 0)
        if count <= 1:
            _local_session_holders.pop(identity, None)
        else:
            _local_session_holders[identity] = count - 1


def _local_session_is_held(identity: str) -> bool:
    with _state_lock:
        return _local_session_holders.get(identity, 0) > 0


def _gate_path(identity: str) -> Path:
    """Return a stable path not managed as a removable JSON scoped lock."""
    return _get_scope_lock_path(WHATSAPP_RECOVERY_SCOPE, identity).with_suffix(".gate")


def _open_lock_file(path: Path) -> tuple[IO[str], bool]:
    """Open *path* without following a final symlink; report if it was created."""
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | getattr(os, "O_NONBLOCK", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags | os.O_CREAT | os.O_EXCL | nofollow, 0o600)
        created = True
        existing_stat = None
    except FileExistsError:
        existing_stat = path.lstat()
        if not stat.S_ISREG(existing_stat.st_mode):
            raise OSError("WhatsApp coordination state is not a regular file")
        fd = os.open(path, flags | nofollow)
        created = False
    try:
        opened_stat = os.fstat(fd)
        if not stat.S_ISREG(opened_stat.st_mode) or (
            existing_stat is not None
            and (opened_stat.st_dev, opened_stat.st_ino)
            != (existing_stat.st_dev, existing_stat.st_ino)
        ):
            raise OSError("WhatsApp coordination state is not a regular file")
        try:
            os.fchmod(fd, 0o600)
        except (AttributeError, OSError):
            pass
        return os.fdopen(fd, "r+", encoding="utf-8"), created
    except Exception:
        os.close(fd)
        raise


def _valid_gate_metadata(value: object, identity: str) -> bool:
    if not isinstance(value, dict):
        return False
    record = cast(dict[str, object], value)
    pid = record.get("pid")
    return (
        record.get("version") == _GATE_METADATA_VERSION
        and isinstance(pid, int)
        and not isinstance(pid, bool)
        and pid > 0
        and record.get("identity_hash") == _scope_hash(identity)
        and record.get("operation") in {"connect", "probe", "reset-pair"}
    )


def _read_gate_metadata(handle: IO[str], identity: str) -> bool:
    """Inspect stale diagnostics; the already-held OS lock remains authority."""
    try:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        if size <= 0 or size > _MAX_LOCK_METADATA_BYTES:
            return False
        handle.seek(0)
        value = json.load(handle)
    except (UnicodeError, json.JSONDecodeError):
        return False
    except OSError as exc:
        raise WhatsAppRecoveryError(
            "WhatsApp recovery coordination is unavailable."
        ) from exc
    return _valid_gate_metadata(value, identity)


def _write_gate_metadata(handle: IO[str], identity: str, operation: str) -> None:
    """Write bounded, content-free diagnostics while the OS lock is held."""
    record = {
        "version": _GATE_METADATA_VERSION,
        "pid": os.getpid(),
        "identity_hash": _scope_hash(identity),
        "operation": operation,
    }
    handle.seek(0)
    handle.truncate()
    json.dump(record, handle, separators=(",", ":"), sort_keys=True)
    handle.flush()
    try:
        os.fsync(handle.fileno())
    except OSError:
        pass


def _acquire_advisory_gate(identity: str, operation: str) -> Optional[IO[str]]:
    """Acquire the OS-owned gate before inspecting or initializing metadata."""
    try:
        handle, created = _open_lock_file(_gate_path(identity))
    except OSError as exc:
        raise WhatsAppRecoveryError(
            "WhatsApp recovery coordination is unavailable."
        ) from exc
    if not _try_acquire_file_lock(handle):
        handle.close()
        return None
    try:
        if not created:
            # Torn or stale diagnostic JSON is safe to replace now that this
            # process owns the advisory lock. Open/fstat failures above remain
            # fail-closed because they prevent proving a regular lock file.
            _read_gate_metadata(handle, identity)
        _write_gate_metadata(handle, identity, operation)
        return handle
    except Exception:
        _release_file_lock(handle)
        handle.close()
        raise


def _release_advisory_gate(handle: IO[str]) -> None:
    _release_file_lock(handle)
    try:
        handle.close()
    except OSError:
        pass


class WhatsAppConnectGate:
    """Short-lived OS-held gate while an adapter takes its session lock."""

    def __init__(self, handle: IO[str], guard: threading.Lock):
        self._handle = handle
        self._guard = guard
        self.active = True

    def release(self) -> None:
        if not self.active:
            return
        self.active = False
        try:
            _release_advisory_gate(self._handle)
        finally:
            self._guard.release()

    close = release


def acquire_whatsapp_connect_gate(
    session_path: Path,
) -> Optional[WhatsAppConnectGate]:
    """Take the recovery gate non-blockingly for adapter session-lock startup."""
    identity = _session_identity(session_path)
    guard = _local_guard(identity)
    if not guard.acquire(blocking=False):
        return None
    try:
        handle = _acquire_advisory_gate(identity, "connect")
        if handle is None:
            guard.release()
            return None
    except Exception:
        guard.release()
        raise
    return WhatsAppConnectGate(handle, guard)


def whatsapp_recovery_is_active(session_path: Path) -> bool:
    """Return whether recovery currently excludes this adapter session."""
    identity = _session_identity(session_path)
    guard = _local_guard(identity)
    if guard.locked():
        return True
    if not guard.acquire(blocking=False):
        return True
    try:
        handle = _acquire_advisory_gate(identity, "probe")
        if handle is None:
            return True
        _release_advisory_gate(handle)
        return False
    except Exception as exc:
        logger.debug(
            "Could not inspect the WhatsApp recovery gate (%s).",
            type(exc).__name__,
        )
        return True
    finally:
        guard.release()


def _listener_pids_on_port(port: int) -> Optional[list[int]]:
    """Return TCP listener PIDs, or ``None`` when the OS cannot prove them."""
    try:
        import psutil

        pids = {
            int(conn.pid)
            for conn in psutil.net_connections(kind="tcp")
            if conn.pid is not None
            and conn.status == psutil.CONN_LISTEN
            and conn.laddr
            and conn.laddr.port == port
        }
        return sorted(pids)
    except (OSError, ValueError):
        pass
    except Exception as exc:
        logger.debug(
            "Could not enumerate WhatsApp bridge listeners (%s).",
            type(exc).__name__,
        )
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["netstat", "-ano", "-p", "TCP"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        stdout = result.stdout if isinstance(result.stdout, str) else ""
        stderr = result.stderr if isinstance(result.stderr, str) else ""
        if result.returncode != 0 or stderr.strip():
            return None
        pids = []
        for line in stdout.splitlines():
            parts = line.split()
            if (
                len(parts) >= 5
                and parts[3] == "LISTENING"
                and parts[1].endswith(f":{port}")
            ):
                try:
                    pids.append(int(parts[4]))
                except ValueError:
                    continue
        return sorted(set(pids))

    try:
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
        )
    except FileNotFoundError:
        result = None
    except (OSError, subprocess.TimeoutExpired):
        return None
    else:
        stdout = result.stdout if isinstance(result.stdout, str) else ""
        stderr = result.stderr if isinstance(result.stderr, str) else ""
        if stderr.strip():
            return None
        if result.returncode == 1 and not stdout.strip():
            return []
        if result.returncode != 0:
            return None
        pids = []
        for line in stdout.splitlines():
            try:
                pids.append(int(line.strip()))
            except ValueError:
                return None
        return sorted(set(pids))

    try:
        result = subprocess.run(
            ["ss", "-ltnHp", f"sport = :{port}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    stdout = result.stdout if isinstance(result.stdout, str) else ""
    stderr = result.stderr if isinstance(result.stderr, str) else ""
    if result.returncode != 0 or stderr.strip():
        return None
    matches = {int(match) for match in re.findall(r"pid=(\d+)", stdout)}
    if stdout.strip() and not matches:
        return None
    return sorted(matches)


def _default_bridge_script() -> Path:
    from gateway.platforms.whatsapp_common import resolve_whatsapp_bridge_dir

    return resolve_whatsapp_bridge_dir() / "bridge.js"


def _resolved_path(value: object) -> Optional[Path]:
    if isinstance(value, str):
        raw_path = value
    elif isinstance(value, os.PathLike):
        raw_path = os.fspath(cast(os.PathLike[str], value))
    else:
        return None
    try:
        return Path(raw_path).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return None


def _single_cli_value(argv: list[str], option: str) -> Optional[str]:
    positions = [index for index, value in enumerate(argv) if value == option]
    if len(positions) != 1 or positions[0] + 1 >= len(argv):
        return None
    return argv[positions[0] + 1]


def _bridge_cmdline_matches_session(
    pid: int,
    session_path: Path,
    port: int,
    bridge_script: Optional[Path] = None,
) -> bool:
    """Prove the executable and complete bridge invocation identity."""
    try:
        import psutil

        process = psutil.Process(pid)
        executable_path = process.exe()
        argv = process.cmdline()
    except Exception:
        return False
    if len(argv) < 2:
        return False
    executable = Path(executable_path).name.casefold()
    argv_executable = Path(argv[0]).name.casefold()
    if executable not in {"node", "node.exe"} or argv_executable not in {
        "node",
        "node.exe",
    }:
        return False
    expected_script = _resolved_path(bridge_script or _default_bridge_script())
    actual_script = _resolved_path(argv[1])
    if expected_script is None or actual_script != expected_script:
        return False
    session_arg = _single_cli_value(argv, "--session")
    if session_arg is None:
        return False
    actual_session = _resolved_path(session_arg)
    expected_session = _resolved_path(session_path)
    if actual_session is None or actual_session != expected_session:
        return False
    port_arg = _single_cli_value(argv, "--port")
    try:
        if port_arg is None or int(port_arg) != int(port):
            return False
    except (TypeError, ValueError):
        return False
    return True


def bridge_process_is_exact_listener(
    pid: int,
    session_path: Path,
    port: int,
    bridge_script: Path,
    *,
    listener_pids: Optional[list[int]] = None,
) -> bool:
    """True only for the exact Node bridge invocation that owns LISTEN."""
    listeners = _listener_pids_on_port(port) if listener_pids is None else listener_pids
    if listeners is None or pid not in listeners:
        return False
    return _bridge_cmdline_matches_session(pid, session_path, port, bridge_script)


def _read_bounded_regular_text(
    path: Path, *, max_bytes: int, failure_message: str
) -> Optional[str]:
    """Read a small regular file without following or blocking on special files."""
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise WhatsAppRecoveryError(failure_message) from exc
    if not stat.S_ISREG(path_stat.st_mode):
        raise WhatsAppRecoveryError(failure_message)

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise WhatsAppRecoveryError(failure_message) from exc
    try:
        opened_stat = os.fstat(fd)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or (opened_stat.st_dev, opened_stat.st_ino)
            != (path_stat.st_dev, path_stat.st_ino)
            or opened_stat.st_size < 0
            or opened_stat.st_size > max_bytes
        ):
            raise WhatsAppRecoveryError(failure_message)
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(fd, min(4096, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        final_stat = os.fstat(fd)
        if (
            len(payload) > max_bytes
            or (final_stat.st_dev, final_stat.st_ino, final_stat.st_size)
            != (opened_stat.st_dev, opened_stat.st_ino, opened_stat.st_size)
            or len(payload) != final_stat.st_size
        ):
            raise WhatsAppRecoveryError(failure_message)
    except OSError as exc:
        raise WhatsAppRecoveryError(failure_message) from exc
    finally:
        os.close(fd)
    try:
        return payload.decode("utf-8")
    except UnicodeError as exc:
        raise WhatsAppRecoveryError(failure_message) from exc


def _read_recorded_bridge(session_path: Path) -> Optional[tuple[int, Optional[int]]]:
    pid_path = Path(session_path) / "bridge.pid"
    message = "The recorded WhatsApp bridge could not be verified."
    payload = _read_bounded_regular_text(
        pid_path,
        max_bytes=_MAX_BRIDGE_PID_BYTES,
        failure_message=message,
    )
    if payload is None:
        return None
    lines = payload.splitlines()
    try:
        if len(lines) not in {1, 2}:
            raise ValueError
        pid = int(lines[0].strip())
        start_time = int(lines[1].strip()) if len(lines) > 1 and lines[1].strip() else None
    except (IndexError, TypeError, ValueError) as exc:
        raise WhatsAppRecoveryError(message) from exc
    if pid <= 0 or pid == os.getpid():
        raise WhatsAppRecoveryError(message)
    return pid, start_time


def _wait_for_pid_exit(pid: int, deadline: float, poll_interval: float) -> bool:
    while time.monotonic() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(poll_interval)
    return not _pid_exists(pid)


def _stop_proven_bridge(pid: int, deadline: float, poll_interval: float) -> None:
    try:
        terminate_pid(pid, force=False)
    except ProcessLookupError:
        return
    except OSError as exc:
        raise WhatsAppRecoveryError("The WhatsApp bridge could not be stopped safely.") from exc

    remaining = max(0.0, deadline - time.monotonic())
    term_deadline = min(deadline, time.monotonic() + max(poll_interval, remaining / 2))
    if _wait_for_pid_exit(pid, term_deadline, poll_interval):
        return
    try:
        terminate_pid(pid, force=True)
    except ProcessLookupError:
        return
    except OSError as exc:
        raise WhatsAppRecoveryError("The WhatsApp bridge could not be stopped safely.") from exc
    if not _wait_for_pid_exit(pid, deadline, poll_interval):
        raise WhatsAppRecoveryError("The WhatsApp bridge did not stop in time.")


def _read_session_lock_record(identity: str) -> Optional[dict]:
    """Read existing session-lock state; only a genuine absence is empty."""
    lock_path = _get_scope_lock_path(WHATSAPP_SESSION_SCOPE, identity)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        fd = os.open(lock_path, flags)
    except FileNotFoundError as exc:
        try:
            lock_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as stat_exc:
            raise WhatsAppRecoveryError(
                "The WhatsApp session lock could not be verified."
            ) from stat_exc
        raise WhatsAppRecoveryError(
            "The WhatsApp session lock could not be verified."
        ) from exc
    except OSError as exc:
        raise WhatsAppRecoveryError(
            "The WhatsApp session lock could not be verified."
        ) from exc

    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise WhatsAppRecoveryError(
                "The WhatsApp session lock could not be verified."
            )
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            fd = -1
            payload = handle.read(_MAX_LOCK_METADATA_BYTES + 1)
    except (OSError, UnicodeError) as exc:
        raise WhatsAppRecoveryError(
            "The WhatsApp session lock could not be verified."
        ) from exc
    finally:
        if fd >= 0:
            os.close(fd)

    if not payload or len(payload) > _MAX_LOCK_METADATA_BYTES:
        raise WhatsAppRecoveryError("The WhatsApp session lock could not be verified.")
    try:
        record = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise WhatsAppRecoveryError(
            "The WhatsApp session lock could not be verified."
        ) from exc
    if not isinstance(record, dict):
        raise WhatsAppRecoveryError("The WhatsApp session lock could not be verified.")
    pid = record.get("pid")
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or record.get("scope") != WHATSAPP_SESSION_SCOPE
        or record.get("identity_hash") != _scope_hash(identity)
        or not isinstance(record.get("metadata"), dict)
    ):
        raise WhatsAppRecoveryError("The WhatsApp session lock could not be verified.")
    return record


def validate_whatsapp_session_lock_state(session_path: Path) -> None:
    """Fail closed if an on-disk session lock exists but cannot be trusted."""
    _read_session_lock_record(_session_identity(session_path))


def _quiesce_matching_bridge(
    session_path: Path,
    bridge_port: int,
    bridge_script: Path,
    deadline: float,
    poll_interval: float,
) -> None:
    recorded = _read_recorded_bridge(session_path)
    stopped_pid: Optional[int] = None
    if recorded is not None:
        pid, recorded_start = recorded
        if _pid_exists(pid):
            listeners = _listener_pids_on_port(bridge_port)
            if listeners is None:
                raise WhatsAppRecoveryError(
                    "The recorded WhatsApp bridge identity could not be verified."
                )
            if recorded_start is not None:
                current_start = get_process_start_time(pid)
                if current_start is None or current_start != recorded_start:
                    raise WhatsAppRecoveryError(
                        "The recorded WhatsApp bridge identity could not be verified."
                    )
            if listeners != [pid] or not bridge_process_is_exact_listener(
                pid,
                session_path,
                bridge_port,
                bridge_script,
                listener_pids=listeners,
            ):
                raise WhatsAppRecoveryError(
                    "The recorded WhatsApp bridge identity could not be verified."
                )
            _stop_proven_bridge(pid, deadline, poll_interval)
            stopped_pid = pid

    listeners = _listener_pids_on_port(bridge_port)
    if listeners is None:
        if stopped_pid is None:
            raise WhatsAppRecoveryError(
                "The WhatsApp bridge listener could not be checked safely."
            )
        return
    remaining = [pid for pid in listeners if pid != stopped_pid and _pid_exists(pid)]
    if not remaining:
        return
    if len(remaining) != 1 or not bridge_process_is_exact_listener(
        remaining[0],
        session_path,
        bridge_port,
        bridge_script,
        listener_pids=remaining,
    ):
        raise WhatsAppRecoveryError(
            "The process using the WhatsApp bridge port could not be verified."
        )
    _stop_proven_bridge(remaining[0], deadline, poll_interval)


_RESET_SAFETY_ERROR = "The WhatsApp session could not be reset safely."
_MAX_RESET_ENTRIES = 100_000
_MAX_RESET_DEPTH = 128


@dataclass(frozen=True)
class _DirectoryIdentity:
    """Stable identity and security metadata for one path component."""

    device: int
    inode: int
    file_type: int
    permissions: int
    uid: Optional[int]
    gid: Optional[int]
    flags: int


@dataclass(frozen=True)
class _ResetTreeEntry:
    """One completely preflighted entry below the bound session directory."""

    name: str
    stable_identity: _DirectoryIdentity
    observed_identity: tuple[int, ...]
    is_directory: bool
    children: tuple["_ResetTreeEntry", ...] = ()


@dataclass
class _SessionDirectoryBinding:
    """Open descriptors and identities captured while both locks are held."""

    requested_path: Path
    canonical_path: Path
    components: tuple[str, ...]
    chain: tuple[_DirectoryIdentity, ...]
    parent_fd: int
    session_fd: int

    @property
    def session_name(self) -> str:
        return self.components[-1]

    def close(self) -> None:
        for attribute in ("session_fd", "parent_fd"):
            fd = getattr(self, attribute)
            if fd < 0:
                continue
            setattr(self, attribute, -1)
            try:
                os.close(fd)
            except OSError:
                pass


def _directory_identity(path_stat: os.stat_result) -> _DirectoryIdentity:
    return _DirectoryIdentity(
        device=int(path_stat.st_dev),
        inode=int(path_stat.st_ino),
        file_type=stat.S_IFMT(path_stat.st_mode),
        permissions=stat.S_IMODE(path_stat.st_mode),
        uid=getattr(path_stat, "st_uid", None),
        gid=getattr(path_stat, "st_gid", None),
        flags=int(getattr(path_stat, "st_flags", 0)),
    )


def _observed_entry_identity(path_stat: os.stat_result) -> tuple[int, ...]:
    """Include mutable metadata for the no-mutation preflight recheck."""
    return (
        int(path_stat.st_dev),
        int(path_stat.st_ino),
        int(path_stat.st_mode),
        int(getattr(path_stat, "st_uid", -1)),
        int(getattr(path_stat, "st_gid", -1)),
        int(path_stat.st_size),
        int(getattr(path_stat, "st_mtime_ns", int(path_stat.st_mtime * 1e9))),
        int(getattr(path_stat, "st_ctime_ns", int(path_stat.st_ctime * 1e9))),
        int(getattr(path_stat, "st_flags", 0)),
    )


def _reset_primitives_available() -> bool:
    """Whether Python exposes a no-follow, descriptor-relative reset surface."""
    required_dir_fd = (os.open, os.stat, os.access, os.mkdir, os.unlink, os.rmdir)
    return (
        os.name == "posix"
        and bool(getattr(os, "O_DIRECTORY", 0))
        and bool(getattr(os, "O_NOFOLLOW", 0))
        and all(function in os.supports_dir_fd for function in required_dir_fd)
        and os.stat in os.supports_follow_symlinks
        and os.access in os.supports_effective_ids
        and hasattr(os, "fchmod")
    )


def _absolute_requested_path(session_path: Path) -> Path:
    try:
        return Path(os.path.abspath(os.fspath(Path(session_path).expanduser())))
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _open_directory_at(parent_fd: int, name: str) -> tuple[int, os.stat_result]:
    """Open one real directory entry and prove lstat/open identity agreement."""
    try:
        entry_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(entry_stat.st_mode):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        fd = os.open(name, _directory_open_flags(), dir_fd=parent_fd)
    except WhatsAppRecoveryError:
        raise
    except OSError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
    try:
        opened_stat = os.fstat(fd)
        if (
            not stat.S_ISDIR(opened_stat.st_mode)
            or _directory_identity(opened_stat) != _directory_identity(entry_stat)
        ):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        return fd, opened_stat
    except Exception:
        os.close(fd)
        raise


def _capture_session_directory_binding(
    requested_path: Path,
    canonical_path: Path,
) -> Optional[_SessionDirectoryBinding]:
    """Bind every component from the filesystem root to an existing session."""
    try:
        requested_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc

    # Pairing a new session remains available on platforms without these
    # primitives, but destructive reset cannot safely fall back to pathname
    # recursion there. An existing session therefore receives no reset binding
    # and ``reset_session`` will fail closed without touching it.
    if not _reset_primitives_available():
        return None
    if not canonical_path.is_absolute():
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
    anchor = canonical_path.anchor
    if not anchor:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
    try:
        components = tuple(canonical_path.relative_to(anchor).parts)
    except ValueError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
    if not components or any(part in {"", ".", ".."} for part in components):
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)

    opened_fds: list[int] = []
    try:
        root_fd = os.open(anchor, _directory_open_flags())
        opened_fds.append(root_fd)
        root_stat = os.fstat(root_fd)
        if not stat.S_ISDIR(root_stat.st_mode):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        chain = [_directory_identity(root_stat)]
        current_fd = root_fd
        for component in components:
            child_fd, child_stat = _open_directory_at(current_fd, component)
            opened_fds.append(child_fd)
            chain.append(_directory_identity(child_stat))
            current_fd = child_fd

        parent_fd, session_fd = opened_fds[-2:]
        for fd in opened_fds[:-2]:
            os.close(fd)
        opened_fds = []
        return _SessionDirectoryBinding(
            requested_path=requested_path,
            canonical_path=canonical_path,
            components=components,
            chain=tuple(chain),
            parent_fd=parent_fd,
            session_fd=session_fd,
        )
    except WhatsAppRecoveryError:
        raise
    except OSError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
    finally:
        for fd in opened_fds:
            try:
                os.close(fd)
            except OSError:
                pass


def _blocking_file_flags(path_stat: os.stat_result) -> bool:
    mask = 0
    for name in ("UF_IMMUTABLE", "UF_APPEND", "SF_IMMUTABLE", "SF_APPEND"):
        mask |= int(getattr(stat, name, 0))
    return bool(int(getattr(path_stat, "st_flags", 0)) & mask)


def _require_mutable_directory(fd: int, path_stat: os.stat_result) -> None:
    try:
        if (
            not stat.S_ISDIR(path_stat.st_mode)
            or _blocking_file_flags(path_stat)
            or not os.access(
                ".",
                os.W_OK | os.X_OK,
                dir_fd=fd,
                effective_ids=True,
            )
        ):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
    except WhatsAppRecoveryError:
        raise
    except OSError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc


def _scan_reset_tree(
    directory_fd: int,
    *,
    root_device: int,
    seen_directories: set[tuple[int, int]],
    entry_count: list[int],
    depth: int = 0,
) -> tuple[_ResetTreeEntry, ...]:
    """Preflight the complete tree before the first unlink or rmdir."""
    if depth > _MAX_RESET_DEPTH:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
    try:
        names = sorted(os.listdir(directory_fd))
    except OSError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc

    entries: list[_ResetTreeEntry] = []
    for name in names:
        entry_count[0] += 1
        if entry_count[0] > _MAX_RESET_ENTRIES or name in {"", ".", ".."}:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        try:
            entry_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except OSError as exc:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
        if _blocking_file_flags(entry_stat):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)

        if stat.S_ISDIR(entry_stat.st_mode):
            if int(entry_stat.st_dev) != root_device:
                raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
            directory_key = (int(entry_stat.st_dev), int(entry_stat.st_ino))
            if directory_key in seen_directories:
                raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
            seen_directories.add(directory_key)
            child_fd, opened_stat = _open_directory_at(directory_fd, name)
            try:
                _require_mutable_directory(child_fd, opened_stat)
                children = _scan_reset_tree(
                    child_fd,
                    root_device=root_device,
                    seen_directories=seen_directories,
                    entry_count=entry_count,
                    depth=depth + 1,
                )
            finally:
                os.close(child_fd)
            entries.append(
                _ResetTreeEntry(
                    name=name,
                    stable_identity=_directory_identity(entry_stat),
                    observed_identity=_observed_entry_identity(entry_stat),
                    is_directory=True,
                    children=children,
                )
            )
            continue

        # Unlinking a regular file or a symlink is descriptor-relative and does
        # not follow its target. Special entries are unnecessary for Baileys
        # auth state and make safe, bounded preflight ambiguous, so reject them.
        if not (stat.S_ISREG(entry_stat.st_mode) or stat.S_ISLNK(entry_stat.st_mode)):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        entries.append(
            _ResetTreeEntry(
                name=name,
                stable_identity=_directory_identity(entry_stat),
                observed_identity=_observed_entry_identity(entry_stat),
                is_directory=False,
            )
        )
    return tuple(entries)


def _validate_reset_tree(
    directory_fd: int,
    entries: tuple[_ResetTreeEntry, ...],
) -> None:
    """Recheck the complete preflight snapshot, still without deleting."""
    try:
        if sorted(os.listdir(directory_fd)) != [entry.name for entry in entries]:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
    except WhatsAppRecoveryError:
        raise
    except OSError as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc

    for entry in entries:
        try:
            entry_stat = os.stat(
                entry.name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError as exc:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
        if _observed_entry_identity(entry_stat) != entry.observed_identity:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        if not entry.is_directory:
            continue
        child_fd, opened_stat = _open_directory_at(directory_fd, entry.name)
        try:
            _require_mutable_directory(child_fd, opened_stat)
            _validate_reset_tree(child_fd, entry.children)
        finally:
            os.close(child_fd)


def _delete_reset_tree(
    directory_fd: int,
    entries: tuple[_ResetTreeEntry, ...],
) -> None:
    """Delete only entries whose no-follow identity still matches preflight."""
    for entry in entries:
        try:
            entry_stat = os.stat(
                entry.name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError as exc:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
        if _observed_entry_identity(entry_stat) != entry.observed_identity:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        try:
            if not entry.is_directory:
                os.unlink(entry.name, dir_fd=directory_fd)
                continue

            child_fd, opened_stat = _open_directory_at(directory_fd, entry.name)
            try:
                if _directory_identity(opened_stat) != entry.stable_identity:
                    raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
                _delete_reset_tree(child_fd, entry.children)
                if os.listdir(child_fd):
                    raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
            finally:
                os.close(child_fd)
            final_stat = os.stat(
                entry.name, dir_fd=directory_fd, follow_symlinks=False
            )
            if _directory_identity(final_stat) != entry.stable_identity:
                raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
            os.rmdir(entry.name, dir_fd=directory_fd)
        except WhatsAppRecoveryError:
            raise
        except OSError as exc:
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc


def _open_matching_session_binding(
    binding: _SessionDirectoryBinding,
) -> _SessionDirectoryBinding:
    """Reopen the canonical path and match the entire captured identity chain."""
    fresh = _capture_session_directory_binding(
        binding.requested_path,
        binding.canonical_path,
    )
    if fresh is None:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
    try:
        if (
            fresh.requested_path != binding.requested_path
            or fresh.canonical_path != binding.canonical_path
            or fresh.components != binding.components
            or fresh.chain != binding.chain
            or binding.parent_fd < 0
            or binding.session_fd < 0
            or _directory_identity(os.fstat(binding.parent_fd)) != binding.chain[-2]
            or _directory_identity(os.fstat(binding.session_fd)) != binding.chain[-1]
        ):
            raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
        return fresh
    except Exception:
        fresh.close()
        raise


class WhatsAppRecoveryLease:
    """Own the OS recovery gate and session scope through reset plus pairing."""

    def __init__(
        self,
        identity: str,
        requested_path: Path,
        handle: IO[str],
        guard: threading.Lock,
    ):
        self._identity = identity
        self._canonical_session_path = Path(identity)
        self._requested_session_path = requested_path
        self._handle = handle
        self._guard = guard
        self._operation_lock = threading.Lock()
        self._session_binding: Optional[_SessionDirectoryBinding] = None
        self._session_lock_acquired = False
        self._reset_completed = False
        self.active = True

    @property
    def canonical_session_path(self) -> Path:
        """The immutable resolved session path used for both advisory locks."""
        return self._canonical_session_path

    @property
    def canonical_session_identity(self) -> str:
        """The immutable canonical identity string used by scoped locking."""
        return self._identity

    def _capture_reset_binding(self) -> None:
        self._session_binding = _capture_session_directory_binding(
            self._requested_session_path,
            self._canonical_session_path,
        )

    def pair_subprocess_kwargs(self) -> dict[str, object]:
        """Return the one FD inheritance needed by a POSIX pairing child.

        ``flock`` is attached to the shared open file description. Passing the
        held gate FD keeps the gate owned if this parent dies abruptly while
        the pair-only child is still writing session state. Normal callers wait
        for that child before explicitly releasing the lease. Windows does not
        support ``pass_fds`` and retains the existing parent-owned lifecycle.
        """
        with self._operation_lock:
            if not self.active:
                raise WhatsAppRecoveryError(
                    "The WhatsApp recovery lease is not active."
                )
            if os.name != "posix":
                return {}
            return {"pass_fds": (self._handle.fileno(),)}

    def reset_session(self, session_path: Optional[Path] = None) -> None:
        """Delete and recreate only this active lease's captured session.

        ``session_path`` is optional for callers that already retain the lease;
        when supplied it must be the exact normalized path requested during
        acquisition. Every component from the filesystem root (the trusted
        anchor) through the session is reopened with no-follow, descriptor-
        relative operations and compared with the identities captured after
        the recovery and session locks were acquired. The complete contents
        are preflighted twice before the first unlink.
        """
        with self._operation_lock:
            if not self.active or not self._session_lock_acquired:
                raise WhatsAppRecoveryError(
                    "The WhatsApp recovery lease is not active."
                )
            if self._reset_completed:
                raise WhatsAppRecoveryError(
                    "The WhatsApp session was already reset by this lease."
                )

            requested_path = (
                self._requested_session_path
                if session_path is None
                else _absolute_requested_path(session_path)
            )
            binding = self._session_binding
            if (
                requested_path != self._requested_session_path
                or self._requested_session_path != self._canonical_session_path
                or binding is None
                or not _reset_primitives_available()
            ):
                raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)

            fresh: Optional[_SessionDirectoryBinding] = None
            new_session_fd = -1
            try:
                fresh = _open_matching_session_binding(binding)
                parent_stat = os.fstat(fresh.parent_fd)
                session_stat = os.fstat(fresh.session_fd)
                if (
                    _directory_identity(parent_stat) != binding.chain[-2]
                    or _directory_identity(session_stat) != binding.chain[-1]
                ):
                    raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
                _require_mutable_directory(fresh.parent_fd, parent_stat)
                _require_mutable_directory(fresh.session_fd, session_stat)

                if parent_stat.st_mode & stat.S_ISVTX and hasattr(os, "geteuid"):
                    effective_uid = os.geteuid()
                    allowed_owners = {
                        0,
                        int(getattr(parent_stat, "st_uid", -1)),
                        int(getattr(session_stat, "st_uid", -1)),
                    }
                    if effective_uid not in allowed_owners:
                        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)

                session_key = (int(session_stat.st_dev), int(session_stat.st_ino))
                tree = _scan_reset_tree(
                    fresh.session_fd,
                    root_device=int(session_stat.st_dev),
                    seen_directories={session_key},
                    entry_count=[0],
                )
                _validate_reset_tree(fresh.session_fd, tree)

                # Reopen the complete path once more after scanning the tree.
                # A substitution found here is still a preflight failure, so no
                # auth entry has yet been removed.
                final_binding = _open_matching_session_binding(binding)
                final_binding.close()
                _validate_reset_tree(fresh.session_fd, tree)

                _delete_reset_tree(fresh.session_fd, tree)
                if os.listdir(fresh.session_fd):
                    raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
                final_session_stat = os.stat(
                    binding.session_name,
                    dir_fd=fresh.parent_fd,
                    follow_symlinks=False,
                )
                if (
                    _directory_identity(final_session_stat) != binding.chain[-1]
                    or _directory_identity(os.fstat(fresh.session_fd))
                    != binding.chain[-1]
                ):
                    raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)

                os.rmdir(binding.session_name, dir_fd=fresh.parent_fd)
                if binding.session_fd >= 0:
                    os.close(binding.session_fd)
                    binding.session_fd = -1
                os.mkdir(binding.session_name, mode=0o700, dir_fd=fresh.parent_fd)
                new_session_fd, new_session_stat = _open_directory_at(
                    fresh.parent_fd,
                    binding.session_name,
                )

                old_uid = getattr(session_stat, "st_uid", None)
                old_gid = getattr(session_stat, "st_gid", None)
                if (
                    old_uid is not None
                    and old_gid is not None
                    and (
                        getattr(new_session_stat, "st_uid", None) != old_uid
                        or getattr(new_session_stat, "st_gid", None) != old_gid
                    )
                ):
                    if not hasattr(os, "fchown"):
                        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)
                    os.fchown(new_session_fd, old_uid, old_gid)
                os.fchmod(new_session_fd, 0o700)
                recreated_stat = os.fstat(new_session_fd)
                if (
                    not stat.S_ISDIR(recreated_stat.st_mode)
                    or stat.S_IMODE(recreated_stat.st_mode) != 0o700
                    or (
                        old_uid is not None
                        and getattr(recreated_stat, "st_uid", None) != old_uid
                    )
                    or (
                        old_gid is not None
                        and getattr(recreated_stat, "st_gid", None) != old_gid
                    )
                ):
                    raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR)

                binding.session_fd = new_session_fd
                new_session_fd = -1
                self._reset_completed = True
            except WhatsAppRecoveryError:
                raise
            except (OSError, RecursionError, RuntimeError) as exc:
                raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
            finally:
                if new_session_fd >= 0:
                    try:
                        os.close(new_session_fd)
                    except OSError:
                        pass
                if fresh is not None:
                    fresh.close()

    def release(self) -> None:
        with self._operation_lock:
            if not self.active:
                return
            self.active = False
            try:
                if self._session_binding is not None:
                    self._session_binding.close()
                if self._session_lock_acquired:
                    release_scoped_lock(WHATSAPP_SESSION_SCOPE, self._identity)
                    self._session_lock_acquired = False
            finally:
                try:
                    _release_advisory_gate(self._handle)
                finally:
                    self._guard.release()

    close = release

    def __enter__(self) -> "WhatsAppRecoveryLease":
        return self

    def __exit__(self, *_exc) -> None:
        self.release()


def acquire_whatsapp_recovery_lease(
    session_path: Path,
    *,
    bridge_port: int = 3000,
    bridge_script: Optional[Path] = None,
    timeout: float = 10.0,
    poll_interval: float = 0.05,
) -> WhatsAppRecoveryLease:
    """Quiesce a proven bridge, then hold recovery and session scoped locks."""
    requested_path = _absolute_requested_path(session_path)
    try:
        identity = _session_identity(requested_path)
    except (OSError, RuntimeError, ValueError) as exc:
        raise WhatsAppRecoveryError(_RESET_SAFETY_ERROR) from exc
    guard = _local_guard(identity)
    if not guard.acquire(blocking=False):
        raise WhatsAppRecoveryError("WhatsApp recovery is already in progress.")

    handle: Optional[IO[str]] = None
    lease: Optional[WhatsAppRecoveryLease] = None
    try:
        handle = _acquire_advisory_gate(identity, "reset-pair")
        if handle is None:
            raise WhatsAppRecoveryError("WhatsApp recovery is already in progress.")
        lease = WhatsAppRecoveryLease(identity, requested_path, handle, guard)

        deadline = time.monotonic() + max(0.0, timeout)
        _quiesce_matching_bridge(
            requested_path,
            int(bridge_port),
            Path(bridge_script) if bridge_script is not None else _default_bridge_script(),
            deadline,
            max(0.001, poll_interval),
        )

        while time.monotonic() <= deadline:
            if not _local_session_is_held(identity):
                _read_session_lock_record(identity)
                acquired, _existing = acquire_scoped_lock(
                    WHATSAPP_SESSION_SCOPE,
                    identity,
                    metadata={"platform": "whatsapp", "operation": "reset-pair"},
                )
                if acquired:
                    lease._session_lock_acquired = True
                    lease._capture_reset_binding()
                    return lease
            time.sleep(max(0.001, poll_interval))
        raise WhatsAppRecoveryError("The WhatsApp session is still in use.")
    except Exception:
        if lease is not None:
            lease.release()
        else:
            if handle is not None:
                _release_advisory_gate(handle)
            guard.release()
        raise
