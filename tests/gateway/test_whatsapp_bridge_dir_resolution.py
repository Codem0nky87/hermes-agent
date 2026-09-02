"""Tests for resolve_whatsapp_bridge_dir() — read-only install tree handling.

Regression coverage for #49561: in the Docker image the install tree
(/opt/hermes/scripts/whatsapp-bridge) is read-only, so `npm install` fails
with EACCES. The resolver must detect the read-only install dir and mirror the
bridge source into a writable HERMES_HOME location instead.
"""
import importlib
import os
from pathlib import Path

import pytest

from gateway.platforms import whatsapp_common


def _seed_install_tree(install_bridge: Path) -> None:
    """Create a minimal fake bridge source tree."""
    from gateway.platforms.whatsapp_common import WHATSAPP_BRIDGE_RUNTIME_INPUTS

    install_bridge.mkdir(parents=True, exist_ok=True)
    for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
        (install_bridge / filename).write_text(f"// packaged {filename}\n")
    (install_bridge / "bridge.js").write_text("// bridge\n")
    (install_bridge / "package.json").write_text('{"name": "whatsapp-bridge"}\n')
    (install_bridge / "package-lock.json").write_text(
        '{"lockfileVersion":3,"packages":{}}\n'
    )


@pytest.mark.require_symlinks
def test_writability_probe_does_not_follow_predictable_symlink(
    tmp_path, monkeypatch
):
    install_root = tmp_path / "install"
    install_bridge = install_root / "scripts" / "whatsapp-bridge"
    _seed_install_tree(install_bridge)

    outside_target = tmp_path / "outside-target"
    outside_target.write_bytes(b"outside bytes must not change")
    fixed_time_ns = 1_600_000_000_000_000_000
    os.utime(outside_target, ns=(fixed_time_ns, fixed_time_ns))
    target_bytes = outside_target.read_bytes()
    target_before = outside_target.stat()

    predictable_probe = install_bridge / ".write_test"
    predictable_probe.symlink_to(outside_target)
    install_entries_before = sorted(path.name for path in install_bridge.iterdir())

    monkeypatch.setattr(
        whatsapp_common,
        "__file__",
        str(install_root / "gateway" / "platforms" / "whatsapp_common.py"),
    )
    monkeypatch.setattr(
        "hermes_constants.get_hermes_home", lambda: tmp_path / "hermes-home"
    )

    assert whatsapp_common.resolve_whatsapp_bridge_dir() == install_bridge

    target_after = outside_target.stat()
    assert outside_target.read_bytes() == target_bytes
    assert (
        target_after.st_mode,
        target_after.st_uid,
        target_after.st_gid,
        target_after.st_size,
        target_after.st_mtime_ns,
        target_after.st_ctime_ns,
    ) == (
        target_before.st_mode,
        target_before.st_uid,
        target_before.st_gid,
        target_before.st_size,
        target_before.st_mtime_ns,
        target_before.st_ctime_ns,
    )
    assert predictable_probe.is_symlink()
    assert sorted(path.name for path in install_bridge.iterdir()) == install_entries_before


def test_readonly_install_mirrors_to_hermes_home(tmp_path, monkeypatch):
    """A read-only install tree is mirrored into a writable HERMES_HOME."""
    install_root = tmp_path / "install"
    install_bridge = install_root / "scripts" / "whatsapp-bridge"
    _seed_install_tree(install_bridge)

    hermes_home = tmp_path / "hermes_home"
    hermes_home.mkdir()

    monkeypatch.setattr(
        whatsapp_common, "__file__",
        str(install_root / "gateway" / "platforms" / "whatsapp_common.py"),
    )
    monkeypatch.setattr(
        "hermes_constants.get_hermes_home", lambda: hermes_home
    )

    # chmod(0o555) is unreliable under root (CI/Docker bypass permission
    # bits), so force only the resolver's directory write probe to fail.
    monkeypatch.setattr(
        whatsapp_common,
        "_whatsapp_bridge_directory_is_writable",
        lambda path: path != install_bridge,
    )

    resolved = whatsapp_common.resolve_whatsapp_bridge_dir()

    expected = hermes_home / "scripts" / "whatsapp-bridge"
    assert resolved == expected
    # Source was mirrored, not symlinked.
    assert (expected / "bridge.js").read_text() == "// bridge\n"
    assert (expected / "package.json").exists()


def test_existing_mirror_refreshes_packaged_source_but_preserves_runtime_state(
    tmp_path, monkeypatch
):
    """An old writable mirror must not strand source from a prior release."""
    install_root = tmp_path / "install"
    install_bridge = install_root / "scripts" / "whatsapp-bridge"
    _seed_install_tree(install_bridge)
    (install_bridge / "bridge.js").write_text("// packaged-current\n")
    (install_bridge / "connection_close.js").write_text("// current helper\n")
    (install_bridge / "package.json").write_text('{"name":"packaged-current"}\n')
    (install_bridge / "package-lock.json").write_text('{"lock":"packaged-current"}\n')

    hermes_home = tmp_path / "hermes_home"
    mirror = hermes_home / "scripts" / "whatsapp-bridge"
    node_modules = mirror / "node_modules"
    node_modules.mkdir(parents=True)
    (mirror / "bridge.js").write_text("// stale mirror\n")
    (mirror / "connection_close.js").write_text("// stale helper\n")
    (mirror / "package.json").write_text('{"name":"stale"}\n')
    (mirror / "package-lock.json").write_text('{"lock":"stale"}\n')
    (node_modules / "installed-version").write_text("intentional-local-install")
    (mirror / "bridge.log").write_text("runtime-only")

    monkeypatch.setattr(
        whatsapp_common,
        "__file__",
        str(install_root / "gateway" / "platforms" / "whatsapp_common.py"),
    )
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: hermes_home)
    monkeypatch.setattr(
        whatsapp_common,
        "_whatsapp_bridge_directory_is_writable",
        lambda path: path != install_bridge,
    )

    resolved = whatsapp_common.resolve_whatsapp_bridge_dir()

    assert resolved == mirror
    assert (mirror / "bridge.js").read_text() == "// packaged-current\n"
    assert (mirror / "connection_close.js").read_text() == "// current helper\n"
    assert (mirror / "package.json").read_text() == '{"name":"packaged-current"}\n'
    assert (mirror / "package-lock.json").read_text() == '{"lock":"packaged-current"}\n'
    assert (node_modules / "installed-version").read_text() == "intentional-local-install"
    assert (mirror / "bridge.log").read_text() == "runtime-only"


def test_dependency_freshness_is_a_manifest_content_contract(tmp_path):
    bridge_dir = tmp_path / "bridge"
    node_modules = bridge_dir / "node_modules"
    node_modules.mkdir(parents=True)
    manifest = bridge_dir / "package.json"
    lockfile = bridge_dir / "package-lock.json"
    manifest.write_text('{"dependencies":{"bridge":"local-choice"}}')
    lockfile.write_text('{"lockfileVersion":3,"packages":{"":"local-choice"}}')

    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is False

    whatsapp_common.write_whatsapp_bridge_dependency_stamp(bridge_dir)
    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is True

    lockfile.write_text('{"lockfileVersion":3,"packages":{"":"lock-only-change"}}')
    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is False

    whatsapp_common.write_whatsapp_bridge_dependency_stamp(bridge_dir)
    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is True

    manifest.write_text('{"dependencies":{"bridge":"upstream-choice"}}')
    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is False


@pytest.mark.skipif(os.name == "nt", reason="symlink/FIFO setup is POSIX-only")
@pytest.mark.parametrize("hostile_kind", ["symlink", "fifo"])
def test_invalid_manifest_never_writes_or_accepts_a_dependency_stamp(
    tmp_path, hostile_kind
):
    bridge_dir = tmp_path / "bridge"
    node_modules = bridge_dir / "node_modules"
    node_modules.mkdir(parents=True)
    (bridge_dir / "package-lock.json").write_text('{"lockfileVersion":3}')
    manifest = bridge_dir / "package.json"
    if hostile_kind == "symlink":
        private = bridge_dir / "private-manifest"
        private.write_text('{"private":"content"}')
        manifest.symlink_to(private)
    else:
        os.mkfifo(manifest)

    with pytest.raises(OSError) as error:
        whatsapp_common.write_whatsapp_bridge_dependency_stamp(bridge_dir)

    assert str(error.value) == "WhatsApp bridge manifests are unavailable."
    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is False
    assert not (node_modules / ".hermes-pkg-hash").exists()


def test_sync_removes_only_allowlisted_stale_production_when_source_is_missing(
    tmp_path,
):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    _seed_install_tree(source)
    _seed_install_tree(destination)
    (source / "connection_close.js").unlink()
    (destination / "runtime.log").write_text("preserve runtime")

    with pytest.raises(OSError) as error:
        whatsapp_common._sync_whatsapp_bridge_source(source, destination)

    assert str(error.value) == "WhatsApp bridge source synchronization failed."
    assert not (destination / "connection_close.js").exists()
    assert (destination / "runtime.log").read_text() == "preserve runtime"


@pytest.mark.skipif(os.name == "nt", reason="symlink/FIFO setup is POSIX-only")
@pytest.mark.parametrize("hostile_kind", ["root-symlink", "entry-symlink", "entry-fifo"])
def test_mirror_refuses_hostile_destination_before_any_production_mutation(
    tmp_path, monkeypatch, hostile_kind
):
    install_root = tmp_path / "install"
    install_bridge = install_root / "scripts" / "whatsapp-bridge"
    _seed_install_tree(install_bridge)
    (install_bridge / "bridge.js").write_text("// packaged-current\n")

    hermes_home = tmp_path / "hermes-home"
    mirror = hermes_home / "scripts" / "whatsapp-bridge"
    mirror.parent.mkdir(parents=True)
    real_mirror = tmp_path / "real-mirror"
    if hostile_kind == "root-symlink":
        _seed_install_tree(real_mirror)
        (real_mirror / "bridge.js").write_text("// stale-root\n")
        mirror.symlink_to(real_mirror, target_is_directory=True)
    else:
        _seed_install_tree(mirror)
        (mirror / "bridge.js").write_text("// stale-entry-root\n")
        hostile = mirror / "bridge_helpers.js"
        hostile.unlink()
        if hostile_kind == "entry-symlink":
            private = tmp_path / "private-helper"
            private.write_text("private helper")
            hostile.symlink_to(private)
        else:
            os.mkfifo(hostile)

    monkeypatch.setattr(
        whatsapp_common,
        "__file__",
        str(install_root / "gateway" / "platforms" / "whatsapp_common.py"),
    )
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: hermes_home)
    monkeypatch.setattr(
        whatsapp_common,
        "_whatsapp_bridge_directory_is_writable",
        lambda path: path != install_bridge,
    )

    assert whatsapp_common.resolve_whatsapp_bridge_dir() == install_bridge
    if hostile_kind == "root-symlink":
        assert (real_mirror / "bridge.js").read_text() == "// stale-root\n"
    else:
        assert (mirror / "bridge.js").read_text() == "// stale-entry-root\n"

