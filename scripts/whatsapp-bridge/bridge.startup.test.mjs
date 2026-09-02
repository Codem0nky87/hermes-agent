/**
 * Privacy boundary of the bridge's own startup output.
 *
 * The bridge's stdout is not private: in `--pair-json` mode the dashboard and
 * `hermes whatsapp` parse it line by line, and in every other mode it is
 * redirected into a log file the dashboard can display. Two things therefore
 * must not appear in it — the absolute session path (which spells out the OS
 * user's home directory) and the allowlisted phone numbers (which are the
 * operator's and their contacts' real identities).
 *
 * These run the real bridge.js as a subprocess. Each uses a throwaway session
 * directory carrying a revocation marker, which makes the bridge refuse and
 * exit before it opens a socket — so the startup path is exercised for real
 * without any network, and without ever touching real auth state.
 */

import { strict as assert } from 'node:assert';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { BRIDGE_EXIT_LOGGED_OUT, sessionRevokedMarkerPath } from './bridge_helpers.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));

/**
 * A throwaway session directory already marked revoked.
 *
 * A string `marker` is written verbatim, so a test can plant the malformed
 * bytes a real corrupted or hand-edited marker would contain.
 */
function markedSessionDir(marker = {
  revoked: true,
  statusCode: 401,
  reason: 'conflict',
  detail: 'device_removed',
}) {
  const dir = mkdtempSync(path.join(tmpdir(), 'wa-startup-'));
  writeFileSync(
    sessionRevokedMarkerPath(dir),
    typeof marker === 'string' ? marker : JSON.stringify(marker),
  );
  return dir;
}

/** Run bridge.js to its early exit and return its stdout plus exit status. */
function runBridge(extraArgs, env = {}, marker) {
  const sessionDir = markedSessionDir(marker);
  let status = 0;
  let stdout = '';
  try {
    stdout = execFileSync(
      process.execPath,
      [path.join(HERE, 'bridge.js'), '--session', sessionDir, ...extraArgs],
      {
        cwd: HERE,
        encoding: 'utf8',
        timeout: 30_000,
        env: { ...process.env, WHATSAPP_ALLOWED_USERS: '', WHATSAPP_MODE: 'self-chat', ...env },
      },
    );
  } catch (err) {
    status = err.status;
    stdout = err.stdout ?? '';
  }
  return { sessionDir, status, stdout };
}

// -- the session path never reaches stdout --------------------------------

// Pair mode, JSON events: the `started` event used to carry the absolute
// session directory, which the dashboard then had in its parse buffer.
{
  const { sessionDir, status, stdout } = runBridge(['--pair-only', '--pair-json']);
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `expected the early revoked exit (stdout: ${stdout})`);

  const events = stdout
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });

  const started = events.find(e => e.event === 'started');
  assert.ok(started, `a started event must still be emitted (stdout: ${stdout})`);
  assert.deepEqual(Object.keys(started).sort(), ['event', 'ts'], 'started must carry no session path');
  assert.ok(!stdout.includes(sessionDir), 'the absolute session path must not reach the pair-event stream');
}

// A marker is local untrusted input by the time it is read back. Only known
// protocol tags and bounded status codes may be reflected into pair output.
{
  const secret = 'SENTINEL_MARKER_SECRET';
  const numericSecret = '987654321';
  const { status, stdout } = runBridge(
    ['--pair-only', '--pair-json'],
    {},
    { revoked: true, statusCode: Number(numericSecret), reason: secret, detail: 'device_removed' },
  );
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT);
  assert.ok(!stdout.includes(secret.toLowerCase()), 'marker reason must not be reflected');
  assert.ok(!stdout.includes(numericSecret), 'out-of-range marker status must not be reflected');
  assert.match(stdout, /device_removed/, 'the recognised revocation evidence remains actionable');
}

// Pair mode, human output.
{
  const { sessionDir, status, stdout } = runBridge(['--pair-only']);
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `expected the early revoked exit (stdout: ${stdout})`);
  assert.ok(!stdout.includes(sessionDir), 'the absolute session path must not reach the pairing log');
}

// Server mode's startup banner.
{
  const { sessionDir, status, stdout } = runBridge(['--port', '0']);
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `expected the early revoked exit (stdout: ${stdout})`);
  assert.ok(!stdout.includes(sessionDir), 'the absolute session path must not reach the startup banner');
}

// -- allowlisted numbers never reach stdout -------------------------------
//
// The banner used to print every allowlisted number. Operators need to know
// the allowlist is configured and how large it is; the numbers themselves are
// personal data with no diagnostic value in a log.
{
  const numbers = ['27820000001', '27820000002'];
  const { status, stdout } = runBridge(['--port', '0'], { WHATSAPP_ALLOWED_USERS: numbers.join(',') });
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `expected the early revoked exit (stdout: ${stdout})`);

  for (const number of numbers) {
    assert.ok(!stdout.includes(number), `the allowlisted number ${number} must not be logged`);
  }
  assert.match(stdout, /2/, 'the allowlist size must still be reported');
  assert.match(stdout, /mode: self-chat/, 'the mode must still be reported');
}

// An explicit open allowlist is reported as open rather than as a count of
// one, because "1 allowed user" would badly misdescribe `*`.
{
  const { status, stdout } = runBridge(['--port', '0'], { WHATSAPP_ALLOWED_USERS: '*' });
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `expected the early revoked exit (stdout: ${stdout})`);
  assert.match(stdout, /open/i, 'an explicit `*` allowlist must be described as open');
}

// -- a MALFORMED marker parks startup exactly as a valid one does ---------
//
// The marker is the verdict that survives process death, and it is a single
// file in a directory anyone with the operator's disk access can touch. If an
// unreadable or truncated marker meant "no evidence", then corrupting one byte
// — or a write torn by a crash — would silently disarm it and the bridge would
// reconnect into a session WhatsApp has already destroyed, forever, with
// nobody watching an exit code.
//
// So an uninterpretable marker fails CLOSED: the bridge refuses before it
// opens a socket, exits 78, and leaves recovery to the explicit destructive
// reset the CLI and dashboard offer. It never deletes anything itself.

/** Malformed bytes carrying a sentinel none of which may be echoed back. */
const MARKER_SENTINEL = 'SENTINEL_MARKER_CONTENT_MUST_NOT_LEAK';
const MALFORMED_MARKER = `{"revoked": "${MARKER_SENTINEL}", "reason": "device_removed`;

/** Parse the `--pair-json` event stream, ignoring anything that is not JSON. */
function pairEvents(stdout) {
  return stdout
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
}

// Server mode: refused before any socket, and before any auth state is read.
{
  const { sessionDir, status, stdout } = runBridge(['--port', '0'], {}, MALFORMED_MARKER);
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `a malformed marker must park startup (stdout: ${stdout})`);

  // useMultiFileAuthState() creates creds.json the moment it runs, so a
  // session directory still holding nothing but the marker is proof the
  // bridge stopped before it touched auth state — and therefore before it
  // could open a socket.
  assert.deepEqual(readdirSync(sessionDir), ['revoked.json'], 'no socket or auth state may be reached');
  assert.ok(!stdout.includes(MARKER_SENTINEL), 'marker content must not be echoed');
  assert.ok(!stdout.includes(sessionDir), 'the absolute session path must not be logged');
}

// Pair mode: the machine-readable verdict the dashboard and `hermes whatsapp`
// act on is still emitted, carrying nothing read out of the file. That event
// is what drives the operator to the explicit reset/re-pair flow, so the
// recovery route stays usable even though the marker itself was unusable.
{
  const { sessionDir, status, stdout } = runBridge(['--pair-only', '--pair-json'], {}, MALFORMED_MARKER);
  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `pair mode must park too (stdout: ${stdout})`);

  const events = pairEvents(stdout);
  const failure = events.find(e => e.event === 'error');
  assert.ok(failure, `the logged-out verdict must still be reported (stdout: ${stdout})`);
  assert.equal(failure.error, 'logged_out');
  assert.equal(failure.reason, null, 'a fail-closed verdict carries no status');
  assert.equal(failure.streamReason, null, 'a fail-closed verdict invents no stream reason');
  assert.ok(!events.some(e => e.event === 'qr' || e.event === 'connected'), 'no socket may be opened');
  assert.ok(!stdout.includes(MARKER_SENTINEL), 'marker content must not reach the pair-event stream');

  // The bridge signals; it never destroys. Clearing auth state stays the
  // operator's explicit decision, so the reset flow finds the session intact.
  assert.deepEqual(readdirSync(sessionDir), ['revoked.json']);
  assert.equal(readFileSync(sessionRevokedMarkerPath(sessionDir), 'utf8'), MALFORMED_MARKER);
}

console.log('bridge.startup.test.mjs: all assertions passed');
