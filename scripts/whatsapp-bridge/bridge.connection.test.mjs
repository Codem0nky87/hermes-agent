/**
 * Behaviour tests for the production connection-close orchestration.
 *
 * These drive `createConnectionCloseHandler()` — the exact function bridge.js
 * installs on Baileys' `connection.update` — rather than the pure classifiers
 * underneath it. The classifiers can be individually correct while the
 * orchestration around them still writes a marker on a retryable close,
 * exits without recording the verdict, or schedules a reconnect it then
 * abandons by exiting. Only running the real handler pins those down.
 *
 * The disconnect errors here are constructed the way Baileys constructs them:
 * a Boom carrying the binary stream node on `data`. The production handler
 * consumes that object directly, so the whole chain from wire shape to side
 * effect is under test.
 */

import { strict as assert } from 'node:assert';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { Boom } from '@hapi/boom';
import { getAllBinaryNodeChildren, getErrorCodeFromStreamError } from '@whiskeysockets/baileys';

import { createConnectionCloseHandler } from './connection_close.js';
import {
  BRIDGE_EXIT_LOGGED_OUT,
  readSessionRevokedMarker,
  sessionRevokedMarkerPath,
} from './bridge_helpers.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const scratchDir = () => mkdtempSync(path.join(tmpdir(), 'wa-close-'));

/** The stream node WhatsApp sends when it has unlinked this device. */
const deviceRemovedNode = (extra = {}) => ({
  tag: 'stream:error',
  attrs: { code: '401', ...extra },
  content: [{ tag: 'conflict', attrs: { type: 'device_removed', ...extra } }],
});

/**
 * Turn a `<stream:error>` node into a `lastDisconnect` the way the INSTALLED
 * Baileys does, using its own helpers rather than a transcription of them.
 *
 * This mirrors `ws.on('CB:stream:error')` in
 * node_modules/@whiskeysockets/baileys/lib/Socket/socket.js, whose last line is
 *
 *     void end(new Boom(`Stream Errored (${reason})`, { statusCode, data: reasonNode || node }))
 *
 * Two things in there decide whether this bridge ever recognises a revocation,
 * and neither is obvious from the outside:
 *
 *   - `data` is `reasonNode` — the FIRST CHILD of the stream error, already
 *     unwrapped. The whole node is only passed when there are no children, so
 *     a handler that only understands the wrapper understands no real
 *     revocation at all.
 *   - `statusCode` is `+(node.attrs.code || CODE_MAP[reason] || 500)`, and
 *     `CODE_MAP.conflict` is 440. `code` is optional, so a revocation
 *     typically arrives as 440 rather than the 401 that `DisconnectReason`
 *     suggests.
 *
 * Building the error through the library means an upstream change to either
 * of those breaks these tests instead of silently un-terminal-ing revocation.
 */
function producerDisconnect(node) {
  const [reasonNode] = getAllBinaryNodeChildren(node);
  const { reason, statusCode } = getErrorCodeFromStreamError(node);
  return { error: new Boom(`Stream Errored (${reason})`, { statusCode, data: reasonNode || node }) };
}

/**
 * Run the real handler against one disconnect and collect every side effect.
 *
 * `exit` records whether the marker was already readable at the moment it was
 * called — that is the ordering guarantee, observed through the real filesystem
 * rather than through a spy's call order.
 */
function runClose(lastDisconnect, { sessionDir = scratchDir() } = {}) {
  const events = [];
  const logs = [];
  const reconnects = [];
  const reconnectOptions = [];
  const exits = [];

  const handleClose = createConnectionCloseHandler({
    sessionDir,
    emitPairEvent: event => events.push(event),
    log: line => logs.push(line),
    scheduleReconnect: (delayMs, options) => {
      reconnects.push(delayMs);
      reconnectOptions.push(options);
    },
    exit: code => exits.push({ code, markerAtExit: readSessionRevokedMarker(sessionDir) }),
  });

  handleClose(lastDisconnect);
  return { sessionDir, events, logs, reconnects, reconnectOptions, exits };
}

// -- ordinary 401 closes stay retryable -----------------------------------
//
// Status 401 alone is not evidence of revocation: a healthy session emits it
// too. The handler must reconnect, leave the session directory untouched, and
// keep the process alive.

// Status-only 401 — no stream node at all.
{
  const result = runClose({ error: new Boom('Stream Errored', { statusCode: 401 }) });

  assert.deepEqual(result.reconnects, [3000], 'a status-only 401 must schedule a reconnect');
  assert.deepEqual(result.exits, [], 'a status-only 401 must not exit the bridge');
  assert.equal(readSessionRevokedMarker(result.sessionDir), null, 'no marker may be written');
  assert.deepEqual(readdirSync(result.sessionDir), [], 'the session directory must be left untouched');
  assert.equal(result.events.length, 1);
  assert.equal(result.events[0].event, 'disconnected');
  assert.equal(result.events[0].reason, 401);
}

// Nested 401 conflict WITHOUT device_removed — another device took over the
// session; reconnecting is exactly the right response.
{
  const node = {
    tag: 'stream:error',
    attrs: { code: '401' },
    content: [{ tag: 'conflict', attrs: {} }],
  };
  const result = runClose({ error: new Boom('Stream Errored (conflict)', { statusCode: 401, data: node }) });

  assert.deepEqual(result.reconnects, [3000]);
  assert.deepEqual(result.exits, []);
  assert.equal(readSessionRevokedMarker(result.sessionDir), null);
  assert.equal(result.events[0].event, 'disconnected');
  assert.equal(result.events[0].streamReason, 'conflict');
}

// 515 (restart requested, normal right after pairing) keeps its fast retry.
{
  const result = runClose({ error: new Boom('Restart required', { statusCode: 515 }) });
  assert.deepEqual(result.reconnects, [1000], '515 must reconnect promptly');
  assert.deepEqual(result.reconnectOptions, [{ prompt: true }]);
  assert.deepEqual(result.exits, []);
}

// -- against the real producer --------------------------------------------
//
// Everything below this point up to the next banner builds its disconnect
// through the installed Baileys, so these assertions describe the bridge's
// behaviour on the bytes WhatsApp actually sends, not on a shape we invented.

// A revocation with no `code` attribute: status 440, direct-child data. This
// is the ordinary way a device unlink arrives, and it must be terminal.
{
  const result = runClose(producerDisconnect({
    tag: 'stream:error',
    attrs: {},
    content: [{ tag: 'conflict', attrs: { type: 'device_removed' } }],
  }));

  assert.equal(result.exits.length, 1, 'a real device_removed must exit');
  assert.equal(result.exits[0].code, BRIDGE_EXIT_LOGGED_OUT);
  assert.deepEqual(
    result.exits[0].markerAtExit,
    { revoked: true, statusCode: 440, reason: 'conflict', detail: 'device_removed' },
    'the marker must record the status the producer really emits',
  );
  assert.deepEqual(result.reconnects, [], 'a terminal close must not schedule a reconnect');
}

// The same unlink when the server does send `code="401"`.
{
  const result = runClose(producerDisconnect(deviceRemovedNode()));
  assert.equal(result.exits.length, 1);
  assert.equal(result.exits[0].code, BRIDGE_EXIT_LOGGED_OUT);
  assert.equal(result.exits[0].markerAtExit.detail, 'device_removed');
}

// An ordinary conflict — another device took the session — reaches us with
// the SAME 440 and the same direct-child shape, minus the `type`. It must
// reconnect: this is the close that a status-based rule would have condemned.
{
  const result = runClose(producerDisconnect({
    tag: 'stream:error',
    attrs: {},
    content: [{ tag: 'conflict', attrs: {} }],
  }));

  assert.deepEqual(result.reconnects, [3000], 'a bare conflict must reconnect');
  assert.deepEqual(result.exits, [], 'a bare conflict must not exit the bridge');
  assert.equal(readSessionRevokedMarker(result.sessionDir), null, 'a bare conflict must not condemn the session');
  assert.deepEqual(readdirSync(result.sessionDir), [], 'the session directory must be left untouched');
}

// A childless `<stream:error code="401">` is the one case where Baileys falls
// back to passing the whole node. It carries no reason child, so there is no
// durable evidence and the session must survive.
{
  const result = runClose(producerDisconnect({ tag: 'stream:error', attrs: { code: '401' }, content: undefined }));
  assert.deepEqual(result.reconnects, [3000], 'a bare 401 must reconnect');
  assert.deepEqual(result.exits, [], 'a bare 401 must not exit the bridge');
  assert.equal(readSessionRevokedMarker(result.sessionDir), null);
}

// -- explicit device_removed is terminal ----------------------------------

// The marker is durable BEFORE the process is told to exit, the exit code is
// the documented one, and no reconnect is left scheduled behind it.
{
  const result = runClose({
    error: new Boom('Stream Errored (conflict)', { statusCode: 401, data: deviceRemovedNode() }),
  });

  assert.equal(result.exits.length, 1, 'a durable revocation must exit');
  assert.equal(result.exits[0].code, BRIDGE_EXIT_LOGGED_OUT);
  assert.deepEqual(
    result.exits[0].markerAtExit,
    { revoked: true, statusCode: 401, reason: 'conflict', detail: 'device_removed' },
    'the marker must already be readable when exit is invoked',
  );
  assert.deepEqual(result.reconnects, [], 'a terminal close must not schedule a reconnect');

  // The marker is the restrictive one the Python side expects: owner-only,
  // written by the real production writer, no temp file left behind.
  if (process.platform !== 'win32') {
    const mode = statSync(sessionRevokedMarkerPath(result.sessionDir)).mode & 0o777;
    assert.equal(mode, 0o600, 'the marker must be owner-only');
  }
  assert.deepEqual(readdirSync(result.sessionDir), ['revoked.json']);
}

// Only bounded, non-secret metadata reaches the pair-event stream and the
// operator log. Everything the server put in the node stays in the node.
{
  const SENTINEL = 'sentinel-server-payload-do-not-copy';
  const result = runClose({
    error: new Boom(`Stream Errored (${SENTINEL})`, {
      statusCode: 401,
      data: deviceRemovedNode({ token: SENTINEL, secret: SENTINEL }),
    }),
  });

  const emitted = JSON.stringify(result.events);
  assert.ok(!emitted.includes(SENTINEL), 'no server payload may reach a pair event');
  assert.ok(!result.logs.join('\n').includes(SENTINEL), 'no server payload may reach a log line');

  assert.equal(result.events.length, 1);
  assert.deepEqual(
    Object.keys(result.events[0]).sort(),
    ['error', 'event', 'reason', 'streamReason'],
    'the logged-out event carries a fixed, bounded field set',
  );
  assert.equal(result.events[0].error, 'logged_out');
  assert.equal(result.events[0].streamReason, 'conflict: device_removed');
}

// Durable evidence outranks the status number: `code` is an optional
// attribute, so device_removed arriving without one is still terminal.
{
  const node = { tag: 'stream:error', attrs: {}, content: [{ tag: 'device_removed', attrs: {} }] };
  const result = runClose({ error: new Boom('Stream Errored (device_removed)', { statusCode: 500, data: node }) });
  assert.equal(result.exits.length, 1);
  assert.equal(result.exits[0].code, BRIDGE_EXIT_LOGGED_OUT);
  assert.equal(result.exits[0].markerAtExit.revoked, true);
}

// -- malformed / irrelevant metadata stays retryable ----------------------
//
// The handler runs on every close, including ones that carry nothing usable.
// Anything it cannot positively read as durable evidence must reconnect
// rather than condemn the session, and must never throw — a throw here takes
// the reconnect loop down with it.
{
  const malformed = [
    undefined,
    {},
    { error: undefined },
    { error: null },
    { error: 'a bare string' },
    { error: new Error('socket hang up') },
    { error: new Boom('Stream Errored', { statusCode: 401, data: 'not-a-node' }) },
    { error: new Boom('Stream Errored', { statusCode: 401, data: [] }) },
    { error: new Boom('Stream Errored', { statusCode: 401, data: { content: [] } }) },
    { error: new Boom('Stream Errored', { statusCode: 401, data: { content: [null] } }) },
    { error: new Boom('Stream Errored', { statusCode: 401, data: { reason: '401', location: 'fra' } }) },
    // A reason that merely *contains* the durable tag is not the durable tag.
    {
      error: new Boom('Stream Errored', {
        statusCode: 401,
        data: { tag: 'stream:error', attrs: {}, content: [{ tag: 'not_device_removed_really', attrs: {} }] },
      }),
    },
    // Hostile values are dropped by the sanitiser, leaving no evidence.
    {
      error: new Boom('Stream Errored', {
        statusCode: 401,
        data: { tag: 'stream:error', attrs: {}, content: [{ tag: 'device_removed\n[bridge] injected', attrs: {} }] },
      }),
    },
  ];

  for (const lastDisconnect of malformed) {
    const label = JSON.stringify(lastDisconnect?.error?.data ?? String(lastDisconnect?.error));
    const result = runClose(lastDisconnect);
    assert.deepEqual(result.exits, [], `${label} must not exit the bridge`);
    assert.equal(result.reconnects.length, 1, `${label} must schedule exactly one reconnect`);
    assert.equal(readSessionRevokedMarker(result.sessionDir), null, `${label} must not condemn the session`);
    assert.deepEqual(readdirSync(result.sessionDir), [], `${label} must leave the session directory untouched`);
  }
}

// A marker that cannot be written must not stop the bridge from exiting: the
// exit code is the fallback signal for whoever is still watching the child.
{
  const sessionDir = path.join(scratchDir(), 'not-a-directory');
  writeFileSync(sessionDir, 'this path is a file, so the marker write must fail');

  const exits = [];
  const logs = [];
  const handleClose = createConnectionCloseHandler({
    sessionDir,
    emitPairEvent: () => {},
    log: line => logs.push(line),
    scheduleReconnect: () => assert.fail('a terminal close must not schedule a reconnect'),
    exit: code => exits.push(code),
  });
  handleClose({ error: new Boom('Stream Errored (conflict)', { statusCode: 401, data: deviceRemovedNode() }) });

  assert.deepEqual(exits, [BRIDGE_EXIT_LOGGED_OUT], 'an unwritable marker must still exit 78');
  assert.ok(logs.some(line => /marker/i.test(line)), 'the failed marker write must be reported');
}

// -- pair-only refuses a pre-marked session before opening a socket -------
//
// The exit code only helps a caller that is watching this child; the marker
// is what survives process death and an externally launched bridge. This runs
// the real bridge.js as a subprocess against a throwaway session directory —
// never real auth state — and proves the refusal happens before any network
// or credential work.
{
  const sessionDir = scratchDir();
  writeFileSync(
    sessionRevokedMarkerPath(sessionDir),
    JSON.stringify({ revoked: true, statusCode: 401, reason: 'conflict', detail: 'device_removed' }),
  );

  let status = 0;
  let stdout = '';
  try {
    stdout = execFileSync(
      process.execPath,
      [path.join(HERE, 'bridge.js'), '--pair-only', '--pair-json', '--session', sessionDir],
      {
        cwd: HERE,
        encoding: 'utf8',
        timeout: 30_000,
        env: { ...process.env, WHATSAPP_ALLOWED_USERS: '', WHATSAPP_MODE: 'self-chat' },
      },
    );
  } catch (err) {
    status = err.status;
    stdout = err.stdout ?? '';
  }

  assert.equal(status, BRIDGE_EXIT_LOGGED_OUT, `--pair-only must exit 78 on a marked session (stdout: ${stdout})`);

  const events = stdout
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });

  assert.ok(
    events.some(e => e.event === 'error' && e.error === 'logged_out'),
    `a marked session must report logged_out (stdout: ${stdout})`,
  );
  assert.ok(!events.some(e => e.event === 'qr'), 'a marked session must never emit a QR code');
  assert.ok(!events.some(e => e.event === 'connected'), 'a marked session must never report connected');
  assert.ok(
    !existsSync(path.join(sessionDir, 'creds.json')),
    'refusing must happen before the auth state is opened, so no creds file is created',
  );
}

console.log('bridge.connection.test.mjs: all assertions passed');
