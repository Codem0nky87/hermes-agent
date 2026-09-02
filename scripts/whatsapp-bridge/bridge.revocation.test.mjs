/**
 * Unit tests for revocation classification and the revoked-session marker.
 *
 * These cover the two decisions that can permanently disable a WhatsApp
 * session: reading a disconnect off the wire, and recording the verdict on
 * disk. The production orchestration that calls them — which side effect
 * happens in which order — is covered in bridge.connection.test.mjs.
 */

import { strict as assert } from 'node:assert';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, readdirSync, rmSync, statSync, symlinkSync } from 'node:fs';
import * as fsModule from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

import {
  BRIDGE_EXIT_LOGGED_OUT,
  classifyDisconnectReason,
  extractDisconnectMetadata,
  readSessionRevokedMarker,
  sessionRevokedMarkerPath,
  writeSessionRevokedMarker,
} from './bridge_helpers.js';

const scratchDir = () => mkdtempSync(path.join(tmpdir(), 'wa-revocation-'));
const MAX_REVOKED_MARKER_BYTES = 4096;
const HELPERS_URL = new URL('./bridge_helpers.js', import.meta.url).href;

/** Read one synthetic marker in a kill-bounded child process. */
function subprocessMarkerRead(sessionDir, label) {
  const script = `
    import { readSessionRevokedMarker } from ${JSON.stringify(HELPERS_URL)};
    process.stdout.write(JSON.stringify(readSessionRevokedMarker(process.env.TEST_SESSION_DIR)));
  `;
  const result = spawnSync(process.execPath, ['--input-type=module', '--eval', script], {
    encoding: 'utf8',
    env: { ...process.env, TEST_SESSION_DIR: sessionDir },
    timeout: 1500,
  });
  assert.equal(result.error?.code, undefined, `${label} marker read exceeded the timeout`);
  assert.equal(result.status, 0, `${label} marker reader failed: ${result.stderr}`);
  return JSON.parse(result.stdout);
}

/**
 * Stand-in for the auth material these tests prove cannot escape.
 *
 * Deliberately NOT shaped like a real key. A realistic base64 blob would be
 * indistinguishable from a committed credential to a secret scanner, and a
 * fixture that trips CI on every run trains people to wave the scanner
 * through. Nothing here depends on the value's shape — only on it being a
 * distinctive string that must not appear in the output.
 */
const SECRET_SENTINEL = 'sentinel-auth-material-must-not-leak';
/** What an uninterpretable marker must read back as -- and nothing more. */
const SAFE_SYNTHETIC_VERDICT = { revoked: true, statusCode: null, reason: null, detail: null };

// -- extractDisconnectMetadata --------------------------------------------
//
// Baileys turns a WhatsApp `<stream:error>` into a Boom whose status comes
// from the node's OPTIONAL `code` attribute. That means DISTINCT stream
// reasons share status 401: a genuine revocation
// (`<stream:error code="401"><conflict type="device_removed"/></stream:error>`)
// and an ordinary conflict are numerically identical. The only thing that
// tells them apart is the binary node itself, which Boom hands over intact
// on `error.data`.
//
// This helper is the ONLY thing allowed to look at that node, and it copies
// nothing out of it but a numeric status and two short lowercase tags. The
// node can contain arbitrary server-supplied payload, so it must never be
// logged, serialised, or forwarded whole.

// THE shape the installed Baileys actually produces. `ws.on('CB:stream:error')`
// ends with `new Boom(..., { statusCode, data: reasonNode || node })`, where
// `reasonNode` is the FIRST CHILD of `<stream:error>`. So on every real
// revocation `error.data` is the already-unwrapped child — the wrapper only
// survives when the stream error has no children at all.
//
// The status is `+(node.attrs.code || CODE_MAP[reason] || 500)`, and
// `CODE_MAP.conflict` is 440 (connectionReplaced). The `code` attribute is
// optional, so 440 — not 401 — is what a revocation normally arrives with.
// bridge.connection.test.mjs pins this against the real producer; here it is
// pinned at the unit boundary.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 440 },
    data: { tag: 'conflict', attrs: { type: 'device_removed' } },
  });
  assert.equal(meta.statusCode, 440);
  assert.equal(meta.reason, 'conflict', 'the direct child tag is the stream reason');
  assert.equal(meta.detail, 'device_removed');
  assert.equal(classifyDisconnectReason(meta).terminal, true, 'the rc13 revocation shape must be terminal');
}

// The same fact expressed as a bare `<device_removed/>` child.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 440 },
    data: { tag: 'device_removed', attrs: {} },
  });
  assert.equal(meta.reason, 'device_removed');
  assert.equal(meta.detail, null);
  assert.equal(classifyDisconnectReason(meta).terminal, true);
}

// A direct-child conflict WITHOUT device_removed is the ordinary
// "another device took the session" close, and stays retryable. It arrives
// with the very same 440 as a revocation, so the number cannot decide it.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 440 },
    data: { tag: 'conflict', attrs: {} },
  });
  assert.equal(meta.statusCode, 440);
  assert.equal(meta.reason, 'conflict');
  assert.equal(meta.detail, null);
  assert.equal(classifyDisconnectReason(meta).terminal, false, 'conflict alone is not a revocation');
  assert.equal(classifyDisconnectReason(meta).delayMs, 3000);
}

// A childless `<stream:error>` falls through Baileys' `reasonNode || node`
// and arrives as the wrapper itself. Its tag is not a disconnect tag, so it
// yields no evidence — and must not be mistaken for one.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 401 },
    data: { tag: 'stream:error', attrs: { code: '401' } },
  });
  assert.equal(meta.statusCode, 401);
  assert.equal(meta.reason, null);
  assert.equal(meta.detail, null);
  assert.equal(classifyDisconnectReason(meta).terminal, false, 'a bare 401 must stay retryable');
}

// Only the EXACT durable tag counts. A direct child carrying some other
// `type` — including one that merely contains the tag — is not evidence.
{
  for (const type of ['device_removed_pending', 'not_device_removed', 'replaced', 'DEVICE REMOVED']) {
    const meta = extractDisconnectMetadata({
      output: { statusCode: 440 },
      data: { tag: 'conflict', attrs: { type } },
    });
    assert.equal(classifyDisconnectReason(meta).terminal, false, `type ${JSON.stringify(type)} must stay retryable`);
  }
  // ...while the exact tag, in either letter case, is terminal.
  for (const type of ['device_removed', 'DEVICE_REMOVED']) {
    const meta = extractDisconnectMetadata({ output: { statusCode: 440 }, data: { tag: 'conflict', attrs: { type } } });
    assert.equal(classifyDisconnectReason(meta).terminal, true, `type ${JSON.stringify(type)} must be terminal`);
  }
}

// Nothing the server hung off the direct child survives extraction either.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 440 },
    data: {
      tag: 'conflict',
      attrs: { type: 'device_removed', token: 'do-not-copy-me' },
      content: [{ tag: 'do-not-copy-me', attrs: { token: 'do-not-copy-me' } }],
    },
  });
  assert.deepEqual(Object.keys(meta).sort(), ['detail', 'reason', 'statusCode']);
  assert.ok(!JSON.stringify(meta).includes('do-not-copy-me'), 'no server payload may survive extraction');
}

// The nested wrapper shape stays supported: Baileys has moved this boundary
// before, and an older/patched producer that passes the whole node must not
// silently stop being understood.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 401 },
    data: {
      tag: 'stream:error',
      attrs: { code: '401' },
      content: [{ tag: 'conflict', attrs: { type: 'device_removed' } }],
    },
  });
  assert.equal(meta.statusCode, 401);
  assert.equal(meta.reason, 'conflict');
  assert.equal(meta.detail, 'device_removed');
}

// A plain `<device_removed/>` child (no wrapping conflict node) is the same
// durable fact expressed as the reason tag itself.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 401 },
    data: { tag: 'stream:error', attrs: { code: '401' }, content: [{ tag: 'device_removed', attrs: {} }] },
  });
  assert.equal(meta.reason, 'device_removed');
  assert.equal(meta.detail, null);
}

// An ordinary conflict -- another device took the session -- carries status
// 401 and NO device_removed anywhere. This is the shape that the previous
// status-only classification wrongly declared terminal.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 401 },
    data: { tag: 'stream:error', attrs: { code: '401' }, content: [{ tag: 'conflict', attrs: {} }] },
  });
  assert.equal(meta.statusCode, 401);
  assert.equal(meta.reason, 'conflict');
  assert.equal(meta.detail, null);
}

// `CB:failure` takes a different branch in Baileys and passes `node.attrs`
// (a flat object, no `content`) as `data`. A bare numeric `reason` attribute
// is just the status restated, so it must NOT become a reason tag.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 401 },
    data: { reason: '401', location: 'fra' },
  });
  assert.equal(meta.statusCode, 401);
  assert.equal(meta.reason, null, 'a numeric failure attr is not a stream reason');
  assert.equal(meta.detail, null);
}

// Missing/!odd shapes degrade to nulls instead of throwing -- this runs on
// the disconnect path, where a throw would kill the reconnect loop.
{
  for (const error of [undefined, null, {}, new Error('nope'), { data: 'a string' }, { data: [] },
                       { output: {} }, { data: { content: [] } }, { data: { content: 'nope' } },
                       { data: { content: [null] } }]) {
    const meta = extractDisconnectMetadata(error);
    assert.equal(typeof meta, 'object');
    assert.ok(meta.statusCode === null || typeof meta.statusCode === 'number');
    assert.ok(meta.reason === null || typeof meta.reason === 'string');
  }
  assert.equal(extractDisconnectMetadata({ statusCode: 515 }).statusCode, 515, 'a bare statusCode is read too');
}

// Whatever the server sends, only a known disconnect protocol tag survives.
// Syntactically harmless unknown tags are still server-controlled and could
// contain secrets, so they are dropped alongside malformed values.
{
  const hostile = [
    'a'.repeat(200),
    'has space',
    'new\nline',
    '<script>',
    'semi;colon',
    'sentinel_secret_must_not_leak',
    '',
    '   ',
  ];
  for (const tag of hostile) {
    const meta = extractDisconnectMetadata({
      output: { statusCode: 401 },
      data: { tag: 'stream:error', attrs: {}, content: [{ tag, attrs: { type: tag } }] },
    });
    assert.equal(meta.reason, null, `hostile tag ${JSON.stringify(tag)} must be dropped`);
    assert.equal(meta.detail, null, `hostile type ${JSON.stringify(tag)} must be dropped`);
  }
  assert.equal(
    extractDisconnectMetadata({ statusCode: 987654321 }).statusCode,
    null,
    'out-of-range numeric values must not be reflected as status codes',
  );
}

// Nothing from the raw node leaks into the returned object: exactly three
// keys, no `data`, no `attrs`, no nested content.
{
  const meta = extractDisconnectMetadata({
    output: { statusCode: 401 },
    data: {
      tag: 'stream:error',
      attrs: { code: '401', secret: 'do-not-copy-me' },
      content: [{ tag: 'conflict', attrs: { type: 'device_removed', token: 'do-not-copy-me' } }],
    },
  });
  assert.deepEqual(Object.keys(meta).sort(), ['detail', 'reason', 'statusCode']);
  assert.ok(!JSON.stringify(meta).includes('do-not-copy-me'), 'no server payload may survive extraction');
}

// -- classifyDisconnectReason ---------------------------------------------
//
// Terminal means "operator action required, stop retrying forever", so it
// must rest on explicit durable evidence, not on a status number.
//
// Status 401 is not exclusive to revocation: a healthy session emits
// status-401 closes carrying no device_removed reason and then reconnects
// normally on the same credentials. Classifying every 401 as terminal
// therefore discards working sessions -- the bug this suite pins shut.

// Explicit durable revocation, in either of the two shapes WhatsApp uses.
{
  for (const meta of [
    { statusCode: 401, reason: 'conflict', detail: 'device_removed' },
    { statusCode: 401, reason: 'device_removed', detail: null },
  ]) {
    const decision = classifyDisconnectReason(meta);
    assert.equal(decision.terminal, true, `${JSON.stringify(meta)} must be terminal`);
    assert.equal(decision.exitCode, BRIDGE_EXIT_LOGGED_OUT);
    assert.equal(decision.delayMs, undefined, 'a terminal close must not also schedule a reconnect');
  }
  assert.notEqual(BRIDGE_EXIT_LOGGED_OUT, 1, 'terminal exit must be distinguishable from a generic crash');
  assert.notEqual(BRIDGE_EXIT_LOGGED_OUT, 0);
  assert.ok(BRIDGE_EXIT_LOGGED_OUT > 0 && BRIDGE_EXIT_LOGGED_OUT < 126, 'stay clear of shell/signal exit codes');
  // 78 is sysexits.h EX_CONFIG: "this install needs operator action, not a
  // retry". The Python adapter pins the same number, but that agreement is
  // not what this line checks -- two literals matching proves nothing on its
  // own. The contract is exercised end to end in the Python suite
  // (tests/gateway/test_whatsapp_connect.py::TestCrossLanguageMarkerContract),
  // which runs this module's real writer against the real Python parser.
  assert.equal(BRIDGE_EXIT_LOGGED_OUT, 78);
}

// A 401 conflict WITHOUT device_removed is retryable: it means another device
// took the session, which reconnecting is the right response to.
{
  const decision = classifyDisconnectReason({ statusCode: 401, reason: 'conflict', detail: null });
  assert.equal(decision.terminal, false, 'a bare conflict is not a revocation');
  assert.equal(decision.delayMs, 3000);
  assert.equal(decision.exitCode, undefined);
}

// A 401 with no reason metadata at all is NOT automatically terminal.
{
  for (const meta of [{ statusCode: 401, reason: null, detail: null }, { statusCode: 401 }, 401]) {
    const decision = classifyDisconnectReason(meta);
    assert.equal(decision.terminal, false, `${JSON.stringify(meta)} must not be terminal on the status alone`);
    assert.equal(decision.delayMs, 3000);
  }
}

// 515 (restart-after-pairing) keeps its fast reconnect; 408/428 and unknown
// reasons keep the standard reconnect. None of them are terminal.
{
  for (const [statusCode, delayMs] of [[515, 1000], [408, 3000], [428, 3000], [500, 3000], [undefined, 3000], [null, 3000]]) {
    for (const meta of [statusCode, { statusCode }]) {
      const decision = classifyDisconnectReason(meta);
      assert.equal(decision.terminal, false, `status ${statusCode} must stay retryable`);
      assert.equal(decision.delayMs, delayMs, `status ${statusCode} must reconnect after ${delayMs}ms`);
      assert.equal(decision.exitCode, undefined, `status ${statusCode} must not exit the bridge`);
    }
  }
}

// Durable evidence wins regardless of the status number: `statusCode` is
// derived from an optional `code` attribute, so a device_removed node that
// arrives without one must still be terminal rather than reconnect forever.
{
  const decision = classifyDisconnectReason({ statusCode: 500, reason: 'device_removed', detail: null });
  assert.equal(decision.terminal, true);
  assert.equal(decision.exitCode, BRIDGE_EXIT_LOGGED_OUT);
}

// End to end over the wire shape: extraction feeds classification directly.
{
  const revoked = {
    output: { statusCode: 401 },
    data: { tag: 'stream:error', attrs: { code: '401' }, content: [{ tag: 'conflict', attrs: { type: 'device_removed' } }] },
  };
  const replaced = {
    output: { statusCode: 401 },
    data: { tag: 'stream:error', attrs: { code: '401' }, content: [{ tag: 'conflict', attrs: {} }] },
  };
  assert.equal(classifyDisconnectReason(extractDisconnectMetadata(revoked)).terminal, true);
  assert.equal(classifyDisconnectReason(extractDisconnectMetadata(replaced)).terminal, false);
}

// -- the revoked-session marker -------------------------------------------
//
// Exit 78 alone only survives as long as whoever launched the bridge is
// watching it. A marker file inside the session directory makes the verdict
// outlive process death, so a later gateway start (or an externally launched
// bridge that Hermes merely reuses) can refuse to open a socket at all.
//
// It lives INSIDE the session directory on purpose: both re-pair control
// paths remove that directory wholesale, so a re-paired session is naturally
// unmarked and no separate "clear the marker" step can be forgotten.

// Path contract: inside the session dir, next to creds.json.
{
  const sessionDir = path.join('/home', 'u', '.hermes', 'whatsapp', 'session');
  assert.equal(sessionRevokedMarkerPath(sessionDir), path.join(sessionDir, 'revoked.json'));
  // A trailing separator must not push the marker outside the session dir.
  assert.equal(sessionRevokedMarkerPath(`${sessionDir}${path.sep}`), path.join(sessionDir, 'revoked.json'));
}

// A written marker reads back as revoked, and carries only bounded,
// non-secret fields.
{
  const dir = scratchDir();
  const ok = writeSessionRevokedMarker(
    dir,
    { statusCode: 401, reason: 'conflict', detail: 'device_removed' },
    { now: () => '2026-08-25T06:00:00.000Z', log: () => {} },
  );
  assert.equal(ok, true);

  const raw = JSON.parse(readFileSync(sessionRevokedMarkerPath(dir), 'utf8'));
  assert.deepEqual(Object.keys(raw).sort(), ['at', 'detail', 'reason', 'revoked', 'statusCode']);
  assert.equal(raw.revoked, true);
  assert.equal(raw.statusCode, 401);
  assert.equal(raw.reason, 'conflict');
  assert.equal(raw.detail, 'device_removed');
  assert.equal(raw.at, '2026-08-25T06:00:00.000Z');

  // Reading back yields only sanitised, bounded fields. `at` is deliberately
  // NOT among them: it is free-form text in a file that anyone with disk
  // access can edit, and the read path must not hand it to a log line or a
  // pair event. It stays in the file for a human debugging the session.
  assert.deepEqual(readSessionRevokedMarker(dir), {
    revoked: true,
    statusCode: 401,
    reason: 'conflict',
    detail: 'device_removed',
  });
}

// A TAMPERED marker is re-sanitised on read, not trusted wholesale. The file
// is written by us, but it lives in a directory on disk: by the time it is
// read back it is untrusted input, and it gates a "stop and demand operator
// action" verdict. Anything hand-edited into it -- oversized or
// punctuation-bearing reasons, injected fields, a non-numeric status -- is
// dropped, while the explicit `revoked: true` verdict still stands.
{
  const dir = scratchDir();
  writeFileSync(sessionRevokedMarkerPath(dir), JSON.stringify({
    revoked: true,
    statusCode: 'not-a-number',
    reason: 'device_removed\n[bridge] injected log line',
    detail: 'x'.repeat(400),
    at: '[31mANSI[0m',
    noiseKey: SECRET_SENTINEL,
    creds: { me: { id: '27820000000@s.whatsapp.net' } },
  }));

  const marker = readSessionRevokedMarker(dir);
  assert.deepEqual(marker, { revoked: true, statusCode: null, reason: null, detail: null });
  assert.ok(!JSON.stringify(marker).includes('noiseKey'));
  assert.ok(!JSON.stringify(marker).includes(SECRET_SENTINEL));
  assert.ok(!JSON.stringify(marker).includes('s.whatsapp.net'));
  assert.ok(!JSON.stringify(marker).includes(''), 'no terminal escapes may survive a read');
}

// A tampered marker whose reason IS a legitimate tag still reads back
// bounded -- sanitising must not mean discarding usable detail.
{
  const dir = scratchDir();
  writeFileSync(sessionRevokedMarkerPath(dir), JSON.stringify({
    revoked: true, statusCode: '401', reason: 'DEVICE_REMOVED', detail: null, extra: 'ignored',
  }));
  assert.deepEqual(readSessionRevokedMarker(dir), {
    revoked: true, statusCode: 401, reason: 'device_removed', detail: null,
  });
}

// The marker must never carry auth material. Credential-shaped fields handed
// in alongside the reason are dropped, not copied through.
{
  const dir = scratchDir();
  writeSessionRevokedMarker(
    dir,
    {
      statusCode: 401,
      reason: 'conflict',
      detail: 'device_removed',
      noiseKey: SECRET_SENTINEL,
      creds: { me: { id: '27820000000@s.whatsapp.net' } },
      signedIdentityKey: 'do-not-copy-me',
    },
    { now: () => '2026-08-25T06:00:00.000Z', log: () => {} },
  );
  const text = readFileSync(sessionRevokedMarkerPath(dir), 'utf8');
  for (const secret of ['noiseKey', 'signedIdentityKey', 'do-not-copy-me', 's.whatsapp.net', 'creds', SECRET_SENTINEL]) {
    assert.ok(!text.includes(secret), `marker must not contain ${secret}`);
  }
}

// Unknown reason values are dropped by the same allowlist used on the wire,
// so even a tag-shaped secret cannot turn the marker into a smuggling channel.
{
  const dir = scratchDir();
  writeSessionRevokedMarker(
    dir,
    { statusCode: 401, reason: 'sentinel_marker_secret', detail: 'has space' },
    { now: () => '2026-08-25T06:00:00.000Z', log: () => {} },
  );
  const raw = readSessionRevokedMarker(dir);
  assert.equal(raw.reason, null);
  assert.equal(raw.detail, null);
  assert.equal(raw.revoked, true, 'the verdict survives even when the reason does not');
}

// The marker is written 0600 -- POSIX only; `chmod` on Windows does not
// carry the same meaning and NTFS ACLs are out of scope.
if (process.platform !== 'win32') {
  const dir = scratchDir();
  writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, { log: () => {} });
  const mode = statSync(sessionRevokedMarkerPath(dir)).mode & 0o777;
  assert.equal(mode, 0o600, 'the marker must be written with mode 0600');
}

// Written atomically, leaving no temp file behind -- the marker is written
// immediately before process.exit(78), so a half-written file would be read
// back by the next start.
{
  const dir = scratchDir();
  writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, { log: () => {} });
  assert.deepEqual(
    readdirSync(dir).filter(f => f !== 'revoked.json'),
    [],
    'no temp files may remain after writing the marker',
  );
}

// The durable write protocol is structural, not just an eventual-content
// assertion: random same-directory temp, exclusive 0600 open, whole-file
// fsync, atomic rename, then parent-directory fsync on supported platforms.
if (process.platform !== 'win32') {
  const dir = scratchDir();
  const operations = [];
  const pathByFd = new Map();
  const observingFs = {
    ...fsModule,
    openSync(filePath, flags, mode) {
      const fd = fsModule.openSync(filePath, flags, mode);
      pathByFd.set(fd, filePath);
      operations.push({ op: 'open', filePath, flags, mode, fd });
      return fd;
    },
    fsyncSync(fd) {
      operations.push({ op: 'fsync', filePath: pathByFd.get(fd), fd });
      return fsModule.fsyncSync(fd);
    },
    closeSync(fd) {
      operations.push({ op: 'close', filePath: pathByFd.get(fd), fd });
      return fsModule.closeSync(fd);
    },
    renameSync(from, to) {
      operations.push({ op: 'rename', from, to });
      return fsModule.renameSync(from, to);
    },
  };

  assert.equal(writeSessionRevokedMarker(dir, { reason: 'device_removed' }, {
    fs: observingFs,
    log: () => {},
  }), true);

  const destination = sessionRevokedMarkerPath(dir);
  const tempOpen = operations.find(item => item.op === 'open' && item.filePath !== dir);
  assert.ok(tempOpen, 'the temp file must be opened');
  assert.equal(path.dirname(tempOpen.filePath), dir, 'the temp must be in the destination directory');
  assert.match(path.basename(tempOpen.filePath), /^\.revoked\.\d+\.[a-f0-9]{12}\.tmp$/,
    'the temp name must carry fresh random entropy');
  assert.equal(tempOpen.flags, 'wx', 'the temp open must be exclusive');
  assert.equal(tempOpen.mode, 0o600, 'the temp open must request owner-only mode');

  const fileSyncIndex = operations.findIndex(item => item.op === 'fsync' && item.filePath === tempOpen.filePath);
  const renameIndex = operations.findIndex(item => item.op === 'rename' && item.to === destination);
  const dirOpen = operations.find(item => item.op === 'open' && item.filePath === dir);
  const dirSyncIndex = operations.findIndex(item => item.op === 'fsync' && item.filePath === dir);
  assert.ok(fileSyncIndex >= 0 && fileSyncIndex < renameIndex, 'the temp file must be fsynced before rename');
  assert.ok(dirOpen, 'the parent directory must be opened for durability');
  assert.ok(dirSyncIndex > renameIndex, 'the parent directory must be fsynced after rename');
}

// The explicit Windows fallback keeps the restrictive exclusive-temp + file
// fsync + atomic rename protocol but skips directory descriptors, which Node
// does not support reliably on Windows.
{
  const dir = scratchDir();
  let directoryOpenAttempts = 0;
  const windowsFs = {
    ...fsModule,
    openSync(filePath, flags, mode) {
      if (filePath === dir) {
        directoryOpenAttempts += 1;
        throw new Error('synthetic Windows directory-open failure');
      }
      return fsModule.openSync(filePath, flags, mode);
    },
  };
  assert.equal(writeSessionRevokedMarker(dir, { reason: 'device_removed' }, {
    fs: windowsFs,
    platform: 'win32',
    log: () => {},
  }), true);
  assert.equal(directoryOpenAttempts, 0, 'the Windows fallback must not attempt a directory fd');
  assert.equal(readSessionRevokedMarker(dir).reason, 'device_removed');
}

// A caller-provided clock is a test seam, not a metadata smuggling channel.
// Only a strict application-owned UTC timestamp may enter the marker.
{
  const dir = scratchDir();
  const sentinel = 'SENTINEL_UNTRUSTED_MARKER_METADATA';
  assert.equal(writeSessionRevokedMarker(dir, { reason: 'device_removed' }, {
    now: () => sentinel,
    log: () => {},
  }), true);
  const raw = readFileSync(sessionRevokedMarkerPath(dir), 'utf8');
  assert.ok(!raw.includes(sentinel), 'arbitrary metadata must not be persisted');
  assert.equal(JSON.parse(raw).at, null, 'an invalid timestamp must degrade to a fixed null');
}

// Metadata is copied from own data descriptors only. Accessors and hostile
// proxies must neither execute nor stop the terminal close path from writing a
// minimal verdict.
{
  const dir = scratchDir();
  let getterCalls = 0;
  const metadata = {};
  for (const key of ['statusCode', 'reason', 'detail']) {
    Object.defineProperty(metadata, key, {
      enumerable: true,
      get() {
        getterCalls += 1;
        throw new Error('SENTINEL_MARKER_GETTER');
      },
    });
  }
  assert.equal(writeSessionRevokedMarker(dir, metadata, { log: () => {} }), true);
  assert.equal(getterCalls, 0, 'marker serialization must not execute metadata getters');
  assert.deepEqual(readSessionRevokedMarker(dir), SAFE_SYNTHETIC_VERDICT,
    'unsafe metadata must degrade to the minimal revoked verdict');

  const proxyDir = scratchDir();
  const proxy = new Proxy({}, {
    getOwnPropertyDescriptor() { throw new Error('SENTINEL_MARKER_PROXY'); },
    get() { throw new Error('SENTINEL_MARKER_PROXY'); },
  });
  assert.equal(writeSessionRevokedMarker(proxyDir, proxy, { log: () => {} }), true);
  assert.deepEqual(readSessionRevokedMarker(proxyDir), SAFE_SYNTHETIC_VERDICT);
}

// Every pre-rename failure leaves a previously valid destination byte-for-byte
// intact and removes the random temp. This includes the parent-directory open,
// which is deliberately acquired before rename so unsupported uncertainty
// cannot destroy the prior verdict.
if (process.platform !== 'win32') {
  const failures = [
    {
      label: 'exclusive temp open',
      decorate(fsImpl, dir) {
        return { ...fsImpl, openSync(filePath, flags, mode) {
          if (filePath !== dir) throw new Error('synthetic temp open failure');
          return fsImpl.openSync(filePath, flags, mode);
        } };
      },
    },
    {
      label: 'file fsync',
      decorate(fsImpl) {
        return { ...fsImpl, fsyncSync() { throw new Error('synthetic file fsync failure'); } };
      },
    },
    {
      label: 'file close',
      decorate(fsImpl) {
        let first = true;
        return { ...fsImpl, closeSync(fd) {
          fsImpl.closeSync(fd);
          if (first) {
            first = false;
            throw new Error('synthetic file close failure');
          }
        } };
      },
    },
    {
      label: 'parent directory open',
      decorate(fsImpl, dir) {
        return { ...fsImpl, openSync(filePath, flags, mode) {
          if (filePath === dir) throw new Error('synthetic directory open failure');
          return fsImpl.openSync(filePath, flags, mode);
        } };
      },
    },
  ];

  for (const failure of failures) {
    const dir = scratchDir();
    writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, {
      now: () => '2026-08-25T06:00:00.000Z',
      log: () => {},
    });
    const destination = sessionRevokedMarkerPath(dir);
    const before = readFileSync(destination, 'utf8');
    const logs = [];
    const ok = writeSessionRevokedMarker(dir, { statusCode: 440, reason: 'conflict', detail: 'device_removed' }, {
      fs: failure.decorate(fsModule, dir),
      log: line => logs.push(line),
    });
    assert.equal(ok, false, `${failure.label} must report failure`);
    assert.equal(readFileSync(destination, 'utf8'), before, `${failure.label} must preserve the destination`);
    assert.deepEqual(readdirSync(dir), ['revoked.json'], `${failure.label} must clean up the temp`);
    assert.ok(!logs.join('\n').includes(dir), `${failure.label} must not log a path`);
  }
}

// A rename failure is nonfatal and logged: failing to write the marker must
// never stop the bridge from exiting 78.
{
  const dir = scratchDir();
  const logs = [];
  const ok = writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, {
    log: line => logs.push(line),
    fs: { ...fsModule, renameSync: () => { throw new Error('EPERM: rename failed'); } },
  });
  assert.equal(ok, false);
  assert.equal(readSessionRevokedMarker(dir), null);
  assert.ok(logs.some(l => /Could not persist the WhatsApp revoked-session marker/.test(l)));
  assert.ok(!logs.some(l => /EPERM|rename failed/.test(l)), 'raw filesystem errors must not be logged');
  assert.ok(!logs.some(l => l.includes(dir)), 'the absolute session path must not be logged');
  assert.deepEqual(readdirSync(dir), [], 'the orphaned temp file must be cleaned up');
}

// -- short writes ---------------------------------------------------------
//
// `writeSync` is NOT obliged to write the whole buffer. It returns how many
// bytes it actually took, and a partial write is a normal outcome, not an
// error: it is what a signal arriving mid-write, or a pipe/filesystem with a
// smaller internal transfer size, produces. Ignoring that return value writes
// a TRUNCATED file and then reports success — and this file is written
// immediately before `process.exit(78)`, so the truncated version is what the
// next bridge start reads back.
//
// Reading is fail-closed, so a truncated marker no longer lets the bridge
// reconnect into a dead session — but it still costs the operator the
// actionable reason, and turns a recorded verdict into an uninterpretable one
// they have to resolve by re-pairing. The write is atomic so that never
// happens by accident.

/**
 * An fs whose `writeSync` never takes more than `maxBytes` at a time.
 *
 * It really writes those bytes through to the real fd, so the assertion is
 * about the file that ends up on disk rather than about call bookkeeping.
 */
function shortWriteFs(maxBytes, calls = []) {
  return {
    ...fsModule,
    writeSync(fd, data, offset = 0, length) {
      const buffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
      const remaining = (length ?? buffer.length - offset);
      const take = Math.min(maxBytes, remaining);
      calls.push(take);
      return fsModule.writeSync(fd, buffer, offset, take);
    },
  };
}

// A partial write must be resumed until the whole payload is on disk.
{
  const dir = scratchDir();
  const calls = [];
  const logs = [];
  const ok = writeSessionRevokedMarker(dir, { statusCode: 440, reason: 'conflict', detail: 'device_removed' }, {
    now: () => '2026-08-25T06:00:00.000Z',
    log: line => logs.push(line),
    fs: shortWriteFs(7, calls),
  });

  assert.equal(ok, true, 'a resumable short write is not a failure');
  assert.ok(calls.length > 1, 'the writer must have needed more than one write call');
  assert.deepEqual(readSessionRevokedMarker(dir), {
    revoked: true, statusCode: 440, reason: 'conflict', detail: 'device_removed',
  }, 'the marker must be complete despite the short writes');
  assert.deepEqual(logs, [], 'a successful write must not report a failure');

  // Byte-exact: nothing duplicated, nothing dropped, trailing newline intact.
  const raw = readFileSync(sessionRevokedMarkerPath(dir), 'utf8');
  assert.equal(raw, `${JSON.stringify({
    revoked: true, statusCode: 440, reason: 'conflict', detail: 'device_removed', at: '2026-08-25T06:00:00.000Z',
  })}\n`);
  assert.deepEqual(readdirSync(dir), ['revoked.json'], 'no temp file may remain');
}

// A one-byte-at-a-time write still completes without duplicating or dropping
// bytes, even though the writer must resume after every byte.
{
  const dir = scratchDir();
  const ok = writeSessionRevokedMarker(dir, { statusCode: 440, reason: 'device_removed' }, {
    now: () => '2026-08-25T06:00:00.000Z',
    log: () => {},
    fs: shortWriteFs(1),
  });
  assert.equal(ok, true);
  const raw = readFileSync(sessionRevokedMarkerPath(dir), 'utf8');
  assert.equal(JSON.parse(raw).at, '2026-08-25T06:00:00.000Z');
  assert.equal(readSessionRevokedMarker(dir).reason, 'device_removed');
}

// A write that makes NO progress must give up rather than spin. Returning 0
// forever is what a full or broken destination looks like from here, and the
// bridge is on its way out: an infinite retry loop would hang the exit.
{
  const dir = scratchDir();
  const logs = [];
  let calls = 0;
  const ok = writeSessionRevokedMarker(dir, { statusCode: 440, reason: 'device_removed' }, {
    log: line => logs.push(line),
    fs: {
      ...fsModule,
      writeSync() {
        calls += 1;
        // Convert a hang into a test failure instead of an unbounded run.
        if (calls > 10_000) throw new Error('writer spun on a zero-length write');
        return 0;
      },
    },
  });

  assert.equal(ok, false, 'a stalled write must be reported as a failure');
  assert.ok(calls < 100, `the writer must stop on non-progress, not spin (${calls} calls)`);
  assert.equal(readSessionRevokedMarker(dir), null, 'no marker may be left behind');
  assert.deepEqual(readdirSync(dir), [], 'the temp file must be cleaned up');
  assert.ok(logs.some(l => /Could not persist the WhatsApp revoked-session marker/.test(l)));
}

// A write that fails PART WAY THROUGH must leave an existing marker exactly as
// it was: the destination is only touched by the final rename, so a
// half-written payload never becomes the file a later start reads.
{
  const dir = scratchDir();
  writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, {
    now: () => '2026-08-25T06:00:00.000Z',
    log: () => {},
  });
  const before = readFileSync(sessionRevokedMarkerPath(dir), 'utf8');

  for (const failing of [
    { label: 'a throwing write', writeSync: () => { throw new Error('ENOSPC: no space left on device'); } },
    { label: 'a stalled write', writeSync: () => 0 },
    {
      label: 'a write that stops after the first chunk',
      writeSync: (fd, data, offset = 0) => {
        if (offset > 0) throw new Error('ENOSPC: no space left on device');
        const buffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
        return fsModule.writeSync(fd, buffer, 0, Math.min(4, buffer.length));
      },
    },
  ]) {
    const logs = [];
    const ok = writeSessionRevokedMarker(dir, { statusCode: 440, reason: 'conflict', detail: 'device_removed' }, {
      log: line => logs.push(line),
      fs: { ...fsModule, writeSync: failing.writeSync },
    });

    assert.equal(ok, false, `${failing.label} must be reported as a failure`);
    assert.equal(readFileSync(sessionRevokedMarkerPath(dir), 'utf8'), before,
      `${failing.label} must leave the existing marker untouched`);
    assert.deepEqual(readdirSync(dir), ['revoked.json'], `${failing.label} must clean up its temp file`);
    assert.ok(logs.some(l => /Could not persist the WhatsApp revoked-session marker/.test(l)));
    assert.ok(!logs.some(l => /ENOSPC|no space left/.test(l)), 'raw filesystem errors must not be logged');
    assert.ok(!logs.some(l => l.includes(dir)), 'the absolute session path must not be logged');
  }
}

// -- the marker reads FAIL-CLOSED -----------------------------------------
//
// ABSENCE is the only state that means "no revocation evidence". A freshly
// paired session directory has no marker at all, and that is the common case
// this must never block.
//
// Everything else -- a marker that cannot be read, or one whose contents do
// not unambiguously say `revoked: true` -- is a marker we cannot interpret,
// and the safe reading of an uninterpretable revocation record is that the
// session IS revoked. The alternative fails OPEN: anyone who can truncate or
// chmod this one file (or a write torn by a crash) silently disarms the
// verdict and the bridge reconnects into a session WhatsApp already
// destroyed, forever. Parking startup is recoverable in one explicit
// operator step; reconnecting into a dead session is not recoverable at all.
//
// The fail-closed verdict is SYNTHETIC: it reflects nothing from the file it
// could not trust, so unreadable content can never become output.

// No marker at all: not revoked. This is the freshly paired state, and it is
// also what an explicit reset leaves behind (both control paths remove the
// whole session directory), so recovery keeps working.
{
  assert.equal(readSessionRevokedMarker(scratchDir()), null);
  assert.equal(readSessionRevokedMarker(path.join(scratchDir(), 'does-not-exist')), null);

  // A marked session that is then explicitly reset stops being revoked --
  // the destructive re-pair flow is the sanctioned way out of fail-closed.
  const dir = scratchDir();
  writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, { log: () => {} });
  assert.equal(readSessionRevokedMarker(dir).revoked, true, 'precondition');
  rmSync(sessionRevokedMarkerPath(dir));
  assert.equal(readSessionRevokedMarker(dir), null, 'clearing the session must clear the verdict');
}

// A corrupt, truncated, empty, non-object or hand-edited marker fails CLOSED.
{
  for (const junk of ['', '   ', 'not json', '{', '{"revoked": tru', '[]', 'null', '"revoked"',
                      '{}', '{"revoked": false}', '{"revoked": "true"}', '{"revoked": 1}',
                      '{"revoked": null}', '{"statusCode": 401}', '[{"revoked": true}]']) {
    const dir = scratchDir();
    writeFileSync(sessionRevokedMarkerPath(dir), junk);
    assert.deepEqual(
      readSessionRevokedMarker(dir),
      SAFE_SYNTHETIC_VERDICT,
      `unusable marker ${JSON.stringify(junk)} must fail closed`,
    );
  }
}

// A marker that cannot be READ fails closed too, for every reason except
// "it is not there". A real one first: a directory where the file belongs
// makes readFileSync raise EISDIR without any injection.
{
  const dir = scratchDir();
  mkdirSync(sessionRevokedMarkerPath(dir));
  assert.deepEqual(readSessionRevokedMarker(dir), SAFE_SYNTHETIC_VERDICT, 'an unreadable marker must fail closed');
}

// ...and a permission-denied read, which is how a hostile or broken install
// would most plausibly try to disarm the verdict. POSIX only: chmod does not
// carry this meaning on Windows.
if (process.platform !== 'win32' && !(process.getuid && process.getuid() === 0)) {
  const dir = scratchDir();
  writeSessionRevokedMarker(dir, { statusCode: 401, reason: 'device_removed' }, { log: () => {} });
  fsModule.chmodSync(sessionRevokedMarkerPath(dir), 0o000);
  try {
    assert.deepEqual(readSessionRevokedMarker(dir), SAFE_SYNTHETIC_VERDICT);
  } finally {
    fsModule.chmodSync(sessionRevokedMarkerPath(dir), 0o600);
  }
}

// ENOENT from the initial lstat is the ONLY error that means "no evidence";
// every other failure is unreadable or a race, not an absent marker.
{
  const dir = scratchDir();
  const throwing = (code) => {
    const err = new Error(`${code}: synthetic`);
    err.code = code;
    throw err;
  };
  assert.equal(
    readSessionRevokedMarker(dir, { fs: { ...fsModule, lstatSync: () => throwing('ENOENT') } }),
    null,
    'ENOENT means the session was never marked',
  );
  for (const code of ['EACCES', 'EISDIR', 'EIO', 'ELOOP', 'ENAMETOOLONG', undefined]) {
    assert.deepEqual(
      readSessionRevokedMarker(dir, { fs: { ...fsModule, lstatSync: () => throwing(code) } }),
      SAFE_SYNTHETIC_VERDICT,
      `a ${code} metadata failure must fail closed`,
    );
  }
}

// ENOENT after the marker was observed is a race, not genuine absence. The
// reader must fail closed rather than let an attacker win by swapping or
// unlinking the file between lstat and open.
{
  const dir = scratchDir();
  writeFileSync(sessionRevokedMarkerPath(dir), JSON.stringify({ revoked: true }));
  const racedFs = {
    ...fsModule,
    openSync() {
      const err = new Error('ENOENT: synthetic open race');
      err.code = 'ENOENT';
      throw err;
    },
  };
  assert.deepEqual(readSessionRevokedMarker(dir, { fs: racedFs }), SAFE_SYNTHETIC_VERDICT);
}

// Symlinks are never followed. This includes a readable target, a dangling
// target, and the dangerous symlink-to-FIFO shape that made readFileSync block
// forever. Every probe runs in a timeout-bounded child so a regression fails
// the test rather than hanging the suite.
if (process.platform !== 'win32') {
  const readableDir = scratchDir();
  const readableTarget = path.join(readableDir, 'target.json');
  writeFileSync(readableTarget, JSON.stringify({ revoked: true, reason: 'device_removed' }));
  symlinkSync(readableTarget, sessionRevokedMarkerPath(readableDir));
  assert.deepEqual(
    subprocessMarkerRead(readableDir, 'regular-file symlink'),
    SAFE_SYNTHETIC_VERDICT,
    'even a readable marker symlink must fail closed',
  );

  const danglingDir = scratchDir();
  symlinkSync(path.join(danglingDir, 'missing-target'), sessionRevokedMarkerPath(danglingDir));
  assert.deepEqual(
    subprocessMarkerRead(danglingDir, 'dangling symlink'),
    SAFE_SYNTHETIC_VERDICT,
    'a dangling symlink exists and therefore is not absence',
  );

  const fifoDir = scratchDir();
  const fifoPath = path.join(fifoDir, 'marker-fifo');
  execFileSync('mkfifo', [fifoPath]);
  symlinkSync(fifoPath, sessionRevokedMarkerPath(fifoDir));
  assert.deepEqual(subprocessMarkerRead(fifoDir, 'symlink-to-FIFO'), SAFE_SYNTHETIC_VERDICT);

  const directFifoDir = scratchDir();
  execFileSync('mkfifo', [sessionRevokedMarkerPath(directFifoDir)]);
  assert.deepEqual(subprocessMarkerRead(directFifoDir, 'FIFO'), SAFE_SYNTHETIC_VERDICT);
}

// Oversized regular files are rejected before parsing, with a bounded child
// probe proving the reader neither blocks nor accepts an actionable reason
// hidden behind an arbitrarily large allocation.
{
  const dir = scratchDir();
  const padding = 'x'.repeat(MAX_REVOKED_MARKER_BYTES + 1);
  writeFileSync(
    sessionRevokedMarkerPath(dir),
    JSON.stringify({ revoked: true, reason: 'device_removed', padding }),
  );
  assert.deepEqual(subprocessMarkerRead(dir, 'oversized'), SAFE_SYNTHETIC_VERDICT);
}

// A regular marker is opened no-follow/nonblocking where the host exposes the
// flags, read with a bounded buffer, and checked with fstat before parsing.
{
  const dir = scratchDir();
  writeFileSync(
    sessionRevokedMarkerPath(dir),
    JSON.stringify({ revoked: true, statusCode: 401, reason: 'device_removed' }),
  );
  const calls = { flags: null, fstats: 0, maxRead: 0 };
  const observingFs = {
    ...fsModule,
    openSync(filePath, flags, mode) {
      calls.flags = flags;
      return fsModule.openSync(filePath, flags, mode);
    },
    fstatSync(fd, options) {
      calls.fstats += 1;
      return fsModule.fstatSync(fd, options);
    },
    readSync(fd, buffer, offset, length, position) {
      calls.maxRead = Math.max(calls.maxRead, length);
      return fsModule.readSync(fd, buffer, offset, length, position);
    },
  };
  assert.equal(readSessionRevokedMarker(dir, { fs: observingFs }).reason, 'device_removed');
  if (fsModule.constants.O_NOFOLLOW !== undefined) {
    assert.ok((calls.flags & fsModule.constants.O_NOFOLLOW) !== 0, 'regular marker open must use O_NOFOLLOW');
  }
  if (fsModule.constants.O_NONBLOCK !== undefined) {
    assert.ok((calls.flags & fsModule.constants.O_NONBLOCK) !== 0, 'regular marker open must use O_NONBLOCK');
  }
  assert.ok(calls.fstats >= 2, 'the opened marker must be fstat-checked before and after reading');
  assert.ok(calls.maxRead <= MAX_REVOKED_MARKER_BYTES + 1, 'marker reads must stay bounded');
}

// The fail-closed verdict reflects NOTHING from the file it refused to
// trust. The unparseable bytes are attacker-influenced by assumption -- that
// is why they are not trusted -- and they end up in log lines and pair-event
// JSON, so not one of them may survive the read.
{
  const dir = scratchDir();
  writeFileSync(
    sessionRevokedMarkerPath(dir),
    `{"revoked": "${SECRET_SENTINEL}", "statusCode": "[31m", "reason": "device_removed`,
  );
  const marker = readSessionRevokedMarker(dir);
  assert.deepEqual(marker, SAFE_SYNTHETIC_VERDICT);
  const serialised = JSON.stringify(marker);
  assert.ok(!serialised.includes(SECRET_SENTINEL), 'no marker content may survive a fail-closed read');
  assert.ok(!serialised.includes('device_removed'), 'no reason may be invented from an untrusted file');
  assert.ok(!serialised.includes(''), 'no terminal escapes may survive a fail-closed read');
}

// A read failure must not reflect the error or the session path either --
// the verdict is the whole output, and it is a fixed shape.
{
  const dir = scratchDir();
  writeFileSync(sessionRevokedMarkerPath(dir), JSON.stringify({ revoked: true }));
  const marker = readSessionRevokedMarker(dir, {
    fs: {
      ...fsModule,
      openSync: () => { throw new Error(`EACCES: permission denied, open '${sessionRevokedMarkerPath(dir)}'`); },
    },
  });
  const serialised = JSON.stringify(marker);
  assert.ok(!serialised.includes(dir), 'the session path must not reach the verdict');
  assert.ok(!serialised.includes('EACCES'), 'the raw error must not reach the verdict');
}

console.log('bridge.revocation.test.mjs: all assertions passed');
