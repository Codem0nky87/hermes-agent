/** Behaviour tests for bridge output and HTTP privacy boundaries. */

import { strict as assert } from 'node:assert';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(HERE, 'bridge.js');
const FAKE_BAILEYS_LOADER = path.join(HERE, 'bridge.fake-baileys-loader.mjs');

const IDENTITY_SENTINELS = {
  FAKE_WA_IDENTITY_ID: 'SENTINEL_PHONE_27820000001:7@s.whatsapp.net',
  FAKE_WA_IDENTITY_LID: 'SENTINEL_LID_918273645@lid',
  FAKE_WA_IDENTITY_NAME: 'SENTINEL_PRIVATE_PROFILE_NAME',
  FAKE_WA_IDENTITY_VERIFIED_NAME: 'SENTINEL_PRIVATE_VERIFIED_NAME',
};

const ERROR_SENTINELS = {
  FAKE_WA_ERROR_SECRET: 'SENTINEL_THROWN_SECRET',
  FAKE_WA_ERROR_JID: 'SENTINEL_FAILURE_27820000002@s.whatsapp.net',
  FAKE_WA_ERROR_PATH: '/private/SENTINEL_FAILURE_HOME/media/file.mp3',
};

const DIAGNOSTIC_SENTINELS = {
  FAKE_WA_DEBUG_CHAT: 'SENTINEL_CHAT_27820000003@s.whatsapp.net',
  FAKE_WA_DEBUG_SENDER: 'SENTINEL_SENDER_27820000004:9@s.whatsapp.net',
  FAKE_WA_MESSAGE_ID: 'SENTINEL_MESSAGE_ID_ABC123',
  FAKE_WA_FOREIGN_POLL_ID: 'SENTINEL_FOREIGN_POLL_ID_ABC456',
  FAKE_WA_FOREIGN_VOTE_ID: 'SENTINEL_FOREIGN_VOTE_ID_ABC789',
  FAKE_WA_POLL_ID: 'SENTINEL_POLL_ID_ABC000',
  FAKE_WA_POLL_VOTE_ID: 'SENTINEL_POLL_VOTE_ID_ABC111',
  FAKE_WA_OBJECT_KEY: 'SENTINEL_HOSTILE_OBJECT_KEY',
  FAKE_WA_BODY_SECRET: 'SENTINEL_PRIVATE_BODY_VALUE',
  FAKE_WA_MEDIA_TEXT_SECRET: 'SENTINEL_PRIVATE_MEDIA_VALUE',
  FAKE_WA_POLL_OPTION_SECRET: 'SENTINEL_PRIVATE_OPTION_ALPHA',
  FAKE_WA_POLL_SUCCESS_CHAT: 'SENTINEL_POLL_CHAT_27820000005@s.whatsapp.net',
};

function runPairBridge(extraArgs, extraEnv = {}) {
  const sessionDir = mkdtempSync(path.join(tmpdir(), 'wa-privacy-pair-'));
  const result = spawnSync(
    process.execPath,
    ['--import', FAKE_BAILEYS_LOADER, BRIDGE, '--pair-only', '--session', sessionDir, ...extraArgs],
    {
      cwd: HERE,
      encoding: 'utf8',
      timeout: 10_000,
      env: {
        ...process.env,
        WHATSAPP_ALLOWED_USERS: '',
        WHATSAPP_MODE: 'self-chat',
        ...IDENTITY_SENTINELS,
        ...extraEnv,
      },
    },
  );
  result.sessionDir = sessionDir;
  return result;
}

function assertNoIdentity(output) {
  for (const sentinel of Object.values(IDENTITY_SENTINELS)) {
    assert.ok(!output.includes(sentinel), `bridge output leaked ${sentinel}`);
  }
  assert.ok(!output.includes('@s.whatsapp.net'), 'bridge output leaked a phone JID');
  assert.ok(!output.includes('@lid'), 'bridge output leaked a LID');
}

function reservePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(error => error ? reject(error) : resolve(port));
    });
  });
}

async function startHttpBridge() {
  const port = await reservePort();
  const sessionDir = mkdtempSync(path.join(tmpdir(), 'wa-privacy-http-'));
  const pollChatId = DIAGNOSTIC_SENTINELS.FAKE_WA_POLL_SUCCESS_CHAT;
  const child = spawn(
    process.execPath,
    ['--import', FAKE_BAILEYS_LOADER, BRIDGE, '--port', String(port), '--session', sessionDir, '--mode', 'bot'],
    {
      cwd: HERE,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        WHATSAPP_ALLOWED_USERS: '',
        WHATSAPP_DM_POLICY: 'pairing',
        WHATSAPP_MODE: 'bot',
        WHATSAPP_SEND_READ_RECEIPTS: 'true',
        WHATSAPP_FORWARD_OWNER_MESSAGES: 'true',
        WHATSAPP_DEBUG: '1',
        ...IDENTITY_SENTINELS,
        ...ERROR_SENTINELS,
        ...DIAGNOSTIC_SENTINELS,
        FAKE_WA_ERROR_MESSAGE: Object.values(ERROR_SENTINELS).join(' | '),
        FAKE_WA_EMIT_HOSTILE_DEBUG: '1',
      },
    },
  );

  let stdout = '';
  let stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk.toString('utf8'); });
  child.stderr.on('data', chunk => { stderr += chunk.toString('utf8'); });

  await Promise.race([
    new Promise((resolve, reject) => {
      const waitUntilConnected = () => {
        if (stdout.includes('WhatsApp connected!')) resolve();
        else if (child.exitCode !== null) reject(new Error(`bridge exited ${child.exitCode}: ${stderr}`));
        else setTimeout(waitUntilConnected, 10);
      };
      waitUntilConnected();
    }),
    new Promise((_, reject) => setTimeout(() => reject(new Error(`bridge startup timed out: ${stdout}\n${stderr}`)), 5_000)),
  ]);

  return {
    child,
    port,
    pollChatId,
    output: () => `${stdout}\n${stderr}`,
    async stop() {
      if (child.exitCode !== null) return;
      child.kill('SIGTERM');
      await new Promise(resolve => child.once('exit', resolve));
    },
  };
}

async function postJson(port, endpoint, body) {
  const response = await fetch(`http://127.0.0.1:${port}${endpoint}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() };
}

// Pair JSON is a public machine-readable stream. A successful connection is
// useful state, but the account identity is not part of that state contract.
{
  const result = runPairBridge(['--pair-json']);
  assert.equal(result.status, 0, `pair-only bridge must exit cleanly: ${result.stderr}`);

  const output = `${result.stdout}\n${result.stderr}`;
  assertNoIdentity(output);

  const events = result.stdout
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => JSON.parse(line));
  const connected = events.find(event => event.event === 'connected');
  assert.ok(connected, `expected a connected event in: ${result.stdout}`);
  assert.deepEqual(Object.keys(connected).sort(), ['event', 'ts']);
}

// Human pair output keeps its useful status lines without naming the account.
{
  const result = runPairBridge([]);
  assert.equal(result.status, 0, `pair-only bridge must exit cleanly: ${result.stderr}`);

  const output = `${result.stdout}\n${result.stderr}`;
  assertNoIdentity(output);
  assert.match(result.stdout, /WhatsApp connected/);
  assert.match(result.stdout, /Pairing complete/);
}

// Pair startup failures cross the same dashboard/CLI-visible output boundary.
// The thrown Error carries a path, JID and token, but both JSON and human modes
// must replace the entire value with bridge-owned text.
for (const pairJson of [true, false]) {
  const result = runPairBridge(pairJson ? ['--pair-json'] : [], {
    ...ERROR_SENTINELS,
    FAKE_WA_ERROR_MESSAGE: Object.values(ERROR_SENTINELS).join(' | '),
    FAKE_WA_START_FAILURE: '1',
  });
  assert.equal(result.status, 1, `synthetic pair startup must fail (${result.stdout}\n${result.stderr})`);

  const output = `${result.stdout}\n${result.stderr}`;
  assertNoIdentity(output);
  assert.ok(!output.includes(result.sessionDir), 'pair startup failure must not log its session path');
  for (const sentinel of Object.values(ERROR_SENTINELS)) {
    assert.ok(!output.includes(sentinel), `pair startup failure leaked ${sentinel}`);
  }

  if (pairJson) {
    const events = result.stdout
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean)
      .map(line => JSON.parse(line));
    const failure = events.find(event => event.event === 'error');
    assert.ok(failure, `pair JSON must retain a structural failure event: ${result.stdout}`);
    assert.deepEqual(Object.keys(failure).sort(), ['error', 'event', 'ts']);
    assert.equal(failure.error, 'WhatsApp pairing failed.');
  } else {
    assert.match(result.stderr, /WhatsApp pairing failed\./);
  }
}

// Every thrown-value boundary is exercised through the real HTTP handlers
// and socket event callbacks. The Error carries the sentinels both in its
// message and on structured fields so neither logging style is safe to echo.
{
  const bridge = await startHttpBridge();
  const mediaDir = mkdtempSync(path.join(tmpdir(), 'wa-SENTINEL_MEDIA_PATH-'));
  const documentPath = path.join(mediaDir, 'SENTINEL_DOCUMENT_PATH.bin');
  const gifPath = path.join(mediaDir, 'SENTINEL_GIF_PATH.gif');
  const audioPath = path.join(mediaDir, 'SENTINEL_AUDIO_PATH.mp3');
  const missingPath = path.join(mediaDir, 'SENTINEL_MISSING_PATH.gif');
  writeFileSync(documentPath, 'synthetic document');
  writeFileSync(gifPath, 'not a real gif');
  writeFileSync(audioPath, 'not real audio');

  let responses;
  try {
    const failingChat = ERROR_SENTINELS.FAKE_WA_ERROR_JID;
    responses = {
      send: await postJson(bridge.port, '/send', { chatId: failingChat, message: 'hello' }),
      edit: await postJson(bridge.port, '/edit', { chatId: failingChat, messageId: 'm1', message: 'hello' }),
      mediaMissing: await postJson(bridge.port, '/send-media', { chatId: failingChat, filePath: missingPath }),
      media: await postJson(bridge.port, '/send-media', { chatId: failingChat, filePath: documentPath, mediaType: 'document' }),
      poll: await postJson(bridge.port, '/send-poll', { chatId: failingChat, question: 'Q?', options: ['A', 'B'] }),
      location: await postJson(bridge.port, '/send-location', { chatId: failingChat, latitude: 1, longitude: 2 }),
      read: await postJson(bridge.port, '/read', {
        key: { remoteJid: failingChat, id: 'inbound-1', participant: failingChat, fromMe: false },
      }),
      gif: await postJson(bridge.port, '/send-media', { chatId: failingChat, filePath: gifPath, mediaType: 'image' }),
      audio: await postJson(bridge.port, '/send-media', { chatId: failingChat, filePath: audioPath, mediaType: 'audio' }),
      pollSeed: await postJson(bridge.port, '/send-poll', {
        chatId: bridge.pollChatId,
        question: 'Aggregation?',
        options: ['A', 'B'],
      }),
    };
    await new Promise(resolve => setTimeout(resolve, 150));
  } finally {
    await bridge.stop();
  }

  assert.deepEqual(responses, {
    send: { status: 500, body: { error: 'Failed to send message' } },
    edit: { status: 500, body: { error: 'Failed to edit message' } },
    mediaMissing: { status: 404, body: { error: 'File not found' } },
    media: { status: 500, body: { error: 'Failed to send media' } },
    poll: { status: 400, body: { error: 'Failed to send poll' } },
    location: { status: 400, body: { error: 'Failed to send location' } },
    read: { status: 500, body: { error: 'Failed to send read receipt' } },
    gif: { status: 500, body: { error: 'Failed to send media' } },
    audio: { status: 500, body: { error: 'Failed to send media' } },
    // Successful local transport responses intentionally retain their message
    // id. The privacy boundary under test is process diagnostics and fixed
    // error JSON, not functional API fields the gateway requires.
    pollSeed: {
      status: 200,
      body: { success: true, messageId: DIAGNOSTIC_SENTINELS.FAKE_WA_POLL_ID },
    },
  });

  const output = bridge.output();
  const forbidden = [
    ...Object.values(IDENTITY_SENTINELS),
    ...Object.values(ERROR_SENTINELS),
    ...Object.values(DIAGNOSTIC_SENTINELS),
    mediaDir,
    documentPath,
    gifPath,
    audioPath,
    missingPath,
    // Full-value assertions alone miss the old "redaction", which retained a
    // recognisable phone suffix and the complete JID domain.
    '0003',
    '0004',
    '0005',
    '@s.whatsapp.net',
    '@lid',
    'PRIVATE_OPTION_ALPHA',
  ];
  for (const sentinel of forbidden) {
    assert.ok(!output.includes(sentinel), `bridge process output leaked ${sentinel}`);
  }
  assert.match(output, /\[bridge\] failed to aggregate poll update\./);
  assert.match(output, /\[bridge\] failed to aggregate poll upsert\./);
  assert.match(output, /\[bridge\] gif conversion failed; sending as image\/gif\./);
  assert.match(output, /\[bridge\] audio conversion failed; sending as file attachment\./);
  assert.match(output, /\[bridge\] failed to send read receipt\./);

  const jsonLines = output
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });

  const upsertDiagnostic = jsonLines.find(event => event.event === 'debug' && event.stage === 'upsert');
  assert.ok(upsertDiagnostic, `hostile upsert must leave a structural diagnostic: ${output}`);
  assert.deepEqual(
    Object.keys(upsertDiagnostic).sort(),
    ['deliveryType', 'event', 'fromMe', 'stage'],
    'debug upsert diagnostics may expose only an allowlisted type and a boolean',
  );

  const queuedDiagnostic = jsonLines.find(event => event.event === 'debug' && event.stage === 'queued');
  assert.ok(queuedDiagnostic, `queued message must leave a structural diagnostic: ${output}`);
  assert.deepEqual(
    Object.keys(queuedDiagnostic).sort(),
    ['bodyLength', 'event', 'fromOwner', 'hasMedia', 'queueLength', 'stage'],
    'queued diagnostics may expose booleans and bounded counts, never body/media values',
  );

  const foreignPollDiagnostic = jsonLines.find(
    event => event.event === 'debug' && event.stage === 'ignored' && event.reason === 'foreign_poll_update',
  );
  assert.deepEqual(
    Object.keys(foreignPollDiagnostic || {}).sort(),
    ['event', 'reason', 'stage'],
    'foreign-poll diagnostics must retain the reason but no poll id',
  );

  const ordinaryIgnoredDiagnostic = jsonLines.find(
    event => event.event === 'ignored' && event.reason === 'allowlist_mismatch_owner_chat',
  );
  assert.deepEqual(
    ordinaryIgnoredDiagnostic,
    { event: 'ignored', reason: 'allowlist_mismatch_owner_chat' },
    'ordinary always-on rejection diagnostics must retain only the fixed policy reason',
  );

  const pollDiagnostics = jsonLines.filter(event => event.event === 'poll_update_decode');
  assert.ok(pollDiagnostics.some(event => event.source === 'messages.update'));
  assert.ok(pollDiagnostics.some(event => event.source === 'messages.upsert'));
  for (const event of pollDiagnostics) {
    assert.deepEqual(
      Object.keys(event).sort(),
      [
        'aggregationOptionCount',
        'aggregationVoterCount',
        'event',
        'hasVote',
        'pollCreationFound',
        'selectedOptionCount',
        'source',
        'updateCount',
      ],
      'poll diagnostics must contain structure only',
    );
    for (const [key, value] of Object.entries(event)) {
      if (key === 'event' || key === 'source') continue;
      if (typeof value === 'number') {
        assert.ok(Number.isInteger(value) && value >= 0 && value <= 1000, `${key} must be a bounded count`);
      } else {
        assert.equal(typeof value, 'boolean', `${key} must be a boolean or bounded count`);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// A pairing QR is rendered only on a terminal, never into a redirected sink.
//
// The managed bridge is spawned by the adapter with stdout/stderr pointed at
// bridge.log. If Baileys reaches a QR state there, rendering it would persist
// live pairing material. The bridge must stop instead.
// ---------------------------------------------------------------------------
{
  // Shaped like a Baileys QR payload but wholly fabricated.
  const FAKE_QR = '2@FAKEQRCANARY0000/SyntheticPairingRef,FAKEPUBKEYCANARY0000=,FAKEIDKEYCANARY0000=';

  // spawnSync gives the child a pipe, so process.stdout.isTTY is false —
  // exactly the managed/redirected case.
  const result = runPairBridge([], { FAKE_WA_QR: FAKE_QR });
  const output = `${result.stdout || ''}${result.stderr || ''}`;

  assert.ok(
    !output.includes(FAKE_QR),
    'bridge wrote a raw QR payload to a non-tty sink',
  );
  assert.ok(
    !output.includes('Scan this QR code'),
    'bridge rendered QR prompt to a non-tty sink',
  );
  assert.equal(
    result.status,
    79,
    'bridge must exit BRIDGE_EXIT_REPAIR_REQUIRED instead of printing a QR',
  );
  assert.ok(
    output.includes('needs to be paired again'),
    'bridge must report generic re-pair-required state',
  );
  assertNoIdentity(output);

  // Nothing may have been persisted into the session directory either.
  const persisted = readdirSync(result.sessionDir, { recursive: true })
    .map(entry => path.join(result.sessionDir, String(entry)))
    .filter(entry => statSync(entry).isFile())
    .map(entry => readFileSync(entry, 'utf8'))
    .join('\n');
  assert.ok(!persisted.includes(FAKE_QR), 'QR payload was persisted to the session dir');
}

// ---------------------------------------------------------------------------
// --pair-json must not bypass the non-tty rule.
//
// The JSON stream is consumed by the dashboard/background pairing watcher and
// is written to a pipe, never a terminal. Carrying the QR there moves live
// pairing material off the operator's own terminal and into a process that
// stores and re-serves it. The payload must not appear in any field, and no
// `event:"qr"` record may be produced at all.
// ---------------------------------------------------------------------------
{
  const FAKE_QR = '2@FAKEQRCANARY0001/SyntheticPairJsonRef,FAKEPUBKEYCANARY0001=';

  const result = runPairBridge(['--pair-json'], { FAKE_WA_QR: FAKE_QR });
  const output = `${result.stdout || ''}${result.stderr || ''}`;

  assert.ok(!output.includes(FAKE_QR), '--pair-json emitted a raw QR payload');

  const events = output
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .flatMap(line => {
      try { return [JSON.parse(line)]; } catch { return []; }
    });

  assert.ok(
    !events.some(event => event.event === 'qr'),
    '--pair-json emitted an event:"qr" record',
  );
  // No field anywhere may carry the payload.
  assert.ok(
    !events.some(event => JSON.stringify(event).includes(FAKE_QR)),
    '--pair-json leaked the QR payload through another field',
  );
  assert.ok(
    events.some(event => event.event === 'repair_required'),
    '--pair-json must emit a fixed generic repair_required event',
  );
  assert.equal(result.status, 79, '--pair-json must exit BRIDGE_EXIT_REPAIR_REQUIRED');
  assertNoIdentity(output);

  const persisted = readdirSync(result.sessionDir, { recursive: true })
    .map(entry => path.join(result.sessionDir, String(entry)))
    .filter(entry => statSync(entry).isFile())
    .map(entry => readFileSync(entry, 'utf8'))
    .join('\n');
  assert.ok(!persisted.includes(FAKE_QR), '--pair-json persisted the QR payload');
}

// ---------------------------------------------------------------------------
// Credential artifacts the pairing path creates are owner-only, whatever the
// ambient umask happens to be.
//
// Baileys writes creds.json with a plain write, so the resulting mode is
// `0666 & ~umask`. On a normal developer/service host that is 0644 — a
// world-readable WhatsApp credential. The bridge must therefore establish its
// own owner-only umask rather than inherit whatever it was started with.
// ---------------------------------------------------------------------------
{
  const previousUmask = process.umask(0o000); // maximally permissive ambient
  let result;
  try {
    result = runPairBridge([], { FAKE_WA_WRITE_CREDS: '1' });
  } finally {
    process.umask(previousUmask);
  }

  const sessionDir = result.sessionDir;
  const creds = path.join(sessionDir, 'creds.json');
  assert.ok(existsSync(creds), 'fixture did not produce a credential artifact');

  const credsMode = statSync(creds).mode & 0o777;
  assert.equal(
    credsMode.toString(8).padStart(4, '0'),
    '0600',
    'bridge-created credentials must be owner read/write only',
  );

  const dirMode = statSync(path.join(sessionDir, 'auth')).mode & 0o777;
  assert.equal(
    dirMode.toString(8).padStart(4, '0'),
    '0700',
    'bridge-created session directory must be owner-only',
  );
}

console.log('bridge.privacy.test.mjs: all assertions passed');
