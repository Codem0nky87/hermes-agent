/**
 * Tests for the logger handed to Baileys.
 *
 * Baileys logs generously on the failure paths that matter most: it passes
 * whole binary stream nodes, Boom errors that still carry those nodes on
 * `.data`, and slices of auth state into `logger.warn`/`logger.error`. A
 * plain pino instance serialises all of it, so raising the bridge's log level
 * to see why a session died would also dump server payload and key material
 * into a file the dashboard reads back.
 *
 * These tests drive the real logger — a real pino instance behind the bounded
 * wrapper, writing to a captured stream — with synthetic sentinel secrets, at
 * the levels operators actually enable.
 */

import { strict as assert } from 'node:assert';
import { Writable } from 'node:stream';

import { Boom } from '@hapi/boom';

import { boundedLogText, createBaileysLogger } from './baileys_logger.js';

/** A sentinel that must never survive the logger. */
const SECRET = 'SENTINEL_SECRET_MUST_NOT_LEAK';

/** The stream node WhatsApp sends, with server payload salted through it. */
const streamNode = () => ({
  tag: 'stream:error',
  attrs: { code: '401', token: SECRET },
  content: [{ tag: 'conflict', attrs: { type: 'device_removed', secret: SECRET } }],
});

/** Auth state of the shape Baileys hands to its logger on key failures. */
const authState = () => ({
  creds: {
    noiseKey: { private: SECRET, public: SECRET },
    signedIdentityKey: { private: SECRET },
    me: { id: '27820000000@s.whatsapp.net', name: SECRET },
  },
  keys: { 'session-27820000000': SECRET },
});

function captureLogger(options = {}) {
  const chunks = [];
  const destination = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(chunk.toString('utf8'));
      callback();
    },
  });
  const logger = createBaileysLogger({ destination, ...options });
  return { logger, output: () => chunks.join('') };
}

// -- nothing server-supplied or secret survives, at any enabled level ------
{
  for (const level of ['warn', 'error']) {
    const { logger, output } = captureLogger({ level });

    logger[level](streamNode(), `stream errored ${SECRET}`);
    logger[level]({ err: new Boom(`Stream Errored (${SECRET})`, { statusCode: 401, data: streamNode() }) }, `closing ${SECRET}`);
    logger[level](new Boom(`Stream Errored (${SECRET})`, { statusCode: 401, data: streamNode() }), `closing ${SECRET}`);
    logger[level](authState(), `auth state ${SECRET}`);
    logger[level]({ node: streamNode(), creds: authState().creds }, `decrypt failed ${SECRET}`);
    logger.child({ sessionKey: SECRET, class: 'baileys' })[level]({ token: SECRET }, 'child line');

    const text = output();
    assert.ok(text.length > 0, `level ${level} must still produce output`);
    assert.ok(!text.includes(SECRET), `level ${level} leaked the sentinel secret`);
    assert.ok(!text.includes('s.whatsapp.net'), `level ${level} leaked an account identifier`);
    assert.ok(!text.includes('noiseKey'), `level ${level} leaked a key field name`);
    assert.ok(!text.includes('signedIdentityKey'), `level ${level} leaked a key field name`);
    assert.ok(!text.includes('stream:error'), `level ${level} leaked the raw stream node`);
    assert.ok(!text.includes('stack'), `level ${level} leaked an error stack`);
  }
}

// -- no caller-supplied VALUE survives, whatever it is named --------------
//
// An allowlist of field NAMES is not a boundary. It decides what to copy from
// the name a caller chose, while the thing that must not escape is the VALUE
// a caller supplied — and Baileys does not own those values either; they come
// off the wire. A secret parked in `reason`, `detail`, `type`, `code`,
// `class` or `statusCode` is copied out verbatim by a name-based rule, and so
// is a secret that happens to be a number, since numbers skip string
// sanitising entirely.
//
// So the logger keeps NOTHING a caller passed. That costs nothing
// operationally: the disconnect diagnostics an operator acts on — the status
// code and the sanitised reason tags — are emitted by the bridge's own
// connection-close handler, from metadata it extracted and bounded itself
// (see connection_close.js and bridge.connection.test.mjs). This logger only
// has to say that Baileys logged something, and at what level.
{
  const NUMERIC_SENTINEL = 27820000001;
  const SHORT_TAG_SECRET = 'devicekey';

  for (const level of ['warn', 'error']) {
    const { logger, output } = captureLogger({ level });

    // Values shaped exactly like the diagnostics a name-based allowlist keeps.
    logger[level]({ reason: SHORT_TAG_SECRET, detail: SHORT_TAG_SECRET, type: SHORT_TAG_SECRET }, 'closed');
    logger[level]({ class: SHORT_TAG_SECRET, code: SHORT_TAG_SECRET }, 'closed');
    logger[level]({ statusCode: NUMERIC_SENTINEL, code: NUMERIC_SENTINEL, reason: NUMERIC_SENTINEL }, 'closed');
    logger[level]({ err: { statusCode: NUMERIC_SENTINEL, reason: SHORT_TAG_SECRET } }, 'closed');
    logger[level]({ error: { output: { statusCode: NUMERIC_SENTINEL } } }, 'closed');

    // A Boom still carries the node on `.data`, and its own status is just as
    // caller-supplied as anything else here.
    logger[level](new Boom('Stream Errored (conflict)', { statusCode: 401, data: streamNode() }), 'closed');

    // Custom fields hung off an Error survive `instanceof Error` handling.
    const custom = new Error('transport failed');
    Object.assign(custom, { reason: SHORT_TAG_SECRET, statusCode: NUMERIC_SENTINEL, sessionKey: SECRET });
    logger[level](custom, 'closed');

    const text = output();
    assert.ok(text.length > 0, `level ${level} must still produce output`);
    assert.ok(!text.includes(SHORT_TAG_SECRET), `level ${level} kept a tag-shaped caller value`);
    assert.ok(!text.includes(String(NUMERIC_SENTINEL)), `level ${level} kept a numeric caller value`);
    assert.ok(!text.includes('401'), `level ${level} kept a caller-supplied status code`);
    assert.ok(!text.includes(SECRET));
    assert.ok(!text.includes('transport failed'), `level ${level} kept an Error message`);

    // What remains is the bridge's own line, and pino's ordinary metadata.
    for (const line of text.trim().split('\n')) {
      assert.deepEqual(
        Object.keys(JSON.parse(line)).sort(),
        ['level', 'msg', 'time'],
        `level ${level} emitted a field the bridge does not own: ${line}`,
      );
      assert.equal(JSON.parse(line).msg, `WhatsApp transport ${level}`);
    }
  }
}

// Child bindings are caller-supplied too, and worse: they ride along on every
// subsequent line rather than just the one that carried them.
{
  const { logger, output } = captureLogger({ level: 'warn' });
  const child = logger.child({ class: 'baileys', sessionKey: SECRET, statusCode: 27820000001 });
  child.error({}, 'from the child');
  child.child({ reason: 'devicekey' }).error({}, 'from the grandchild');

  const text = output();
  assert.ok(!text.includes(SECRET), 'a child binding must not leak a secret');
  assert.ok(!text.includes('27820000001'), 'a child binding must not leak a numeric value');
  assert.ok(!text.includes('devicekey'), 'a grandchild binding must not leak a value');
  assert.ok(!text.includes('baileys'), 'even a harmless-looking binding value is caller-supplied');
  for (const line of text.trim().split('\n')) {
    assert.deepEqual(Object.keys(JSON.parse(line)).sort(), ['level', 'msg', 'time']);
  }
}

// The level still distinguishes the lines, which is the whole of what the
// bridge log needs from this logger.
{
  const { logger, output } = captureLogger({ level: 'trace' });
  logger.warn({}, 'x');
  logger.error({}, 'x');
  const msgs = output().trim().split('\n').map(line => JSON.parse(line).msg);
  assert.deepEqual(msgs, ['WhatsApp transport warn', 'WhatsApp transport error']);
}

// -- arbitrary messages are replaced --------------------------------------
//
// Baileys can interpolate server-provided values into Error.message and log
// strings. A length cap would still leak a short token, so the wrapper keeps
// only bridge-owned text.
{
  const { logger, output } = captureLogger({ level: 'warn' });
  logger.error({}, `${SECRET}\n[bridge] forged second line`);

  const text = output();
  assert.ok(!text.includes(SECRET), 'the source message must not survive');
  assert.ok(!text.includes('forged second line'), 'source text must be discarded wholesale');
  assert.match(text, /WhatsApp transport error/);
}

// The bridge startup catch uses the same closed boundary for thrown values.
{
  assert.equal(
    boundedLogText(new Error(`pairing failed: ${SECRET}`), 'WhatsApp pairing failed.'),
    'WhatsApp pairing failed.',
  );
}

// -- level gating still works ---------------------------------------------
//
// The wrapper must not accidentally promote suppressed levels to visible
// output: the bridge runs Baileys at `warn` precisely to stay quiet.
{
  const { logger, output } = captureLogger({ level: 'warn' });
  logger.trace({}, 'trace line');
  logger.debug({}, 'debug line');
  logger.info({}, 'info line');
  assert.equal(output(), '', 'levels below the configured one must produce nothing');

  logger.warn({}, 'warn line');
  assert.match(output(), /WhatsApp transport warn/);
  assert.ok(!output().includes('warn line'), 'source message text must not survive');
}

// -- the interface Baileys actually calls ---------------------------------
{
  const { logger } = captureLogger({ level: 'warn' });
  for (const method of ['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'child']) {
    assert.equal(typeof logger[method], 'function', `Baileys requires logger.${method}()`);
  }
  assert.equal(logger.level, 'warn', 'Baileys reads logger.level');
  const child = logger.child({ class: 'baileys' });
  assert.equal(typeof child.child, 'function', 'a child logger must itself be a logger');
  assert.equal(child.level, 'warn');
}

console.log('bridge.logger.test.mjs: all assertions passed');
