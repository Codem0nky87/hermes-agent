/**
 * Unit tests for the reconnect scheduling and version resolution guards.
 *
 * Regression tests for the reconnect-wedge trap: startSocket() awaits network
 * I/O (fetchLatestBaileysVersion has no AbortSignal) before it creates a
 * socket, and the close handler used to re-enter it via a bare
 * `setTimeout(startSocket, ...)`. A rejection was then unhandled and a stalled
 * fetch left the bridge permanently disconnected while its HTTP server kept
 * answering 503 — one "Reconnecting in 3s..." line and then silence.
 *
 * These tests avoid importing bridge.js because that file starts an HTTP
 * server and Baileys socket at module load. Keep the helper module pure.
 *
 * Revocation classification and the revoked-session marker live in
 * bridge.revocation.test.mjs; the production close orchestration that drives
 * them lives in bridge.connection.test.mjs.
 */

import { strict as assert } from 'node:assert';

import {
  createReconnectScheduler,
  createVersionResolver,
} from './bridge_helpers.js';

const tick = () => new Promise(resolve => setImmediate(resolve));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));


// -- createReconnectScheduler ---------------------------------------------

// Ordinary closes and start failures share one bounded exponential sequence.
// A successful start alone does not reset it: only a confirmed connection
// open may do that, via resetBackoff().
{
  const secret = 'SENTINEL_RECONNECT_SECRET';
  const timers = [];
  const logs = [];
  let attempts = 0;
  const startFn = async () => {
    attempts += 1;
    if (attempts === 1) throw new Error(secret);
  };

  const schedule = createReconnectScheduler(startFn, {
    baseDelayMs: 3000,
    maxDelayMs: 12000,
    log: line => logs.push(line),
    setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
  });

  assert.equal(schedule(3000), 3000);
  assert.equal(timers.length, 1);
  assert.equal(timers[0].ms, 3000);

  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(attempts, 1);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /Reconnect failed/);
  assert.ok(!logs[0].includes(secret), 'reconnect errors must not reflect arbitrary messages');
  assert.equal(timers.length, 1, 'rejection must schedule a retry');
  assert.equal(timers[0].ms, 6000);

  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(attempts, 2);
  assert.equal(timers.length, 0, 'success must not schedule another attempt');
  assert.equal(logs.length, 1);

  assert.equal(schedule(3000), 12000, 'the next close must continue the shared sequence');
  assert.equal(timers[0].ms, 12000);
  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(schedule(3000), 12000, 'ordinary retries must remain capped');
  timers.shift().fn();
  await tick();
  await tick();

  schedule.resetBackoff();
  assert.equal(schedule(3000), 3000, 'confirmed open must reset the sequence');
  assert.equal(timers[0].ms, 3000);
}

// A synchronous throw from the start function is contained the same way as
// an async rejection.
{
  const secret = 'SENTINEL_SYNC_RECONNECT_SECRET';
  const timers = [];
  const logs = [];
  const schedule = createReconnectScheduler(
    () => { throw new Error(secret); },
    {
      baseDelayMs: 2000,
      maxDelayMs: 8000,
      log: line => logs.push(line),
      setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
    },
  );

  assert.equal(schedule(0, { initial: true }), 0, 'initial startup must be immediate');
  timers[0].fn();
  await tick();
  await tick();

  assert.equal(logs.length, 1);
  assert.match(logs[0], /Reconnect failed/);
  assert.ok(!logs[0].includes(secret), 'sync reconnect errors must not reflect arbitrary messages');
  assert.equal(timers.length, 2);
  assert.equal(timers[1].ms, 2000);
}

// Repeated close notifications cannot create parallel timers or starts. A
// close that arrives during the one in-flight start is parked once and armed
// only after that start settles.
{
  const timers = [];
  let starts = 0;
  let finishStart;
  const schedule = createReconnectScheduler(
    () => {
      starts += 1;
      return new Promise(resolve => { finishStart = resolve; });
    },
    {
      baseDelayMs: 3000,
      maxDelayMs: 12000,
      log: () => {},
      setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
    },
  );

  assert.equal(schedule(3000), 3000);
  assert.equal(schedule(3000), false);
  assert.equal(schedule(1000, { prompt: true }), false);
  assert.equal(timers.length, 1, 'only one timer may be pending');

  timers.shift().fn();
  await tick();
  assert.equal(starts, 1);
  assert.equal(schedule(3000), false);
  assert.equal(schedule(3000), false);
  assert.equal(timers.length, 0, 'no timer may overlap an in-flight start');

  finishStart();
  await tick();
  await tick();
  assert.equal(timers.length, 1, 'one parked close must survive the in-flight start');
  assert.equal(timers[0].ms, 6000);
}

// Initial startup is immediate without consuming the failure streak. The
// first 515 in that streak may use the one prompt retry; every later 515 or
// ordinary failure advances through the same bounded backoff and can never
// regress to one second.
{
  const timers = [];
  const schedule = createReconnectScheduler(async () => {}, {
    baseDelayMs: 3000,
    maxDelayMs: 12000,
    log: () => {},
    setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
  });

  assert.equal(schedule(0, { initial: true }), 0);
  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(schedule(1000, { prompt: true }), 1000, 'the first 515 may retry promptly');
  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(schedule(1000, { prompt: true }), 3000, 'a second 515 must enter backoff');
  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(schedule(1000, { prompt: true }), 6000, 'later 515 retries must keep escalating');
  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(schedule(3000), 12000, 'an ordinary close shares the same failure sequence');
  timers.shift().fn();
  await tick();
  await tick();

  assert.equal(schedule(1000, { prompt: true }), 12000, 'a 515 may not regress from the cap');
  timers.shift().fn();
  await tick();
  await tick();

  schedule.resetBackoff();
  assert.equal(schedule(1000, { prompt: true }), 1000, 'a confirmed open restores one prompt retry');
}

// If an ordinary close starts the streak, a later 515 cannot jump backwards
// to the prompt delay. It consumes the next shared exponential step instead.
{
  const timers = [];
  const schedule = createReconnectScheduler(async () => {}, {
    baseDelayMs: 3000,
    maxDelayMs: 12000,
    log: () => {},
    setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
  });

  assert.equal(schedule(3000), 3000);
  timers.shift().fn();
  await tick();
  await tick();
  assert.equal(schedule(1000, { prompt: true }), 6000, '515 must not regress after ordinary backoff began');
}

// -- createVersionResolver ------------------------------------------------

// A confirmed fetch returns a candidate. It is not promoted until the socket
// reaches connection:'open' and the bridge explicitly confirms that exact
// candidate.
{
  const resolveVersion = createVersionResolver(
    async () => ({ version: [2, 3000, 99], isLatest: true }),
    { log: () => {} },
  );
  const candidate = await resolveVersion();
  assert.deepEqual(candidate, [2, 3000, 99]);
  assert.equal(resolveVersion.confirm(candidate), true, 'connection open must promote the exact pending candidate');
}

// rc13 reports fetch/parsing failures as a RESOLVED object carrying its
// compiled-in version, isLatest:false, and an error. Resolution is not proof:
// this soft failure must use the last open-confirmed version (or the library
// default), and neither the candidate nor its arbitrary error may be cached.
{
  const secret = 'SENTINEL_SOFT_VERSION_FAILURE';
  const softCandidate = [2, 3000, 777];
  const logs = [];
  let calls = 0;
  const resolveVersion = createVersionResolver(
    async () => {
      calls += 1;
      if (calls === 1) {
        return { version: softCandidate, isLatest: false, error: new Error(secret) };
      }
      throw new Error(secret);
    },
    { timeoutMs: 20, log: line => logs.push(line) },
  );

  assert.equal(await resolveVersion(), null, 'a resolved soft failure must fall back to the library default');
  assert.equal(await resolveVersion(), null, 'the soft-failure candidate must not poison a later fallback');
  assert.ok(!logs.join('\n').includes(secret), 'soft-failure errors must not reach diagnostics');
  assert.ok(!logs.join('\n').includes('777'), 'an unconfirmed candidate must not reach diagnostics');
}

// Even an isLatest:true candidate remains provisional until open. If startup
// fails before that event, the next failed fetch falls back instead of reusing
// the candidate that may have caused the failure.
{
  const candidate = [2, 3000, 778];
  let calls = 0;
  const resolveVersion = createVersionResolver(
    async () => {
      calls += 1;
      if (calls === 1) return { version: candidate, isLatest: true };
      throw new Error('synthetic startup failure');
    },
    { timeoutMs: 20, log: () => {} },
  );

  assert.deepEqual(await resolveVersion(), candidate, 'the fetched candidate may be tried once');
  assert.equal(await resolveVersion(), null, 'an unconfirmed candidate must not become the fallback');
  assert.equal(resolveVersion.confirm(candidate), false, 'a discarded candidate cannot be promoted by a late open');
}

// A fetch that never settles resolves within the timeout bound instead of
// pending forever; before any success there is no cache, so the resolver
// yields null (callers fall back to the Baileys default).
{
  const logs = [];
  const resolveVersion = createVersionResolver(
    () => new Promise(() => {}),
    { timeoutMs: 20, log: line => logs.push(line) },
  );
  assert.equal(await resolveVersion(), null);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /version fetch failed/i);
  assert.match(logs[0], /library default/);
}

// After connection open confirms a candidate, later failures fall back to it.
{
  const secret = 'SENTINEL_VERSION_FETCH_SECRET';
  const logs = [];
  let calls = 0;
  const resolveVersion = createVersionResolver(
    async () => {
      calls += 1;
      if (calls === 1) return { version: [2, 3000, 42], isLatest: true };
      throw new Error(secret);
    },
    { timeoutMs: 20, log: line => logs.push(line) },
  );
  const candidate = await resolveVersion();
  assert.deepEqual(candidate, [2, 3000, 42]);
  assert.equal(resolveVersion.confirm(candidate), true);
  assert.deepEqual(await resolveVersion(), [2, 3000, 42]);
  assert.equal(logs.length, 1);
  assert.ok(!logs[0].includes(secret), 'version errors must not reflect arbitrary messages');
  assert.match(logs[0], /cached version/);
}

// The losing timeout timer is cleared after a fast success, so the resolver
// does not hold the event loop open for the full timeout window.
{
  const resolveVersion = createVersionResolver(
    async () => ({ version: [2, 3000, 1], isLatest: true }),
    { timeoutMs: 60_000, log: () => {} },
  );
  const before = Date.now();
  await resolveVersion();
  await sleep(10);
  assert.ok(Date.now() - before < 1000);
}

console.log('bridge.reconnect.test.mjs: all assertions passed');
