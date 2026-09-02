/**
 * The bridge's `connection: 'close'` orchestration.
 *
 * Extracted from bridge.js so it can be exercised directly: bridge.js opens
 * an HTTP server and a Baileys socket at module load, which makes the close
 * path — the one that decides whether a session is dead — the hardest part of
 * the bridge to test in place. Everything it touches (the pair-event stream,
 * the operator log, the reconnect scheduler, process exit) arrives by
 * injection; bridge.js supplies the real ones.
 */

import {
  classifyDisconnectReason,
  extractDisconnectMetadata,
  writeSessionRevokedMarker,
} from './bridge_helpers.js';

/**
 * Render disconnect metadata as a short, log-safe phrase (or null).
 *
 * Both inputs are already sanitised reason tags, so the result is bounded and
 * free of anything the server chose.
 */
export function describeDisconnect({ reason, detail } = {}) {
  return [reason, detail].filter(Boolean).join(': ') || null;
}

/**
 * Build the handler bridge.js installs for `connection: 'close'`.
 *
 * @param {object} deps
 * @param {string} deps.sessionDir      Directory holding this session's auth state.
 * @param {(event: object) => void} deps.emitPairEvent  Pair-event JSON sink.
 * @param {(line: string) => void} deps.log             Human-facing log sink.
 * @param {(delayMs: number, options?: object) => void} deps.scheduleReconnect
 * @param {(code: number) => void} deps.exit            Terminates the process.
 * @param {Function} [deps.writeMarker]  Overridable for tests; defaults to the
 *   real atomic 0600 writer.
 */
export function createConnectionCloseHandler({
  sessionDir,
  emitPairEvent,
  log,
  scheduleReconnect,
  exit,
  writeMarker = writeSessionRevokedMarker,
}) {
  return function handleConnectionClose(lastDisconnect) {
    // Baileys supplies a Boom here, including the original binary node on
    // `.data`. Extract directly so no wrapper can discard that evidence. Only
    // bounded status/reason fields come back out — the node itself is never
    // logged or emitted, since it is server-supplied and may hold anything.
    const disconnect = extractDisconnectMetadata(lastDisconnect?.error);
    const reason = disconnect.statusCode;
    const streamReason = describeDisconnect(disconnect);
    const decision = classifyDisconnectReason(disconnect);

    if (!decision.terminal) {
      emitPairEvent({ event: 'disconnected', reason, streamReason });
      if (reason === 515) {
        log('↻ WhatsApp requested restart (code 515). Reconnecting...');
        scheduleReconnect(decision.delayMs, { prompt: true });
      } else {
        const shown = streamReason ? `${reason} ${streamReason}` : `${reason}`;
        log(`⚠️  Connection closed (reason: ${shown}). Reconnecting with backoff...`);
        scheduleReconnect(decision.delayMs);
      }
      return;
    }

    // Record the verdict BEFORE exiting, synchronously and atomically, so it
    // outlives this process: the exit code alone is invisible to anyone who
    // is not watching this child (notably the reused/external-bridge path,
    // where Hermes never sees an exit code at all). A failed write is logged
    // and does not stop the exit — the exit code is still worth something to
    // a caller that is watching.
    writeMarker(sessionDir, disconnect, { log });
    emitPairEvent({ event: 'error', error: 'logged_out', reason, streamReason });
    log(`❌ WhatsApp revoked this device${streamReason ? ` (${streamReason})` : ''}.`);
    log('   Re-pair with `hermes whatsapp` — reconnecting cannot recover this session.');
    // Distinct exit code so the Python adapter can mark this non-retryable
    // instead of treating it as an ordinary crash.
    exit(decision.exitCode);
  };
}
