/**
 * The logger the bridge hands to Baileys.
 *
 * Baileys logs generously on exactly the paths that carry sensitive data: it
 * passes whole binary stream nodes, Boom errors that still hold those nodes
 * on `.data`, and slices of auth state straight to `logger.warn`/`error`. A
 * plain pino instance serialises every one of those arguments, so raising the
 * level to diagnose a dead session would also write server payload and key
 * material into the bridge log — a file the dashboard reads back.
 *
 * So nothing a caller passes is logged. Not the message, not the fields, not
 * the child bindings — only the level, and a line the bridge wrote itself.
 *
 * WHY NOT AN ALLOWLIST OF FIELD NAMES, the obvious middle ground: an
 * allowlist decides what to keep from the NAME a caller chose, but the thing
 * that must not escape is the VALUE, and Baileys does not own those values
 * either — they come off the wire. `reason`, `detail`, `type`, `code`,
 * `class` and `statusCode` are all names a caller can park a secret under,
 * and a numeric secret skips string sanitising altogether. Keeping the set
 * empty makes the boundary decidable instead of a judgement call about which
 * names are safe today.
 *
 * Nothing is lost by it. The disconnect diagnostics an operator acts on — the
 * status code and the sanitised reason tags — are emitted by the bridge's own
 * connection-close handler from metadata it extracted and bounded itself
 * (connection_close.js). This logger only has to report that Baileys logged
 * something, and at what level.
 */

import pino from 'pino';

/** Levels the wrapper forwards; `fatal` is included for pino API parity. */
const LEVELS = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'];

/**
 * Replace an arbitrary thrown value with caller-owned text.
 *
 * Even an Error.message is untrusted here: transport libraries can interpolate
 * server payload or identifiers into it. The fallback is a fixed string owned
 * by the bridge, so neither the object graph nor a secret-bearing message can
 * reach stdout.
 */
export function boundedLogText(_value, fallback) {
  return typeof fallback === 'string' && fallback ? fallback : 'WhatsApp bridge error.';
}

/**
 * Wrap a pino instance so that no argument reaching it is ever serialised.
 *
 * Every call signature Baileys uses — `(msg)`, `(obj, msg)`, `(err, msg)`,
 * `({ err }, msg)` — collapses to the same fixed line, because none of those
 * arguments is bridge-owned. `child()` returns another wrapper rather than a
 * pino child, so bindings are discarded at the point they are offered instead
 * of being attached to every later line.
 */
function boundedLogger(target) {
  const logger = {
    get level() { return target.level; },
    set level(value) { target.level = value; },
    child: () => boundedLogger(target),
  };
  for (const level of LEVELS) {
    logger[level] = () => target[level](`WhatsApp transport ${level}`);
  }
  return logger;
}

/**
 * Create the bounded logger.
 *
 * @param {object} [options]
 * @param {string} [options.level]        pino level; the bridge runs at `warn`.
 * @param {object} [options.destination]  Writable stream; defaults to pino's
 *   (stdout), overridden in tests to capture output.
 */
export function createBaileysLogger({ level = 'warn', destination } = {}) {
  // `base: null` drops pino's default pid/hostname bindings — machine
  // identity is not diagnostics the bridge log needs to carry.
  return boundedLogger(pino({ level, base: null }, destination));
}
