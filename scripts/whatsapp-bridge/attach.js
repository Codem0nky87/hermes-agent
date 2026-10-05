/**
 * WhatsApp batch-attach protocol for the Hermes bridge.
 *
 * Owner flow (one chat at a time):
 *   /attach start   -> one acknowledgement ("waiting for attachments")
 *   <media> ...     -> one short receipt per file (name + size), nothing more
 *   /attach stop    -> numbered manifest summary + a question asking what to
 *                      do with the files (the files are NOT described or
 *                      inspected by the bridge)
 *
 * All protocol replies are sent straight from the bridge and the messages
 * are never pushed into the agent queue. That keeps the agent from treating
 * the first attachment of a large burst as the thing to focus on; the agent
 * only wakes up once, for the stop summary, and is asked first.
 *
 * Downloaded files stay in the usual media cache directories; the per-chat
 * manifest (next to the bridge session dir) records batch membership,
 * original names, sizes, and cache paths so the agent can act on them
 * without re-asking.
 *
 * This module is deliberately pure (no socket, no HTTP) so it can be unit
 * tested without loading bridge.js.
 */

import path from 'path';
import { readFileSync, writeFileSync, mkdirSync } from 'fs';

const ATTACH_START_RE = /^\/attach\s+start$/i;
const ATTACH_STOP_RE = /^\/attach\s+stop$/i;
const ATTACH_USAGE_RE = /^\/attach(\s+\S+)?$/i;

export const MAX_MANIFEST_BATCHES = 20;
export const MAX_SUMMARY_LINES = 50;

export const ATTACH_START_ACK =
  '📥 Attachments: waiting for them. I will confirm each one as it lands — send /attach stop when you are done.';
export const ATTACH_NOT_COLLECTING =
  'No attach batch in progress. Send /attach start first.';
export const ATTACH_USAGE =
  'Usage: /attach start — then send your files — then /attach stop.';

/**
 * Classify a message body as an attach command.
 * Returns 'start' | 'stop' | 'usage' | null (not an attach command).
 * Case-insensitive, tolerant of surrounding whitespace, strict otherwise —
 * a caption or a sentence that merely contains "/attach" must not trigger.
 */
export function parseAttachCommand(text) {
  const t = String(text ?? '').trim().toLowerCase();
  if (ATTACH_START_RE.test(t)) return 'start';
  if (ATTACH_STOP_RE.test(t)) return 'stop';
  if (ATTACH_USAGE_RE.test(t)) return 'usage';
  return null;
}

/** 1536 -> "1.5 KB", 512 -> "512 B", -1 -> "". */
export function humanSize(bytes) {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n < 0) return '';
  if (n < 1024) return `${Math.round(n)} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  const s = v >= 100 ? Math.round(v) : Math.round(v * 10) / 10;
  return `${s} ${units[i]}`;
}

/**
 * Best display name for a downloaded inbound media event: the original
 * document fileName when WhatsApp supplied one, else the cache file's
 * basename, else a type-tagged fallback.
 */
export function displayNameForEvent({ fileName, mediaType, mediaUrls, messageId }) {
  if (fileName) return String(fileName);
  if (Array.isArray(mediaUrls) && mediaUrls.length > 0) {
    const base = path.basename(String(mediaUrls[0]));
    if (base) return base;
  }
  return `${mediaType || 'attachment'}-${String(messageId || 'file').slice(0, 12)}`;
}

/**
 * Per-chat manifest location, kept with the rest of the bridge's WhatsApp
 * state, next to the session dir:
 *   <parent of session dir>/whatsapp-attach/<chat-number>/manifest.json
 */
export function attachManifestPath(sessionDir, chatId) {
  // Keep the full chat identity (number + ':device' segment, as produced by
  // the bridge's own chat-key convention) — it stays unique per chat. Only
  // strip characters that are unsafe inside a path component; ':' is legal
  // on APFS/ext4 and preserves the segment.
  const chatKey = String(chatId || '')
    .replace(/@.*/, '')
    .replace(/[\\/\u0000-\u001f]/g, '');
  const dir = path.join(String(sessionDir || '.'), '..', 'whatsapp-attach', chatKey || 'unknown');
  return { dir, file: path.join(dir, 'manifest.json') };
}

/** Tolerant manifest read: any corruption yields a fresh manifest. */
export function loadAttachManifest(file) {
  try {
    const data = JSON.parse(readFileSync(file, 'utf8'));
    if (data && Array.isArray(data.batches)) {
      data.chatId = String(data.chatId || '');
      return data;
    }
  } catch {
    // missing/corrupt manifest: start fresh
  }
  return { chatId: '', batches: [] };
}

/**
 * Persist one batch (upserted by id) into the chat manifest, keeping only
 * the most recent MAX_MANIFEST_BATCHES batches. Returns true on success.
 * All filesystem errors are swallowed by the caller's log line; this
 * function reports via its boolean return instead of throwing.
 */
export function persistAttachManifest({ dir, file, chatId, batch }) {
  try {
    const manifest = loadAttachManifest(file);
    manifest.chatId = chatId || manifest.chatId;
    const idx = manifest.batches.findIndex((b) => b && b.id === batch?.id);
    if (idx >= 0) manifest.batches[idx] = batch;
    else manifest.batches.push(batch);
    if (manifest.batches.length > MAX_MANIFEST_BATCHES) {
      manifest.batches = manifest.batches.slice(-MAX_MANIFEST_BATCHES);
    }
    mkdirSync(dir, { recursive: true });
    writeFileSync(file, JSON.stringify(manifest, null, 2));
    return true;
  } catch {
    return false;
  }
}

/**
 * Stop-time summary: numbered name+size list plus the question. Deliberately
 * says nothing about the file contents — the agent asks first.
 */
export function buildAttachSummary(entries) {
  const list = Array.isArray(entries) ? entries : [];
  if (list.length === 0) {
    return 'No attachments were received in that batch. Send /attach start to begin a new one.';
  }
  const lines = list
    .slice(0, MAX_SUMMARY_LINES)
    .map((e) => {
      const size = humanSize(e?.sizeBytes);
      const sizeText = size ? ` (${size})` : '';
      const flag = e?.failed ? ' — NOT downloaded' : '';
      return `  ${e?.seq ?? ''}. ${e?.name ?? 'attachment'}${sizeText}${flag}`;
    });
  if (list.length > MAX_SUMMARY_LINES) {
    lines.push(`  … and ${list.length - MAX_SUMMARY_LINES} more`);
  }
  const count = list.length;
  return (
    `📥 ${count} attachment${count === 1 ? '' : 's'} ready:\n` +
    lines.join('\n') +
    '\nWhat should I do with them?'
  );
}
