import path from 'path';
import {
  closeSync,
  constants as fsConstants,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readSync,
  renameSync,
  unlinkSync,
  writeFileSync,
  writeSync,
} from 'fs';
import { createHash, randomBytes } from 'crypto';

export const BRIDGE_RUNTIME_INPUTS = Object.freeze([
  'allowlist.js',
  'baileys_logger.js',
  'bridge.js',
  'bridge_helpers.js',
  'connection_close.js',
  'outbound_ids.js',
  'owner_message_gate.js',
  'package.json',
  'package-lock.json',
]);
export const BRIDGE_MISSING_INPUT_SENTINEL = 'HERMES-BRIDGE-MISSING-v1';
const MAX_BRIDGE_RUNTIME_INPUT_BYTES = 4 * 1024 * 1024;
const BRIDGE_RUNTIME_INPUT_ERROR = 'WhatsApp bridge runtime inputs are unavailable.';
const defaultRuntimeHashFs = {
  closeSync,
  constants: fsConstants,
  fstatSync,
  lstatSync,
  openSync,
  readSync,
};

function runtimeInputError() {
  return new Error(BRIDGE_RUNTIME_INPUT_ERROR);
}

function runtimeStatSize(stats) {
  const size = stats?.size;
  if (typeof size === 'bigint') {
    if (size < 0n || size > BigInt(Number.MAX_SAFE_INTEGER)) return null;
    return Number(size);
  }
  return Number.isSafeInteger(size) && size >= 0 ? size : null;
}

function sameRuntimeStat(left, right, { includeDirectoryTimes = true } = {}) {
  const fields = includeDirectoryTimes
    ? ['dev', 'ino', 'mode', 'size', 'mtimeNs', 'ctimeNs']
    : ['dev', 'ino', 'mode', 'size', 'mtimeNs', 'ctimeNs'];
  return fields.every(field => sameStatField(left, right, field));
}

function isRegularRuntimeInput(stats, maxFileBytes) {
  try {
    const size = runtimeStatSize(stats);
    return stats?.isFile?.() === true
      && stats?.isSymbolicLink?.() === false
      && size !== null
      && size <= maxFileBytes;
  } catch {
    return false;
  }
}

function inspectRuntimeRoot(root, fsImpl) {
  let stats;
  try {
    stats = fsImpl.lstatSync(root, { bigint: true });
  } catch {
    throw runtimeInputError();
  }
  try {
    if (stats?.isDirectory?.() !== true || stats?.isSymbolicLink?.() !== false) {
      throw runtimeInputError();
    }
  } catch {
    throw runtimeInputError();
  }
  return stats;
}

function readRuntimeInput(filePath, fsImpl, maxFileBytes) {
  let beforeOpen;
  try {
    beforeOpen = fsImpl.lstatSync(filePath, { bigint: true });
  } catch (error) {
    if (error?.code === 'ENOENT') return null;
    throw runtimeInputError();
  }
  if (!isRegularRuntimeInput(beforeOpen, maxFileBytes)) throw runtimeInputError();

  const constants = fsImpl.constants || fsConstants;
  let flags = constants.O_RDONLY;
  for (const flag of ['O_CLOEXEC', 'O_NOFOLLOW', 'O_NONBLOCK']) {
    if (Number.isInteger(constants[flag])) flags |= constants[flag];
  }

  let fd = null;
  let payload = null;
  let failed = false;
  try {
    fd = fsImpl.openSync(filePath, flags);
    const opened = fsImpl.fstatSync(fd, { bigint: true });
    if (!isRegularRuntimeInput(opened, maxFileBytes) || !sameRuntimeStat(beforeOpen, opened)) {
      failed = true;
    } else {
      const buffer = Buffer.alloc(maxFileBytes + 1);
      let bytesRead = 0;
      while (bytesRead < buffer.length) {
        const remaining = buffer.length - bytesRead;
        const count = fsImpl.readSync(fd, buffer, bytesRead, remaining, null);
        if (!Number.isInteger(count) || count < 0 || count > remaining) {
          failed = true;
          break;
        }
        if (count === 0) break;
        bytesRead += count;
      }
      const afterRead = fsImpl.fstatSync(fd, { bigint: true });
      if (
        failed
        || bytesRead > maxFileBytes
        || !isRegularRuntimeInput(afterRead, maxFileBytes)
        || !sameRuntimeStat(opened, afterRead)
        || runtimeStatSize(afterRead) !== bytesRead
      ) {
        failed = true;
      } else {
        payload = buffer.subarray(0, bytesRead);
      }
    }
  } catch {
    failed = true;
  } finally {
    if (fd !== null) {
      try { fsImpl.closeSync(fd); } catch { failed = true; }
    }
  }
  if (failed || payload === null) throw runtimeInputError();
  return payload;
}

export function fingerprintBridgeInputs(bridgeDir, filenames = BRIDGE_RUNTIME_INPUTS, {
  fs: fsImpl = defaultRuntimeHashFs,
  maxFileBytes = MAX_BRIDGE_RUNTIME_INPUT_BYTES,
} = {}) {
  if (!Number.isSafeInteger(maxFileBytes) || maxFileBytes < 1) throw runtimeInputError();
  let names;
  try {
    names = [...new Set(filenames)].sort();
  } catch {
    throw runtimeInputError();
  }
  if (names.length === 0 || names.some(name => (
    typeof name !== 'string'
    || !/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(name)
    || path.basename(name) !== name
  ))) {
    throw runtimeInputError();
  }

  const beforeRoot = inspectRuntimeRoot(bridgeDir, fsImpl);
  const digest = createHash('sha256');
  let allPresent = true;
  for (const filename of names) {
    digest.update(filename, 'utf8');
    digest.update(Buffer.from([0]));
    const payload = readRuntimeInput(path.join(bridgeDir, filename), fsImpl, maxFileBytes);
    if (payload === null) {
      allPresent = false;
      digest.update(BRIDGE_MISSING_INPUT_SENTINEL, 'utf8');
    } else {
      digest.update(payload);
    }
    digest.update(Buffer.from([0]));
  }
  const afterRoot = inspectRuntimeRoot(bridgeDir, fsImpl);
  if (!sameRuntimeStat(beforeRoot, afterRoot)) throw runtimeInputError();
  return { hash: digest.digest('hex'), allPresent };
}

export function computeBridgeRuntimeHash(bridgeDir, options = {}) {
  const result = fingerprintBridgeInputs(bridgeDir, BRIDGE_RUNTIME_INPUTS, options);
  if (!result.allPresent) throw runtimeInputError();
  return result.hash;
}

export const MIME_MAP = {
  jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png',
  webp: 'image/webp', gif: 'image/gif',
  mp4: 'video/mp4', mov: 'video/quicktime', avi: 'video/x-msvideo',
  mkv: 'video/x-matroska', '3gp': 'video/3gpp',
  pdf: 'application/pdf',
  doc: 'application/msword',
  docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
};

export function normalizeWhatsAppId(value) {
  if (!value) return '';
  return String(value).replace(':', '@');
}

export function getMessageContent(msg) {
  const content = msg?.message || {};
  if (content.ephemeralMessage?.message) return content.ephemeralMessage.message;
  if (content.viewOnceMessage?.message) return content.viewOnceMessage.message;
  if (content.viewOnceMessageV2?.message) return content.viewOnceMessageV2.message;
  if (content.documentWithCaptionMessage?.message) return content.documentWithCaptionMessage.message;
  if (content.templateMessage?.hydratedTemplate) return content.templateMessage.hydratedTemplate;
  if (content.buttonsMessage) return content.buttonsMessage;
  if (content.listMessage) return content.listMessage;
  return content;
}

export function getContextInfo(messageContent) {
  if (!messageContent || typeof messageContent !== 'object') return {};
  for (const value of Object.values(messageContent)) {
    if (value && typeof value === 'object' && value.contextInfo) {
      return value.contextInfo;
    }
  }
  return {};
}

export function createBoundedMessageStore(limit = 512) {
  const byId = new Map();

  function remember(msg) {
    const id = msg?.key?.id;
    if (!id) return;
    byId.delete(id);
    byId.set(id, msg);
    while (byId.size > limit) {
      const oldest = byId.keys().next().value;
      byId.delete(oldest);
    }
  }

  function get(id) {
    if (!id || !byId.has(id)) return null;
    const msg = byId.get(id);
    byId.delete(id);
    byId.set(id, msg);
    return msg;
  }

  return { remember, get };
}

export function pollCreationMessageSecret(pollCreation) {
  return pollCreation?.message?.messageContextInfo?.messageSecret
    || pollCreation?.messageContextInfo?.messageSecret
    || null;
}

function uniqueStrings(values) {
  const seen = new Set();
  const out = [];
  for (const value of values || []) {
    const text = String(value || '').trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    out.push(text);
  }
  return out;
}

export function pollUpdateForAggregation({
  pollUpdateMessage,
  pollUpdateMessageKey,
  pollCreation,
  decryptPollVote,
  getKeyAuthor,
  meId = 'me',
  pollCreatorJids = [],
  voterJids = [],
}) {
  if (!pollUpdateMessage) return null;
  const updateKey = pollUpdateMessage.pollUpdateMessageKey
    || pollUpdateMessageKey
    || pollUpdateMessage.key;
  if (!updateKey) return null;

  if (pollUpdateMessage.vote?.selectedOptions) {
    return {
      pollUpdateMessageKey: updateKey,
      vote: pollUpdateMessage.vote,
      senderTimestampMs: pollUpdateMessage.senderTimestampMs,
    };
  }

  const creationKey = pollUpdateMessage.pollCreationMessageKey;
  const secret = pollCreationMessageSecret(pollCreation);
  if (
    !creationKey?.id
    || !secret
    || !pollUpdateMessage.vote?.encPayload
    || !pollUpdateMessage.vote?.encIv
    || typeof decryptPollVote !== 'function'
    || typeof getKeyAuthor !== 'function'
  ) {
    return null;
  }

  // Baileys poll decryption keys include both creator and voter JIDs.  On
  // WhatsApp LID chats, the poll creator can be the linked-device LID even
  // when sock.user.id is the classic @s.whatsapp.net JID.  Try the exact
  // candidates the live bridge knows before falling back to the generic helper.
  const creatorCandidates = uniqueStrings([
    ...pollCreatorJids,
    getKeyAuthor(creationKey, meId),
  ]);
  const voterCandidates = uniqueStrings([
    ...voterJids,
    getKeyAuthor(updateKey, meId),
  ]);

  let lastError = null;
  for (const pollCreatorJid of creatorCandidates) {
    for (const voterJid of voterCandidates) {
      try {
        const vote = decryptPollVote(pollUpdateMessage.vote, {
          pollCreatorJid,
          pollMsgId: creationKey.id,
          pollEncKey: secret,
          voterJid,
        });
        return {
          pollUpdateMessageKey: updateKey,
          vote,
          senderTimestampMs: pollUpdateMessage.senderTimestampMs,
        };
      } catch (err) {
        lastError = err;
      }
    }
  }
  if (lastError) throw lastError;
  return null;
}

export function buildTextSendPayload(text, { replyTo, messageStore } = {}) {
  const content = { text };
  const options = {};
  const quoted = messageStore?.get(replyTo);
  if (quoted?.key && quoted?.message) {
    // Baileys expects quoted messages as sendMessage options, not inside the
    // message content payload. Keeping this split avoids silently sending a
    // literal/ignored `quoted` field instead of a native WhatsApp reply.
    options.quoted = quoted;
  }
  return { content, options };
}

export function buildLocationPayload({ latitude, longitude, name, address } = {}) {
  const lat = Number(latitude);
  const lon = Number(longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error('latitude and longitude must be numbers');
  }
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    throw new Error('latitude/longitude out of range');
  }

  const location = {
    degreesLatitude: lat,
    degreesLongitude: lon,
  };
  if (name) location.name = String(name);
  if (address) location.address = String(address);
  return { location };
}

function textFromQuotedMessage(quotedMessage) {
  if (!quotedMessage) return '';
  if (quotedMessage.conversation) return quotedMessage.conversation;
  if (quotedMessage.extendedTextMessage?.text) return quotedMessage.extendedTextMessage.text;
  if (quotedMessage.imageMessage?.caption) return quotedMessage.imageMessage.caption;
  if (quotedMessage.videoMessage?.caption) return quotedMessage.videoMessage.caption;
  if (quotedMessage.documentMessage?.caption) return quotedMessage.documentMessage.caption;
  if (quotedMessage.documentMessage?.fileName) return `[Document: ${quotedMessage.documentMessage.fileName}]`;
  if (quotedMessage.locationMessage) return formatLocationText(quotedMessage.locationMessage, false);
  if (quotedMessage.contactMessage) return formatContactText(quotedMessage.contactMessage);
  if (quotedMessage.pollCreationMessage) return formatPollText(quotedMessage.pollCreationMessage);
  return '';
}

function mediaExtForMime(mime, fallback) {
  const normalized = String(mime || '').split(';', 1)[0].toLowerCase();
  const extMap = {
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/webp': '.webp',
    'image/gif': '.gif',
    'video/mp4': '.mp4',
    'video/quicktime': '.mov',
    'video/x-matroska': '.mkv',
    'audio/ogg': '.ogg',
    'audio/mp4': '.m4a',
    'audio/mpeg': '.mp3',
    'application/pdf': '.pdf',
  };
  return extMap[normalized] || fallback;
}

function defaultWriteMediaFile({ buffer, dir, prefix, ext, fileName }) {
  mkdirSync(dir, { recursive: true });
  let safeName = fileName ? `_${path.basename(fileName).replace(/[^a-zA-Z0-9._-]/g, '_')}` : '';
  if (safeName && ext && !path.extname(safeName)) {
    safeName = `${safeName}${ext}`;
  }
  const filePath = path.join(dir, `${prefix}_${randomBytes(6).toString('hex')}${safeName || ext}`);
  writeFileSync(filePath, buffer);
  return filePath;
}

function formatLocationText(location, isLive) {
  const name = location.name || location.address || '';
  const lat = location.degreesLatitude ?? location.latitude;
  const lng = location.degreesLongitude ?? location.longitude;
  const kind = isLive ? 'Live location' : 'Location';
  const coords = lat !== undefined && lng !== undefined ? `${lat},${lng}` : '';
  return `[${kind}: ${[name, coords].filter(Boolean).join(' ')}]`;
}

function locationMetadata(location, isLive) {
  return {
    name: location.name || '',
    address: location.address || '',
    latitude: location.degreesLatitude ?? location.latitude ?? null,
    longitude: location.degreesLongitude ?? location.longitude ?? null,
    isLive,
  };
}

function formatContactText(contact) {
  const name = contact.displayName || contact.vcard?.match(/FN:(.+)/)?.[1] || 'unknown';
  const phone = contact.vcard?.match(/TEL[^:]*:(.+)/)?.[1] || '';
  return `[Contact: ${[name, phone].filter(Boolean).join(' ')}]`;
}

function formatContactsText(contacts) {
  const names = contacts.map(c => c.displayName).filter(Boolean);
  return `[Contacts: ${names.join(', ') || contacts.length}]`;
}

function formatReactionText(reaction) {
  const emoji = reaction.text || '';
  const target = reaction.key?.id || '';
  return `[Reaction: ${emoji}${target ? ` to ${target}` : ''}]`;
}

function pollOptions(poll) {
  return (poll.options || [])
    .map(option => option.optionName || option.name)
    .filter(Boolean);
}

function formatPollText(poll) {
  const question = poll.name || poll.title || 'poll';
  const options = pollOptions(poll);
  return `[Poll: ${question}${options.length ? ` Options: ${options.join(', ')}` : ''}]`;
}

function formatPollUpdateText(update) {
  const target = update.pollCreationMessageKey?.id || update.key?.id || '';
  return `[Poll update${target ? `: ${target}` : ''}]`;
}

/**
 * Append a visible note for media that failed to download, so the agent knows
 * something was sent rather than silently losing the attachment. Returns
 * `content` unchanged when nothing failed. (Port of nanoclaw#2895.)
 */
export function appendMediaFailureNote(content, failures) {
  if (!failures || failures.length === 0) return content;
  const note = failures.map((t) => `[${t} could not be downloaded]`).join(' ');
  return content ? `${content}\n${note}` : note;
}

export async function extractBridgeEvent({
  msg,
  chatId,
  senderId,
  senderNumber,
  botIds = [],
  isGroup = false,
  downloadMedia,
  writeMediaFile,
  cacheDirs = {},
}) {
  const messageContent = getMessageContent(msg);
  const contextInfo = getContextInfo(messageContent);
  const mentionedIds = Array.from(new Set((contextInfo?.mentionedJid || []).map(normalizeWhatsAppId).filter(Boolean)));
  const quotedMessageId = contextInfo?.stanzaId || null;
  const quotedParticipant = normalizeWhatsAppId(contextInfo?.participant || '') || null;
  const quotedRemoteJid = normalizeWhatsAppId(contextInfo?.remoteJid || '') || null;
  const hasQuotedMessage = !!contextInfo?.quotedMessage;
  const quotedText = textFromQuotedMessage(contextInfo?.quotedMessage);

  let body = '';
  let hasMedia = false;
  let mediaType = '';
  let mime = '';
  let fileName = '';
  let nativeType = '';
  const mediaUrls = [];
  const nativeMetadata = {};

  const mediaFailures = [];

  const saveMedia = async ({ mediaMessage, dir, prefix, fallbackExt, fileName: name, type }) => {
    if (!downloadMedia) return;
    try {
      const buf = await downloadMedia(msg);
      const ext = mediaExtForMime(mediaMessage?.mimetype, fallbackExt);
      const writer = writeMediaFile || defaultWriteMediaFile;
      const saved = await writer({ buffer: buf, dir, prefix, ext, fileName: name });
      if (saved) mediaUrls.push(saved);
    } catch (err) {
      // A failed CDN fetch (expired media URL, transient network error) must
      // never reject out of extractBridgeEvent — that would drop this message
      // AND every remaining message in the same upsert batch. Record the
      // failure so the agent is told media was sent instead of losing it
      // silently. (Port of nanoclaw#2895's never-silently-drop guarantee; the
      // reuploadRequest recovery half is already wired in bridge.js.)
      mediaFailures.push(type || 'media');
      try {
        console.warn(`[bridge] failed to download inbound ${type || 'media'}`);
      } catch {}
    }
  };

  if (messageContent.conversation) {
    body = messageContent.conversation;
    nativeType = 'conversation';
  } else if (messageContent.extendedTextMessage?.text) {
    body = messageContent.extendedTextMessage.text;
    nativeType = 'extendedTextMessage';
  } else if (messageContent.imageMessage) {
    const item = messageContent.imageMessage;
    body = item.caption || '';
    hasMedia = true;
    mediaType = 'image';
    nativeType = 'imageMessage';
    mime = item.mimetype || 'image/jpeg';
    await saveMedia({ mediaMessage: item, dir: cacheDirs.image, prefix: 'img', fallbackExt: '.jpg', type: 'image' });
  } else if (messageContent.videoMessage) {
    const item = messageContent.videoMessage;
    body = item.caption || '';
    hasMedia = true;
    mediaType = item.gifPlayback ? 'gif' : 'video';
    nativeType = 'videoMessage';
    mime = item.mimetype || 'video/mp4';
    nativeMetadata.video = { gifPlayback: !!item.gifPlayback };
    await saveMedia({ mediaMessage: item, dir: cacheDirs.document, prefix: 'vid', fallbackExt: '.mp4', type: mediaType });
  } else if (messageContent.audioMessage || messageContent.pttMessage) {
    const item = messageContent.pttMessage || messageContent.audioMessage;
    hasMedia = true;
    mediaType = item.ptt || messageContent.pttMessage ? 'ptt' : 'audio';
    nativeType = messageContent.pttMessage ? 'pttMessage' : 'audioMessage';
    mime = item.mimetype || 'audio/ogg';
    nativeMetadata.audio = { ptt: mediaType === 'ptt' };
    await saveMedia({ mediaMessage: item, dir: cacheDirs.audio, prefix: 'aud', fallbackExt: '.ogg', type: 'audio' });
  } else if (messageContent.documentMessage) {
    const item = messageContent.documentMessage;
    body = item.caption || '';
    hasMedia = true;
    mediaType = 'document';
    nativeType = 'documentMessage';
    mime = item.mimetype || 'application/octet-stream';
    fileName = item.fileName || 'document';
    await saveMedia({ mediaMessage: item, dir: cacheDirs.document, prefix: 'doc', fallbackExt: '.bin', fileName, type: 'document' });
  } else if (messageContent.stickerMessage) {
    hasMedia = true;
    mediaType = 'sticker';
    nativeType = 'stickerMessage';
    mime = messageContent.stickerMessage.mimetype || 'image/webp';
    body = '[Sticker]';
    nativeMetadata.sticker = {
      animated: !!messageContent.stickerMessage.isAnimated,
      mimetype: mime,
    };
    await saveMedia({ mediaMessage: messageContent.stickerMessage, dir: cacheDirs.image, prefix: 'sticker', fallbackExt: '.webp', type: 'sticker' });
  } else if (messageContent.locationMessage || messageContent.liveLocationMessage) {
    const isLive = !!messageContent.liveLocationMessage;
    const item = messageContent.liveLocationMessage || messageContent.locationMessage;
    mediaType = isLive ? 'live_location' : 'location';
    nativeType = isLive ? 'liveLocationMessage' : 'locationMessage';
    body = formatLocationText(item, isLive);
    nativeMetadata.location = locationMetadata(item, isLive);
  } else if (messageContent.contactMessage) {
    mediaType = 'contact';
    nativeType = 'contactMessage';
    body = formatContactText(messageContent.contactMessage);
    nativeMetadata.contact = {
      displayName: messageContent.contactMessage.displayName || '',
      vcard: messageContent.contactMessage.vcard || '',
    };
  } else if (messageContent.contactsArrayMessage) {
    const contacts = messageContent.contactsArrayMessage.contacts || [];
    mediaType = 'contacts';
    nativeType = 'contactsArrayMessage';
    body = formatContactsText(contacts);
    nativeMetadata.contacts = contacts.map(contact => ({
      displayName: contact.displayName || '',
      vcard: contact.vcard || '',
    }));
  } else if (messageContent.reactionMessage) {
    mediaType = 'reaction';
    nativeType = 'reactionMessage';
    body = formatReactionText(messageContent.reactionMessage);
    nativeMetadata.reaction = {
      text: messageContent.reactionMessage.text || '',
      messageId: messageContent.reactionMessage.key?.id || '',
      remoteJid: normalizeWhatsAppId(messageContent.reactionMessage.key?.remoteJid || ''),
      participant: normalizeWhatsAppId(messageContent.reactionMessage.key?.participant || ''),
    };
  } else if (messageContent.pollCreationMessage || messageContent.pollCreationMessageV2 || messageContent.pollCreationMessageV3) {
    const item = messageContent.pollCreationMessage || messageContent.pollCreationMessageV2 || messageContent.pollCreationMessageV3;
    mediaType = 'poll';
    nativeType = messageContent.pollCreationMessage ? 'pollCreationMessage' : messageContent.pollCreationMessageV2 ? 'pollCreationMessageV2' : 'pollCreationMessageV3';
    body = formatPollText(item);
    nativeMetadata.poll = {
      question: item.name || item.title || '',
      options: pollOptions(item),
      selectableCount: item.selectableOptionsCount || item.selectableCount || 1,
    };
  } else if (messageContent.pollUpdateMessage) {
    mediaType = 'poll_update';
    nativeType = 'pollUpdateMessage';
    body = formatPollUpdateText(messageContent.pollUpdateMessage);
    nativeMetadata.pollUpdate = messageContent.pollUpdateMessage;
  }

  // Surface failed downloads to the agent instead of silently losing the
  // attachment. Applied before the generic "[<type> received]" fallback so an
  // uncaptioned message whose download failed reads "[image could not be
  // downloaded]" rather than claiming the media arrived.
  body = appendMediaFailureNote(body, mediaFailures);

  if (hasMedia && !body) {
    body = `[${mediaType} received]`;
  }

  return {
    messageId: msg.key.id,
    chatId,
    senderId,
    senderName: msg.pushName || senderNumber,
    chatName: isGroup ? (chatId.split('@')[0]) : (msg.pushName || senderNumber),
    isGroup,
    body,
    hasMedia,
    mediaType,
    mime,
    fileName,
    nativeType,
    nativeMetadata,
    mediaUrls,
    mentionedIds,
    quotedMessageId,
    quotedParticipant,
    quotedRemoteJid,
    quotedText,
    hasQuotedMessage,
    botIds,
    readReceiptKey: {
      remoteJid: msg.key.remoteJid || chatId,
      id: msg.key.id,
      participant: msg.key.participant || senderId,
      fromMe: Boolean(msg.key.fromMe),
    },
    timestamp: msg.messageTimestamp,
  };
}

export function inferMediaType(ext) {
  if (['jpg', 'jpeg', 'png', 'webp', 'gif'].includes(ext)) return 'image';
  if (['mp4', 'mov', 'avi', 'mkv', '3gp'].includes(ext)) return 'video';
  if (['ogg', 'opus', 'mp3', 'wav', 'm4a'].includes(ext)) return 'audio';
  return 'document';
}

export function inboundReadReceiptKeys({ key, enabled }) {
  if (!enabled || !key || key.fromMe || !key.id || !key.remoteJid) return [];
  // Preserve participant for group messages: Baileys needs the original key.
  return [key];
}

export function mediaPayloadForFile({ buffer, filePath, mediaType, caption, fileName }) {
  const ext = filePath.toLowerCase().split('.').pop();
  const type = mediaType || inferMediaType(ext);
  if (type === 'image' && ext === 'gif') {
    // Pure helper fallback: do not lie and label raw GIF bytes as mp4.
    // The live bridge tries ffmpeg conversion to WhatsApp gifPlayback video
    // before it falls back to this regular image payload.
    return { image: buffer, caption: caption || undefined, mimetype: MIME_MAP[ext] || 'image/gif' };
  }
  switch (type) {
    case 'image':
      return { image: buffer, caption: caption || undefined, mimetype: MIME_MAP[ext] || 'image/jpeg' };
    case 'video':
      return { video: buffer, caption: caption || undefined, mimetype: MIME_MAP[ext] || 'video/mp4' };
    case 'document':
      return {
        document: buffer,
        fileName: fileName || path.basename(filePath),
        caption: caption || undefined,
        mimetype: MIME_MAP[ext] || 'application/octet-stream',
      };
    default:
      return null;
  }
}

export function buildPollPayload({ question, options, selectableCount = 1 }) {
  const cleanQuestion = String(question || '').trim();
  const cleanOptions = (options || []).map(option => String(option || '').trim()).filter(Boolean);
  if (!cleanQuestion) throw new Error('question is required');
  if (cleanOptions.length < 2) throw new Error('at least two poll options are required');
  if (cleanOptions.length > 12) throw new Error('at most 12 poll options are supported');
  const count = Math.max(1, Math.min(Number(selectableCount) || 1, cleanOptions.length));
  return {
    poll: {
      name: cleanQuestion,
      values: cleanOptions,
      selectableCount: count,
      messageSecret: randomBytes(32),
    },
  };
}

export function pollCreationMessageFromPayload(payload) {
  const poll = payload?.poll;
  if (!poll) return null;
  const values = Array.isArray(poll.values) ? poll.values : [];
  const options = values.map(value => String(value || '').trim()).filter(Boolean);
  if (!poll.name || options.length < 2) return null;
  const selectableOptionsCount = Math.max(1, Math.min(Number(poll.selectableCount) || 1, options.length));
  const message = {};
  if (poll.messageSecret) {
    message.messageContextInfo = { messageSecret: poll.messageSecret };
  }
  message[selectableOptionsCount === 1 ? 'pollCreationMessageV3' : 'pollCreationMessage'] = {
    name: String(poll.name),
    options: options.map(optionName => ({ optionName })),
    selectableOptionsCount,
  };
  return message;
}

/**
 * Reconnect scheduling guard. startSocket() awaits network I/O before it
 * creates a socket or registers event handlers, so a bare
 * `setTimeout(startSocket, ...)` has two unrecoverable failure modes: a
 * rejection is unhandled (crashes the process on modern Node), and a hang
 * leaves the bridge permanently disconnected with nothing left to retry.
 * Every (re)connect must go through the scheduler this returns.
 */
export function createReconnectScheduler(startFn, {
  baseDelayMs = 3000,
  maxDelayMs = 60000,
  log = console.log,
  setTimeoutFn = setTimeout,
} = {}) {
  const baseDelay = Math.max(1, Number(baseDelayMs) || 3000);
  const maxDelay = Math.max(baseDelay, Number(maxDelayMs) || 60000);
  let backoffStep = 0;
  let lastFailureDelay = null;
  let promptRetryUsed = false;
  let initialStartUsed = false;
  let timerPending = false;
  let startInFlight = false;
  let parkedRequest = null;

  function nextBackoffDelay() {
    const delayMs = Math.min(maxDelay, baseDelay * (2 ** Math.min(backoffStep, 30)));
    if (delayMs < maxDelay) backoffStep += 1;
    return delayMs;
  }

  function armReconnect(delayMs) {
    timerPending = true;
    try {
      setTimeoutFn(() => {
        timerPending = false;
        startInFlight = true;
        Promise.resolve()
          .then(startFn)
          .then(() => {
            startInFlight = false;
            if (parkedRequest) {
              const request = parkedRequest;
              parkedRequest = null;
              scheduleReconnect(request.delayMs, request.options);
            }
          })
          .catch(() => {
            startInFlight = false;
            parkedRequest = null;
            const retryDelay = scheduleReconnect();
            if (retryDelay !== false) {
              log(`⚠️  Reconnect failed. Retrying in ${Math.round(retryDelay / 1000)}s...`);
            }
          });
      }, delayMs);
    } catch (error) {
      timerPending = false;
      throw error;
    }
    return delayMs;
  }

  function scheduleReconnect(delayHint, options = {}) {
    if (timerPending) return false;
    if (startInFlight) {
      if (!parkedRequest) parkedRequest = { delayMs: delayHint, options };
      return false;
    }

    let delayMs;
    if (options.initial && !initialStartUsed && lastFailureDelay === null) {
      initialStartUsed = true;
      delayMs = 0;
    } else if (options.prompt && !promptRetryUsed && lastFailureDelay === null) {
      promptRetryUsed = true;
      delayMs = 1000;
      lastFailureDelay = delayMs;
    } else {
      promptRetryUsed = true;
      delayMs = Math.max(lastFailureDelay ?? 0, nextBackoffDelay());
      lastFailureDelay = delayMs;
    }
    return armReconnect(delayMs);
  }

  scheduleReconnect.resetBackoff = () => {
    backoffStep = 0;
    lastFailureDelay = null;
    promptRetryUsed = false;
  };
  return scheduleReconnect;
}

/**
 * Policy rules that may be named on an `ignored` line, and the tag used when
 * the caller names anything else.
 */
const IGNORED_MESSAGE_REASONS = new Set([
  'allowlist_mismatch',
  'allowlist_mismatch_owner_chat',
  'self_chat_mode_rejects_non_self',
]);
const GENERIC_IGNORED_REASON = 'policy';

/**
 * Build the JSON line the bridge emits when it declines to forward a message.
 *
 * These lines go to ORDINARY stdout — the stream Hermes redirects into a log
 * file the dashboard displays — not to the debug stream, so they are written
 * on every install rather than only when someone opts in. They used to carry
 * `chatId` and `senderId` verbatim, which meant a stranger who merely
 * messaged the operator, and was rejected for it, had their phone number
 * written to that file. Rejection is routine, unsolicited, and high volume:
 * it is the worst place to keep an identity and the least useful.
 *
 * What an operator acts on is the policy — which rule fired, and how often —
 * so that is all this returns. The shape is fixed here rather than at the
 * call sites because the call sites sit inside the message loop with the
 * identities in scope, one well-meaning edit away from putting them back.
 *
 * The reason is checked against an allowlist for the same reason: it is the
 * only variable field on the line, so it is the one channel an identity could
 * take back out.
 */
export function buildIgnoredMessageEvent(reason) {
  return {
    event: 'ignored',
    reason: IGNORED_MESSAGE_REASONS.has(reason) ? reason : GENERIC_IGNORED_REASON,
  };
}

/**
 * Exit code the bridge uses for "WhatsApp logged this device out".
 *
 * DOCUMENTED CONTRACT with the Python adapter
 * (plugins/platforms/whatsapp/adapter.py::BRIDGE_EXIT_LOGGED_OUT): this exit
 * code means the session is dead and re-running the bridge cannot fix it, so
 * the adapter must raise a NON-RETRYABLE fatal and tell the user to re-pair.
 * Any other non-zero exit stays retryable. 78 is sysexits.h EX_CONFIG — the
 * closest standard sense of "this install needs operator action, not a
 * retry" — and sits clear of the generic-failure code 1 and of the 126+
 * range the shell reserves for "cannot execute" and signal deaths.
 */
export const BRIDGE_EXIT_LOGGED_OUT = 78;

/**
 * The session needs re-pairing, and this process is not attached to a
 * terminal that may display a QR.
 *
 * A managed bridge is spawned by the adapter with stdout/stderr pointed at
 * `bridge.log`. If Baileys reaches a QR state there — which it can do at any
 * time, because a session may be revoked server-side long after a legitimate
 * start — rendering the code would write live pairing material into a file on
 * disk. The parent cannot prevent that after the fact: the child writes to
 * the descriptor it was handed before the adapter observes anything. So the
 * bridge stops instead, and the operator re-pairs through the supported
 * one-shot command in their own terminal.
 *
 * 79 sits next to EX_CONFIG (78) and stays inside the retryable-vs-terminal
 * contract the adapter already understands.
 */
export const BRIDGE_EXIT_REPAIR_REQUIRED = 79;

/**
 * Longest reason tag we will ever repeat back. Real stream reasons are short
 * identifiers (`conflict`, `device_removed`, `restart required`); anything
 * appreciably longer is not a reason we know how to act on.
 */
const MAX_REASON_LENGTH = 48;

/** Stream reasons are bare lowercase identifiers — no punctuation, no spaces. */
const SAFE_REASON_PATTERN = /^[a-z0-9][a-z0-9_-]*$/;
const SAFE_DISCONNECT_TAGS = new Set(['conflict', 'device_removed']);

/**
 * Reduce a server-supplied value to a bounded, single-line reason tag, or null.
 *
 * Everything reaching this function came off the wire inside a WhatsApp
 * binary node, so it is attacker-influenced in principle: it is dropped
 * outright (never truncated, never escaped) unless it already looks like the
 * short identifier a stream reason is supposed to be. This is a format guard;
 * callers reflecting data into logs must additionally apply the protocol
 * allowlist in `safeDisconnectTag`.
 */
export function safeReasonTag(value) {
  if (typeof value !== 'string') return null;
  const text = value.trim().toLowerCase();
  if (!text || text.length > MAX_REASON_LENGTH) return null;
  return SAFE_REASON_PATTERN.test(text) ? text : null;
}

/** Keep only protocol tags the bridge understands and may safely disclose. */
function safeDisconnectTag(value) {
  const tag = safeReasonTag(value);
  return tag && SAFE_DISCONNECT_TAGS.has(tag) ? tag : null;
}

/** Coerce a status-code-ish value to a bounded HTTP-style integer, else null. */
function safeStatusCode(value) {
  if (value === null || value === undefined || value === '') return null;
  const numeric = typeof value === 'number'
    ? value
    : (typeof value === 'string' && /^\d{3}$/.test(value.trim()) ? Number.parseInt(value.trim(), 10) : NaN);
  return Number.isInteger(numeric) && numeric >= 100 && numeric <= 599 ? numeric : null;
}

/**
 * Pull a SAFE status code and stream reason out of a Baileys/Boom disconnect
 * error.
 *
 * Baileys turns a WhatsApp `<stream:error>` into a Boom that carries the
 * stream reason in its message, the status in `output.statusCode`, and the
 * original binary node on `data`. The status is taken from the node's
 * OPTIONAL `code` attribute, which has one consequence that matters here:
 * **materially different stream reasons share one status.** A genuine
 * revocation (`<stream:error><conflict type="device_removed"/></stream:error>`)
 * and an ordinary conflict both map through `CODE_MAP.conflict` to 440, and
 * both arrive as 401 when the server does send `code="401"`. The number alone
 * therefore cannot decide whether a session is dead.
 *
 * The reason node survives on `error.data`. It is server-supplied and may
 * contain anything, so this function is the only place allowed to look at it,
 * and it copies out nothing but:
 *
 *   - `statusCode` — a bounded HTTP-style integer, or null
 *   - `reason`     — the reason child's known protocol tag, or null
 *   - `detail`     — that child's known `type` protocol tag, or null
 *
 * The node itself must never be logged, serialised, or forwarded whole.
 *
 * TWO `data` SHAPES, because Baileys hands over `reasonNode || node`:
 *
 *   - the reason child ALREADY UNWRAPPED — `{tag:'conflict', attrs:{type}}` —
 *     which is what every stream error with a child produces, and so what
 *     every real revocation looks like;
 *   - the whole `<stream:error>` wrapper, when it had no children to unwrap.
 *
 * Both are read here. Reading only the wrapper — the shape the protocol
 * documents, and the intuitive one — means recognising no revocation at all,
 * which fails OPEN: the bridge reconnects forever into a session WhatsApp has
 * already destroyed. So the wrapper is tried only as a fallback, after the
 * value has been read as a reason child in its own right; a wrapper's own
 * `stream:error` tag is not a disconnect tag, so it contributes nothing and
 * falls through cleanly.
 *
 * `CB:failure` closes take a third Baileys branch and pass `node.attrs`
 * (flat, no `tag`, no `content`) as `data`; its numeric `reason` attribute is
 * just the status restated, so it is deliberately NOT treated as a stream
 * reason, while a flat `type` still is.
 *
 * Never throws: this runs on the disconnect path, where an exception would
 * take the reconnect loop down with it.
 */
export function extractDisconnectMetadata(error) {
  const statusCode = safeStatusCode(error?.output?.statusCode ?? error?.statusCode);
  const node = error?.data;
  const { reason, detail } = readDisconnectEvidence(node)
    ?? readDisconnectEvidence(Array.isArray(node?.content) ? node.content[0] : null)
    ?? { reason: null, detail: null };

  return { statusCode, reason, detail };
}

/**
 * Read one reason-child-shaped object into allowlisted tags, or null when it
 * yields no evidence at all (so the caller can try the next candidate shape).
 */
function readDisconnectEvidence(child) {
  if (!child || typeof child !== 'object' || Array.isArray(child)) return null;
  const reason = safeDisconnectTag(child.tag);
  const detail = safeDisconnectTag(child.attrs?.type ?? child.type);
  return reason !== null || detail !== null ? { reason, detail } : null;
}

/**
 * Stream reasons that are proof of a DURABLE, server-side revocation.
 *
 * Deliberately tiny. Membership here means "stop retrying forever and demand
 * operator action", which is far too destructive to infer. `device_removed`
 * earns it because WhatsApp only sends it once the device has actually been
 * unlinked account-side; nothing a reconnect can do will undo that.
 */
const DURABLE_REVOCATION_REASONS = new Set(['device_removed']);

/** True when disconnect metadata carries explicit durable-revocation evidence. */
export function isDurableRevocation(metadata) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  return DURABLE_REVOCATION_REASONS.has(meta.reason)
    || DURABLE_REVOCATION_REASONS.has(meta.detail);
}

/**
 * Decide what a `connection: 'close'` means.
 *
 * Terminal ONLY on explicit durable evidence (see
 * DURABLE_REVOCATION_REASONS); everything else reconnects. 515
 * (restart-after-pairing) reconnects promptly, while ordinary closes enter
 * the reconnect scheduler's bounded exponential backoff.
 *
 * WHY NOT "status 401 means logged out" (the obvious reading of
 * `DisconnectReason.loggedOut`): status 401 is not exclusive to revocation.
 * A healthy session emits status-401 closes with no device_removed reason
 * and then reconnects normally on the same credentials. Treating bare 401 as
 * terminal therefore throws away working sessions and demands a re-pair that
 * is not needed.
 *
 * The status code is not consulted for the terminal decision at all, only
 * for the retry delay: `statusCode` is derived from an OPTIONAL `code`
 * attribute, so durable evidence can legitimately arrive alongside some
 * other number, and evidence outranks the number in both directions.
 *
 * Accepts either the metadata object from `extractDisconnectMetadata()` or a
 * bare status number (which by definition carries no evidence, and so is
 * never terminal).
 */
export function classifyDisconnectReason(metadata) {
  const meta = metadata && typeof metadata === 'object' ? metadata : { statusCode: metadata };
  if (isDurableRevocation(meta)) {
    return { terminal: true, exitCode: BRIDGE_EXIT_LOGGED_OUT };
  }
  // 515 = restart requested, normal right after pairing; reconnect promptly.
  return { terminal: false, delayMs: safeStatusCode(meta.statusCode) === 515 ? 1000 : 3000 };
}

/**
 * Where a terminal revocation verdict is recorded: INSIDE the session
 * directory, beside creds.json.
 *
 * Inside is the point. Both re-pair control paths remove this directory
 * wholesale before spawning `--pair-only`, so a re-paired session is
 * naturally unmarked and no explicit "clear the marker" step — which could
 * be skipped, or run at the wrong moment — is needed. The bridge itself only
 * ever writes and reads the marker; deleting auth state is a decision the
 * user makes through a control path, never something the bridge does behind
 * their back.
 */
export function sessionRevokedMarkerPath(sessionDir) {
  return path.join(sessionDir, 'revoked.json');
}

/**
 * Record on disk that WhatsApp revoked this session.
 *
 * Exit code 78 only helps whoever is still watching the process. This marker
 * outlives process death, so a later gateway start — or an externally
 * launched bridge that Hermes merely reuses, where no exit code is ever
 * observed — can refuse to open a socket at all instead of reconnecting into
 * a session that can never come back.
 *
 * Contains NO auth material by construction: the payload is built field by
 * field from a bounded status number and two sanitised reason tags. Whatever
 * else the caller passes in `metadata` is ignored, so it cannot become a
 * channel for credentials.
 *
 * Written atomically and 0600 via the shared helper below, because it is
 * written immediately before `process.exit(78)` — a half-written file would
 * be read back by the next start.
 *
 * Failure is nonfatal and logged: not being able to write the marker must
 * never stop the bridge from exiting.
 */
export function writeSessionRevokedMarker(sessionDir, metadata = {}, {
  fs: fsImpl = defaultAtomicWriteFs,
  log = console.log,
  now = () => new Date().toISOString(),
  platform = process.platform,
} = {}) {
  const payload = {
    revoked: true,
    statusCode: safeStatusCode(markerMetadataValue(metadata, 'statusCode')),
    reason: safeDisconnectTag(markerMetadataValue(metadata, 'reason')),
    detail: safeDisconnectTag(markerMetadataValue(metadata, 'detail')),
    at: safeMarkerTimestamp(now),
  };
  return atomicWriteJsonSync(sessionRevokedMarkerPath(sessionDir), payload, {
    fs: fsImpl,
    log,
    platform,
    tmpPrefix: '.revoked',
    what: 'the WhatsApp revoked-session marker',
  });
}

function markerMetadataValue(metadata, key) {
  if ((typeof metadata !== 'object' || metadata === null) && typeof metadata !== 'function') return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(metadata, key);
    return descriptor && Object.prototype.hasOwnProperty.call(descriptor, 'value')
      ? descriptor.value
      : undefined;
  } catch {
    return undefined;
  }
}

function safeMarkerTimestamp(now) {
  let value;
  try { value = now(); } catch { return null; }
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) return null;
  try {
    return new Date(value).toISOString() === value ? value : null;
  } catch {
    return null;
  }
}

/**
 * The verdict returned for a marker that is there but cannot be interpreted.
 *
 * Deliberately synthetic: it carries nothing read out of the file, because
 * the reason the file is not trusted is that its contents could not be
 * validated. A fresh object each call — callers hand this to log lines and
 * pair-event JSON, and must not be able to mutate a shared one.
 */
function failClosedRevokedVerdict() {
  return { revoked: true, statusCode: null, reason: null, detail: null };
}

/**
 * Read back a revocation verdict, or null when this session is not marked.
 *
 * ABSENCE is the only state that means "no evidence": a freshly paired
 * session directory has no marker at all, and that is the common case. Only
 * ENOENT establishes it, so it is the only read failure treated as absence.
 *
 * Everything else fails CLOSED — a read that failed for any other reason, and
 * content that is empty, unparseable, not a JSON object, or does not say
 * exactly `revoked: true`, all read back as revoked. The marker is one file
 * recording the one verdict that has to survive process death; if content we
 * cannot interpret meant "no evidence", then truncating it, chmod'ing it, or a
 * write torn by a crash would silently disarm it and the bridge would
 * reconnect into a session WhatsApp already destroyed, forever, with nobody
 * watching an exit code. Failing closed costs one explicit operator step (the
 * destructive reset `hermes whatsapp` and the dashboard offer); failing open
 * costs the session, unrecoverably and unnoticed.
 *
 * The parsed file is NEVER returned wholesale. We wrote it, but it lives on
 * disk, so by the time it is read back it is untrusted input — and its
 * contents feed log lines and pair-event JSON. A trusted marker is therefore
 * re-sanitised through exactly the same bounds as the wire path and reduced
 * to the four fields callers may act on; an untrusted one is replaced by the
 * synthetic verdict above, which reflects none of it. The free-form `at`
 * timestamp is deliberately dropped either way: it is useful to a human
 * reading the file and has no business in a log line.
 */
const MAX_REVOKED_MARKER_BYTES = 4096;
const defaultMarkerReadFs = {
  closeSync,
  constants: fsConstants,
  fstatSync,
  lstatSync,
  openSync,
  readSync,
};

export function readSessionRevokedMarker(sessionDir, { fs: fsImpl = defaultMarkerReadFs } = {}) {
  const markerPath = sessionRevokedMarkerPath(sessionDir);
  let beforeOpen;
  try {
    beforeOpen = fsImpl.lstatSync(markerPath, { bigint: true });
  } catch (err) {
    return err?.code === 'ENOENT' ? null : failClosedRevokedVerdict();
  }
  if (!isBoundedRegularMarker(beforeOpen)) return failClosedRevokedVerdict();

  const constants = fsImpl.constants || fsConstants;
  let flags = constants.O_RDONLY;
  if (Number.isInteger(constants.O_NOFOLLOW)) flags |= constants.O_NOFOLLOW;
  if (Number.isInteger(constants.O_NONBLOCK)) flags |= constants.O_NONBLOCK;

  let fd = null;
  let raw = null;
  let readWasSafe = false;
  let closeWasSafe = true;
  try {
    // Any failure here, including ENOENT, happened after the marker was
    // observed and therefore represents uncertainty rather than absence.
    fd = fsImpl.openSync(markerPath, flags);
    const opened = fsImpl.fstatSync(fd, { bigint: true });
    if (!isSameBoundedMarker(beforeOpen, opened)) return failClosedRevokedVerdict();

    const buffer = Buffer.alloc(MAX_REVOKED_MARKER_BYTES + 1);
    let bytesRead = 0;
    while (bytesRead < buffer.length) {
      const remaining = buffer.length - bytesRead;
      const count = fsImpl.readSync(fd, buffer, bytesRead, remaining, null);
      if (!Number.isInteger(count) || count < 0 || count > remaining) {
        return failClosedRevokedVerdict();
      }
      if (count === 0) break;
      bytesRead += count;
    }

    const afterRead = fsImpl.fstatSync(fd, { bigint: true });
    if (
      bytesRead > MAX_REVOKED_MARKER_BYTES
      || !isSameBoundedMarker(opened, afterRead)
      || markerStatSize(afterRead) !== bytesRead
    ) {
      return failClosedRevokedVerdict();
    }
    raw = buffer.subarray(0, bytesRead).toString('utf8');
    readWasSafe = true;
  } catch {
    return failClosedRevokedVerdict();
  } finally {
    if (fd !== null) {
      try { fsImpl.closeSync(fd); } catch { closeWasSafe = false; }
    }
  }
  if (!readWasSafe || !closeWasSafe) return failClosedRevokedVerdict();

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return failClosedRevokedVerdict();
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return failClosedRevokedVerdict();
  if (parsed.revoked !== true) return failClosedRevokedVerdict();

  return {
    revoked: true,
    statusCode: safeStatusCode(parsed.statusCode),
    reason: safeDisconnectTag(parsed.reason),
    detail: safeDisconnectTag(parsed.detail),
  };
}

function markerStatSize(stats) {
  const size = stats?.size;
  if (typeof size === 'bigint') {
    if (size < 0n || size > BigInt(Number.MAX_SAFE_INTEGER)) return null;
    return Number(size);
  }
  return Number.isSafeInteger(size) && size >= 0 ? size : null;
}

function isBoundedRegularMarker(stats) {
  try {
    const size = markerStatSize(stats);
    return stats?.isFile?.() === true
      && stats?.isSymbolicLink?.() === false
      && size !== null
      && size <= MAX_REVOKED_MARKER_BYTES;
  } catch {
    return false;
  }
}

function sameStatField(left, right, field) {
  const leftValue = left?.[field];
  const rightValue = right?.[field];
  const comparable = (typeof leftValue === 'bigint' && typeof rightValue === 'bigint')
    || (Number.isSafeInteger(leftValue) && Number.isSafeInteger(rightValue));
  return comparable && leftValue === rightValue;
}

function isSameBoundedMarker(left, right) {
  if (!isBoundedRegularMarker(right)) return false;
  return ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].every(field => sameStatField(left, right, field));
}

const defaultAtomicWriteFs = { mkdirSync, openSync, writeSync, fsyncSync, closeSync, renameSync, unlinkSync };

/**
 * Write JSON to `filePath` as one indivisible replacement, 0600-only.
 *
 * A uniquely-named temp file in the SAME directory (so the final rename is
 * same-filesystem) is opened mode 0600, written in full, flushed and closed,
 * then renamed over the final path. The destination is never touched until
 * that last rename, so a failure at any earlier step leaves whatever was
 * already there exactly as it was, and no reader can observe a partial file.
 *
 * On POSIX the containing directory is opened before rename and fsync'd after
 * it, so the replacement directory entry is crash-durable too. Windows keeps
 * the exclusive-temp/file-fsync/atomic-rename protocol but skips the directory
 * descriptor because Node does not support it reliably there.
 *
 * `fs` accepts sync fs-function overrides so failure paths can be tested
 * deterministically instead of via OS permission tricks. Failure is nonfatal
 * and logged: the caller is writing an after-the-fact record on its way out,
 * and must not crash the bridge.
 */
function atomicWriteJsonSync(filePath, payload, { fs: fsImpl, log, platform, tmpPrefix, what }) {
  const dir = path.dirname(filePath);
  const tmpPath = path.join(dir, `${tmpPrefix}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`);
  let fd = null;
  let dirFd = null;
  let tempCreated = false;
  try {
    fsImpl.mkdirSync(dir, { recursive: true });
    fd = fsImpl.openSync(tmpPath, 'wx', 0o600);
    tempCreated = true;
    writeAllSync(fsImpl, fd, Buffer.from(`${JSON.stringify(payload)}\n`, 'utf8'));
    fsImpl.fsyncSync(fd);
    fsImpl.closeSync(fd);
    fd = null;
    if (platform !== 'win32') {
      // Acquire this before rename: if the directory cannot be opened on a
      // platform where durability is supported, the prior destination is
      // still untouched and the operation can fail safely.
      dirFd = fsImpl.openSync(dir, 'r');
    }
    fsImpl.renameSync(tmpPath, filePath);
    tempCreated = false;
    if (dirFd !== null) {
      fsImpl.fsyncSync(dirFd);
      fsImpl.closeSync(dirFd);
      dirFd = null;
    }
    return true;
  } catch {
    // Error text can contain the absolute session path. The bridge log is
    // surfaced by the dashboard, so report the operation without reflecting
    // filesystem details or arbitrary thrown values.
    log(`⚠️  Could not persist ${what}.`);
    if (fd !== null) {
      try { fsImpl.closeSync(fd); } catch {}
    }
    if (dirFd !== null) {
      try { fsImpl.closeSync(dirFd); } catch {}
    }
    if (tempCreated) {
      try { fsImpl.unlinkSync(tmpPath); } catch {}
    }
    return false;
  }
}

/**
 * Write a whole buffer to `fd`, resuming until every byte is accepted.
 *
 * `writeSync` returns how many bytes it TOOK, which is not required to be all
 * of them: a signal arriving mid-write, or a destination with a smaller
 * transfer size, produces a short write with no error raised. Treating one
 * call as complete silently truncates the file — and this file is read back
 * after the process is gone, so nobody is left to notice.
 *
 * Progress is tracked by BYTE offset into a pre-encoded buffer, not by
 * character, so a resumed write cannot split a multi-byte UTF-8 sequence.
 *
 * A call that accepts nothing means the destination is not draining (full,
 * broken, closed). Since the caller is on its way to `process.exit()`, that
 * is raised rather than retried forever: a hung exit would be worse than a
 * missing marker.
 */
function writeAllSync(fsImpl, fd, buffer) {
  let written = 0;
  while (written < buffer.length) {
    const n = fsImpl.writeSync(fd, buffer, written, buffer.length - written);
    if (!(n > 0)) throw new Error('write made no progress');
    written += n;
  }
}

/**
 * Version resolution guard. fetchLatestBaileysVersion() is a plain fetch to
 * raw.githubusercontent.com with no AbortSignal; a stalled connection can
 * pend forever and wedge the reconnect path (the scheduler above cannot
 * retry past an await that never settles). Bound the fetch and fall back to
 * the last known-good version, or the Baileys default before first success.
 */
export function createVersionResolver(fetchVersionFn, {
  timeoutMs = 15000,
  log = console.log,
} = {}) {
  let cachedVersion = null;
  let pendingVersion = null;

  const fallbackVersion = () => cachedVersion ? [...cachedVersion] : null;

  async function resolveVersion() {
    // Reaching a second start means the previous candidate never produced an
    // open connection. Discard it before fetching again so a later fetch
    // failure cannot accidentally promote the candidate that may have broken
    // startup.
    pendingVersion = null;
    let timer = null;
    try {
      const result = await Promise.race([
        fetchVersionFn(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error('version fetch timed out')), timeoutMs);
        }),
      ]);
      const candidate = validatedBaileysVersion(result?.version);
      if (result?.isLatest !== true || !candidate) {
        log(`⚠️  Baileys version fetch was unconfirmed; using ${cachedVersion ? 'cached version' : 'library default'}.`);
        return fallbackVersion();
      }
      pendingVersion = candidate;
      return [...candidate];
    } catch {
      log(`⚠️  Baileys version fetch failed; using ${cachedVersion ? 'cached version' : 'library default'}.`);
      return fallbackVersion();
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  resolveVersion.confirm = (version) => {
    const candidate = validatedBaileysVersion(version);
    if (!candidate || !pendingVersion || !sameBaileysVersion(candidate, pendingVersion)) return false;
    cachedVersion = [...pendingVersion];
    pendingVersion = null;
    return true;
  };
  return resolveVersion;
}

function validatedBaileysVersion(value) {
  if (!Array.isArray(value) || value.length !== 3) return null;
  const version = [];
  for (const part of value) {
    if (!Number.isSafeInteger(part) || part < 0) return null;
    version.push(part);
  }
  return version;
}

function sameBaileysVersion(left, right) {
  return left.length === right.length && left.every((part, index) => part === right[index]);
}
