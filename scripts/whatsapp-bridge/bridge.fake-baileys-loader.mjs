/** Test-only Baileys replacement for subprocess behaviour tests. */

import { registerHooks } from 'node:module';

const FAKE_BAILEYS_URL = 'hermes-test:fake-baileys';

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === '@whiskeysockets/baileys') {
      return { url: FAKE_BAILEYS_URL, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },

  load(url, context, nextLoad) {
    if (url !== FAKE_BAILEYS_URL) return nextLoad(url, context);

    return {
      format: 'module',
      shortCircuit: true,
      source: `
        import { appendFileSync } from 'node:fs';

        const handlers = new Map();
        const versionLifecycle = process.env.FAKE_WA_VERSION_LIFECYCLE || '';
        let socketCalls = 0;
        let versionFetchCalls = 0;
        const emit = (name, value) => {
          for (const handler of handlers.get(name) || []) handler(value);
        };

        const traceSocketVersion = (options) => {
          const tracePath = process.env.FAKE_WA_VERSION_TRACE;
          if (!tracePath) return;
          appendFileSync(
            tracePath,
            JSON.stringify(Array.isArray(options?.version) ? options.version : null) + String.fromCharCode(10),
          );
        };

        const syntheticError = () => {
          const error = new Error(process.env.FAKE_WA_ERROR_MESSAGE || 'synthetic failure');
          error.code = 'SYNTHETIC_FAILURE';
          error.data = {
            jid: process.env.FAKE_WA_ERROR_JID,
            filePath: process.env.FAKE_WA_ERROR_PATH,
            secret: process.env.FAKE_WA_ERROR_SECRET,
          };
          error.cause = new Error(process.env.FAKE_WA_ERROR_SECRET || 'synthetic cause');
          return error;
        };

        const pollId = () => process.env.FAKE_WA_POLL_ID || 'fake-poll-id';

        const pollUpdate = (chatId, { decoded = false, suffix = '' } = {}) => ({
          pollCreationMessageKey: { id: pollId(), remoteJid: chatId, fromMe: true },
          pollUpdateMessageKey: {
            id: (process.env.FAKE_WA_POLL_VOTE_ID || 'fake-poll-vote-id') + suffix,
            remoteJid: chatId,
            participant: process.env.FAKE_WA_ERROR_JID,
            fromMe: false,
          },
          vote: decoded
            ? { selectedOptions: [Buffer.from('selected')] }
            : { encPayload: Buffer.from('ciphertext'), encIv: Buffer.from('iv') },
          senderTimestampMs: Date.now(),
        });

        const emitPollFailures = (chatId) => {
          const failed = pollUpdate(chatId, { suffix: '-failed' });
          const decoded = pollUpdate(chatId, { decoded: true, suffix: '-decoded' });
          emit('messages.update', [failed, decoded].map(update => ({
            key: { id: pollId(), remoteJid: chatId, participant: process.env.FAKE_WA_ERROR_JID },
            update: { pollUpdates: [update] },
          })));
          emit('messages.upsert', {
            type: 'notify',
            messages: [failed, decoded].map((update, index) => ({
              key: {
                id: (process.env.FAKE_WA_POLL_VOTE_ID || 'fake-poll-vote-upsert-id') + '-upsert-' + index,
                remoteJid: chatId,
                participant: process.env.FAKE_WA_ERROR_JID,
                fromMe: false,
              },
              messageTimestamp: Math.floor(Date.now() / 1000),
              message: { pollUpdateMessage: update },
            })),
          });
        };

        const emitHostileDebugMessages = () => {
          const message = { conversation: process.env.FAKE_WA_BODY_SECRET || 'synthetic body' };
          message[process.env.FAKE_WA_OBJECT_KEY || 'syntheticObjectKey'] = {
            caption: process.env.FAKE_WA_MEDIA_TEXT_SECRET || 'synthetic media text',
          };
          emit('messages.upsert', {
            type: 'notify',
            messages: [
              {
                key: {
                  id: process.env.FAKE_WA_MESSAGE_ID,
                  remoteJid: process.env.FAKE_WA_DEBUG_CHAT,
                  participant: process.env.FAKE_WA_DEBUG_SENDER,
                  fromMe: false,
                },
                pushName: process.env.FAKE_WA_IDENTITY_NAME,
                messageTimestamp: Math.floor(Date.now() / 1000),
                message,
              },
              {
                key: {
                  id: process.env.FAKE_WA_FOREIGN_VOTE_ID,
                  remoteJid: process.env.FAKE_WA_DEBUG_CHAT,
                  participant: process.env.FAKE_WA_DEBUG_SENDER,
                  fromMe: false,
                },
                messageTimestamp: Math.floor(Date.now() / 1000),
                message: {
                  pollUpdateMessage: {
                    ...pollUpdate(process.env.FAKE_WA_DEBUG_CHAT, { decoded: true }),
                    pollCreationMessageKey: {
                      id: process.env.FAKE_WA_FOREIGN_POLL_ID,
                      remoteJid: process.env.FAKE_WA_DEBUG_CHAT,
                      fromMe: false,
                    },
                  },
                },
              },
              {
                key: {
                  id: (process.env.FAKE_WA_MESSAGE_ID || 'synthetic-message-id') + '-owner',
                  remoteJid: process.env.FAKE_WA_DEBUG_CHAT,
                  fromMe: true,
                },
                messageTimestamp: Math.floor(Date.now() / 1000),
                message: { conversation: process.env.FAKE_WA_BODY_SECRET || 'synthetic body' },
              },
            ],
          });
        };

        export function makeWASocket(options = {}) {
          socketCalls += 1;
          const socketNumber = socketCalls;
          traceSocketVersion(options);
          const sock = {
            user: {
              id: process.env.FAKE_WA_IDENTITY_ID,
              lid: process.env.FAKE_WA_IDENTITY_LID,
              name: process.env.FAKE_WA_IDENTITY_NAME,
              verifiedName: process.env.FAKE_WA_IDENTITY_VERIFIED_NAME,
            },
            ev: {
              on(name, handler) {
                const existing = handlers.get(name) || [];
                existing.push(handler);
                handlers.set(name, existing);
                if (name === 'connection.update') {
                  // Drive the QR branch: a synthetic pairing payload, used to
                  // prove it is rendered only on a tty and never persisted.
                  if (process.env.FAKE_WA_QR) {
                    setImmediate(() => emit(name, { qr: process.env.FAKE_WA_QR }));
                    return;
                  }
                  if (versionLifecycle && socketNumber === 1) {
                    setImmediate(() => {
                      if (versionLifecycle === 'open_then_close') {
                        emit(name, { connection: 'open' });
                        setTimeout(() => emit(name, {
                          connection: 'close',
                          lastDisconnect: { error: { output: { statusCode: 515 } } },
                        }), 20);
                      } else if (versionLifecycle === 'close_before_open') {
                        emit(name, {
                          connection: 'close',
                          lastDisconnect: { error: { output: { statusCode: 515 } } },
                        });
                      }
                    });
                  } else {
                    setImmediate(() => emit(name, { connection: 'open' }));
                  }
                }
                if (name === 'messages.upsert' && process.env.FAKE_WA_EMIT_HOSTILE_DEBUG === '1') {
                  setImmediate(emitHostileDebugMessages);
                }
              },
            },
            async sendMessage(chatId, payload) {
              if (chatId === process.env.FAKE_WA_POLL_SUCCESS_CHAT && payload?.poll) {
                setTimeout(() => emitPollFailures(chatId), 20);
                return { key: { id: pollId(), remoteJid: chatId, fromMe: true } };
              }
              throw syntheticError();
            },
            async readMessages() { throw syntheticError(); },
            async sendPresenceUpdate() { throw syntheticError(); },
            async groupMetadata() { throw syntheticError(); },
            async updateMediaMessage() { throw syntheticError(); },
          };
          return sock;
        }

        export async function useMultiFileAuthState(dir) {
          if (process.env.FAKE_WA_START_FAILURE === '1') throw syntheticError();
          // Real Baileys creates the auth directory and writes creds.json with
          // plain fs calls, so the resulting modes are decided entirely by the
          // process umask. Reproduce that faithfully — with no explicit mode —
          // so tests observe the boundary the bridge itself must establish.
          if (process.env.FAKE_WA_WRITE_CREDS === '1' && dir) {
            const fs = await import('node:fs');
            const nodePath = await import('node:path');
            const authDir = nodePath.join(dir, 'auth');
            fs.mkdirSync(authDir, { recursive: true });
            fs.writeFileSync(
              nodePath.join(dir, 'creds.json'),
              JSON.stringify({ me: { id: 'SYNTHETIC_FAKE_ID' } }),
            );
          }
          return { state: {}, saveCreds() {} };
        }

        export async function fetchLatestBaileysVersion() {
          if (versionLifecycle) {
            versionFetchCalls += 1;
            if (versionFetchCalls === 1) return { version: [2, 3000, 321], isLatest: true };
            throw syntheticError();
          }
          return { version: [2, 3000, 0], isLatest: true };
        }
        export async function downloadMediaMessage() { return Buffer.alloc(0); }
        export function getAggregateVotesInPollMessage() {
          return [{
            name: process.env.FAKE_WA_POLL_OPTION_SECRET || 'synthetic option',
            voters: [process.env.FAKE_WA_DEBUG_SENDER || 'synthetic voter'],
          }];
        }
        export function decryptPollVote() { throw syntheticError(); }
        export function getKeyAuthor(key) { return key?.participant || key?.remoteJid || ''; }
        export function jidNormalizedUser(value) { return value || ''; }
      `,
    };
  },
});
