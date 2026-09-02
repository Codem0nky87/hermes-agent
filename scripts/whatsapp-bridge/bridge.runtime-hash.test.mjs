/** Security and parity contracts for the composite bridge runtime hash. */

import { strict as assert } from 'node:assert';
import { createHash } from 'node:crypto';
import * as realFs from 'node:fs';
import { execFileSync, spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  BRIDGE_MISSING_INPUT_SENTINEL,
  BRIDGE_RUNTIME_INPUTS,
  computeBridgeRuntimeHash,
  fingerprintBridgeInputs,
} from './bridge_helpers.js';

const FIXED_ERROR = 'WhatsApp bridge runtime inputs are unavailable.';
const makeRoot = () => realFs.mkdtempSync(path.join(tmpdir(), 'wa-runtime-hash-'));
const HERE = path.dirname(fileURLToPath(import.meta.url));

function seedRuntime(root) {
  for (const filename of BRIDGE_RUNTIME_INPUTS) {
    realFs.writeFileSync(path.join(root, filename), `production:${filename}\n`);
  }
}

function expectedHash(root) {
  const hash = createHash('sha256');
  for (const filename of [...BRIDGE_RUNTIME_INPUTS].sort()) {
    hash.update(filename, 'utf8');
    hash.update(Buffer.from([0]));
    const file = path.join(root, filename);
    hash.update(realFs.existsSync(file)
      ? realFs.readFileSync(file)
      : Buffer.from(BRIDGE_MISSING_INPUT_SENTINEL, 'utf8'));
    hash.update(Buffer.from([0]));
  }
  return hash.digest('hex');
}

{
  const root = makeRoot();
  seedRuntime(root);
  const result = fingerprintBridgeInputs(root, [...BRIDGE_RUNTIME_INPUTS].reverse());
  assert.equal(result.allPresent, true);
  assert.equal(result.hash, expectedHash(root));
  assert.match(result.hash, /^[a-f0-9]{64}$/);
  assert.equal(computeBridgeRuntimeHash(root), result.hash);

  const beforeHelper = result.hash;
  realFs.appendFileSync(path.join(root, 'bridge_helpers.js'), '// helper-only change\n');
  assert.notEqual(computeBridgeRuntimeHash(root), beforeHelper);

  const beforeLock = computeBridgeRuntimeHash(root);
  realFs.appendFileSync(path.join(root, 'package-lock.json'), '// lock-only change\n');
  assert.notEqual(computeBridgeRuntimeHash(root), beforeLock);
}

{
  const root = makeRoot();
  seedRuntime(root);
  realFs.unlinkSync(path.join(root, 'connection_close.js'));
  const missing = fingerprintBridgeInputs(root);
  assert.equal(missing.allPresent, false);
  assert.equal(missing.hash, expectedHash(root), 'missing files use the stable sentinel');
  assert.throws(() => computeBridgeRuntimeHash(root), error => {
    assert.equal(error.message, FIXED_ERROR);
    assert.ok(!error.message.includes(root));
    return true;
  });
}

for (const hostileKind of ['symlink', 'directory', 'oversize']) {
  const root = makeRoot();
  seedRuntime(root);
  const target = path.join(root, 'allowlist.js');
  realFs.unlinkSync(target);
  if (hostileKind === 'symlink') {
    const secret = path.join(root, 'private-target');
    realFs.writeFileSync(secret, 'private-content');
    realFs.symlinkSync(secret, target);
  } else if (hostileKind === 'directory') {
    realFs.mkdirSync(target);
  } else {
    realFs.writeFileSync(target, Buffer.alloc((4 * 1024 * 1024) + 1, 0x61));
  }
  assert.throws(() => computeBridgeRuntimeHash(root), error => {
    assert.equal(error.message, FIXED_ERROR);
    assert.ok(!error.message.includes(root));
    assert.ok(!error.message.includes('private-content'));
    return true;
  }, hostileKind);
}

if (process.platform !== 'win32') {
  const root = makeRoot();
  seedRuntime(root);
  const fifo = path.join(root, 'owner_message_gate.js');
  realFs.unlinkSync(fifo);
  execFileSync('mkfifo', [fifo]);
  const started = Date.now();
  assert.throws(() => computeBridgeRuntimeHash(root), { message: FIXED_ERROR });
  assert.ok(Date.now() - started < 1000, 'FIFO rejection must not block');
}

{
  const root = makeRoot();
  seedRuntime(root);
  const changed = path.join(root, 'allowlist.js');
  let mutated = false;
  const fsImpl = {
    ...realFs,
    fstatSync(fd, options) {
      const result = realFs.fstatSync(fd, options);
      if (!mutated) {
        mutated = true;
        realFs.appendFileSync(changed, '// changed during read\n');
      }
      return result;
    },
  };
  assert.throws(
    () => computeBridgeRuntimeHash(root, { fs: fsImpl }),
    { message: FIXED_ERROR },
    'pre/post identity changes fail closed',
  );
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

// The compatibility field and the new explicit field both report the same
// composite value from the actual bridge health endpoint.
{
  const port = await reservePort();
  const sessionDir = makeRoot();
  const child = spawn(
    process.execPath,
    [
      '--import', path.join(HERE, 'bridge.fake-baileys-loader.mjs'),
      path.join(HERE, 'bridge.js'),
      '--port', String(port),
      '--session', sessionDir,
      '--mode', 'bot',
    ],
    {
      cwd: HERE,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, WHATSAPP_ALLOWED_USERS: '', WHATSAPP_MODE: 'bot' },
    },
  );
  let output = '';
  child.stdout.on('data', chunk => { output += chunk.toString('utf8'); });
  child.stderr.on('data', chunk => { output += chunk.toString('utf8'); });
  let health;
  try {
    await Promise.race([
      (async () => {
        while (child.exitCode === null) {
          try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            if (response.ok) {
              health = await response.json();
              if (health.status === 'connected') return;
            }
          } catch {}
          await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error('bridge exited before health became ready');
      })(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('health timed out')), 5_000)),
    ]);
  } finally {
    if (child.exitCode === null) {
      child.kill('SIGTERM');
      await new Promise(resolve => child.once('exit', resolve));
    }
  }
  const desired = computeBridgeRuntimeHash(HERE);
  assert.equal(health.runtimeHash, desired, output);
  assert.equal(health.scriptHash, desired, output);
}

console.log('bridge.runtime-hash.test.mjs: all assertions passed');
