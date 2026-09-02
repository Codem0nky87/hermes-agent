/** Subprocess proof that bridge.js promotes only the version of an opened socket. */

import { strict as assert } from 'node:assert';
import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(HERE, 'bridge.js');
const FAKE_BAILEYS_LOADER = path.join(HERE, 'bridge.fake-baileys-loader.mjs');
const CANDIDATE = [2, 3000, 321];

async function runLifecycle(mode) {
  const scratch = mkdtempSync(path.join(tmpdir(), 'wa-version-lifecycle-'));
  const sessionDir = path.join(scratch, 'session');
  const tracePath = path.join(scratch, 'versions.jsonl');
  const child = spawn(
    process.execPath,
    ['--import', FAKE_BAILEYS_LOADER, BRIDGE, '--port', '0', '--session', sessionDir, '--mode', 'bot'],
    {
      cwd: HERE,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        WHATSAPP_ALLOWED_USERS: '',
        WHATSAPP_MODE: 'bot',
        FAKE_WA_VERSION_LIFECYCLE: mode,
        FAKE_WA_VERSION_TRACE: tracePath,
      },
    },
  );

  let stdout = '';
  let stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk.toString('utf8'); });
  child.stderr.on('data', chunk => { stderr += chunk.toString('utf8'); });

  try {
    await Promise.race([
      new Promise((resolve, reject) => {
        const poll = () => {
          const lines = existsSync(tracePath)
            ? readFileSync(tracePath, 'utf8').split('\n').filter(Boolean)
            : [];
          if (lines.length >= 2) resolve();
          else if (child.exitCode !== null) reject(new Error(`bridge exited ${child.exitCode}: ${stdout}\n${stderr}`));
          else setTimeout(poll, 10);
        };
        poll();
      }),
      new Promise((_, reject) => setTimeout(
        () => reject(new Error(`version lifecycle timed out: ${stdout}\n${stderr}`)),
        5_000,
      )),
    ]);
  } finally {
    if (child.exitCode === null) {
      child.kill('SIGTERM');
      await new Promise(resolve => child.once('exit', resolve));
    }
  }

  return readFileSync(tracePath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map(line => JSON.parse(line));
}

// The exact candidate used by socket one becomes fallback only after that
// socket emits connection:'open'. A failed second fetch must therefore give
// socket two the same confirmed version.
{
  const versions = await runLifecycle('open_then_close');
  assert.deepEqual(versions, [CANDIDATE, CANDIDATE]);
}

// Merely constructing a socket with a fetched candidate is not confirmation.
// If it closes before open, the failed second fetch must use the package
// default (represented by no `version` option), not the rejected candidate.
{
  const versions = await runLifecycle('close_before_open');
  assert.deepEqual(versions, [CANDIDATE, null]);
}

console.log('bridge.version-lifecycle.test.mjs: all assertions passed');
