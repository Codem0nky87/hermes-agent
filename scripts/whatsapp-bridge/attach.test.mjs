/**
 * Unit tests for the batch-attach protocol helpers (attach.js).
 *
 * attach.js is pure — no socket, no HTTP — so these run without importing
 * bridge.js (which starts an HTTP server and Baileys socket at load).
 */

import { strict as assert } from 'node:assert';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

import {
  ATTACH_NOT_COLLECTING,
  ATTACH_START_ACK,
  ATTACH_USAGE,
  MAX_MANIFEST_BATCHES,
  attachManifestPath,
  buildAttachSummary,
  displayNameForEvent,
  humanSize,
  loadAttachManifest,
  parseAttachCommand,
  persistAttachManifest,
} from './attach.js';

// -- command parsing --------------------------------------------------------
{
  assert.equal(parseAttachCommand('/attach start'), 'start');
  assert.equal(parseAttachCommand('  /attach start  '), 'start');
  assert.equal(parseAttachCommand('/ATTACH STOP'), 'stop');
  assert.equal(parseAttachCommand('/Attach Start\n'), 'start');
  assert.equal(parseAttachCommand('/attach'), 'usage');
  assert.equal(parseAttachCommand('/attach help'), 'usage');
  assert.equal(parseAttachCommand('/attachstop'), null);
  assert.equal(parseAttachCommand('please /attach start now'), null);
  assert.equal(parseAttachCommand('/attach start extra'), null);
  assert.equal(parseAttachCommand(''), null);
  assert.equal(parseAttachCommand(null), null);
  console.log('  ✓ /attach command parsing is strict and case-insensitive');
}

// -- human size ---------------------------------------------------------------
{
  assert.equal(humanSize(-1), '');
  assert.equal(humanSize(0), '0 B');
  assert.equal(humanSize(512), '512 B');
  assert.equal(humanSize(1024), '1 KB');
  assert.equal(humanSize(1536), '1.5 KB');
  assert.equal(humanSize(2 * 1024 * 1024), '2 MB');
  assert.equal(humanSize(768 * 1024), '768 KB');
  console.log('  ✓ humanSize formats bytes compactly');
}

// -- display name -------------------------------------------------------------
{
  assert.equal(
    displayNameForEvent({ fileName: 'report.pdf', mediaType: 'document', mediaUrls: ['/x/doc-1.bin'], messageId: 'm1' }),
    'report.pdf',
  );
  assert.equal(
    displayNameForEvent({ fileName: '', mediaType: 'image', mediaUrls: ['/x/img-9.jpg'], messageId: 'm1' }),
    'img-9.jpg',
  );
  assert.equal(
    displayNameForEvent({ fileName: '', mediaType: 'video', mediaUrls: [], messageId: 'very-long-message-id-42' }),
    'video-very-long-me',
  );
  console.log('  ✓ displayNameForEvent prefers original name, then cache basename');
}

// -- manifest path ------------------------------------------------------------
{
  const { dir, file } = attachManifestPath('/home/u/.hermes/whatsapp/session', '6741234567890:12@s.whatsapp.net');
  assert.equal(dir, '/home/u/.hermes/whatsapp/whatsapp-attach/6741234567890:12');
  assert.equal(file, path.join(dir, 'manifest.json'));
  console.log('  ✓ attachManifestPath keeps state next to the session dir, keyed by chat number');
}

// -- summary -------------------------------------------------------------------
{
  const summary = buildAttachSummary([
    { seq: 1, name: 'a.pdf', sizeBytes: 1536, path: '/x/a' },
    { seq: 2, name: 'b.mp4', sizeBytes: 2 * 1024 * 1024, path: '/x/b' },
  ]);
  assert.equal(summary.startsWith('📥 2 attachments ready:'), true);
  assert.match(summary, /1\. a\.pdf \(1\.5 KB\)/);
  assert.match(summary, /2\. b\.mp4 \(2 MB\)/);
  assert.equal(summary.endsWith('What should I do with them?'), true);

  assert.match(buildAttachSummary([{ seq: 1, name: 'solo.png', sizeBytes: 10 }]), /1 attachment ready/);
  assert.match(
    buildAttachSummary([{ seq: 1, name: 'gone.pdf', sizeBytes: -1, failed: true }]),
    /gone\.pdf — NOT downloaded/,
  );
  assert.match(buildAttachSummary([]), /No attachments were received/);
  console.log('  ✓ buildAttachSummary lists name+size and asks first');
}

// -- manifest persistence -------------------------------------------------------
{
  const tmp = mkdtempSync(path.join(tmpdir(), 'attach-test-'));
  try {
    const dir = path.join(tmp, 'chat1');
    const file = path.join(dir, 'manifest.json');
    const batch = { id: 'b1', startedAt: 't0', chatId: 'c@x', seq: 0, files: [] };

    assert.equal(persistAttachManifest({ dir, file, chatId: 'c@x', batch }), true);
    let manifest = loadAttachManifest(file);
    assert.equal(manifest.chatId, 'c@x');
    assert.equal(manifest.batches.length, 1);

    // Re-persisting the same batch id updates in place (no duplicate).
    batch.seq = 2;
    batch.files = [
      { seq: 1, name: 'a.pdf', sizeBytes: 100, path: '/x/a' },
      { seq: 2, name: 'b.png', sizeBytes: 200, path: '/x/b' },
    ];
    assert.equal(persistAttachManifest({ dir, file, chatId: 'c@x', batch }), true);
    manifest = loadAttachManifest(file);
    assert.equal(manifest.batches.length, 1);
    assert.equal(manifest.batches[0].seq, 2);
    assert.equal(manifest.batches[0].files.length, 2);

    // A second batch appends; the cap evicts the oldest batches.
    for (let i = 0; i < MAX_MANIFEST_BATCHES; i += 1) {
      persistAttachManifest({
        dir, file, chatId: 'c@x',
        batch: { id: `b${i + 2}`, startedAt: `t${i}`, chatId: 'c@x', seq: 0, files: [] },
      });
    }
    manifest = loadAttachManifest(file);
    assert.equal(manifest.batches.length, MAX_MANIFEST_BATCHES);
    assert.equal(manifest.batches[0].id, 'b2'); // b1 evicted by the cap

    // Corrupt manifest reads back as fresh, and the next write recovers.
    writeFileSync(file, '{not json');
    assert.deepEqual(loadAttachManifest(file), { chatId: '', batches: [] });
    assert.equal(
      persistAttachManifest({ dir, file, chatId: 'c@x', batch: { id: 'bx', startedAt: 't', chatId: 'c@x', seq: 0, files: [] } }),
      true,
    );
    assert.equal(loadAttachManifest(file).batches.length, 1);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
  console.log('  ✓ manifest upsert/cap/corrupt-recovery behave');
}

// -- constants ------------------------------------------------------------------
{
  assert.match(ATTACH_START_ACK, /waiting for them/i);
  assert.equal(typeof ATTACH_NOT_COLLECTING, 'string');
  assert.match(ATTACH_USAGE, /\/attach start/);
  console.log('  ✓ ack/usage copy is present and terse');
}
