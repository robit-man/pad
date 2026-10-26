import { before, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { startBrowser } from './browser.mjs';

let app;
before(async () => { app = await startBrowser(); });
after(async () => { await app?.close(); });

async function room(t, key) {
  const context = await app.browser.newContext();
  t.after(() => context.close());
  return { context, ...await app.room(context, key) };
}

test('rotations await teardown, deduplicate joins, and leave no late work after stopping', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest;
    const old = a.meshWrappers.get('nostr:0');
    for (let epoch = 1; epoch <= 8; epoch++) {
      await Promise.all(Array.from({ length: 8 }, () => a.joinStrategy('nostr', 0, epoch, true)));
    }
    const before = { ...window.__mockMesh };
    old.room.onPeerJoin('late-peer');old.meta.onMessage({ actor: 'late-actor' }, 'late-peer');
    const stalePeers = old.peers.size, staleRoutes = a.peerRoutes.has('late-actor');
    await a.rotateIdleStrategies(100);
    a.stopMesh();
    await new Promise(r => setTimeout(r, 2300));
    return { before, after: window.__mockMesh, stalePeers, staleRoutes, wrappers: a.meshWrappers.size, jobs: a.meshStrategyJobs.size, timers: a.meshRotationTimers.size };
  });
  assert.equal(result.before.joins, 15);
  assert.equal(result.before.live, 7);
  assert.equal(result.before.reused, 0);
  assert.equal(result.before.races, 0);
  assert.equal(result.stalePeers, 0);
  assert.equal(result.staleRoutes, false);
  assert.equal(result.after.live, 0);
  assert.equal(result.wrappers + result.jobs + result.timers, 0);
  assert.deepEqual(errors, []);
});

test('MQTT rotation retires obsolete clients and bounds unacknowledged publications', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest, now = Date.now;
    let max = 0;
    try {
      for (let epoch = 1; epoch <= 30; epoch++) {
        Date.now = () => epoch * 30000;
        await Promise.all(Array.from({ length: 5 }, () => a.startMqttRelay()));
        max = Math.max(max, a.mqttRelayState.clients.size);
      }
    } finally { Date.now = now; }
    for (const [url, c] of a.mqttRelayState.clients) a.mqttRelayState.ready.add(url);
    await Promise.all(Array.from({ length: 100 }, () => a.publishMqttRelay('hello')));
    const clients = [...a.mqttRelayState.clients.values()];
    const pending = clients.map(c => c.pending.length);
    for (const c of clients) for (const done of c.pending.splice(0)) done();
    return { max, current: clients.length, pending, retired: window.__mockMqtt.filter(c => c.ended).length,
      forgottenLive: window.__mockMqtt.filter(c => !c.ended && !clients.includes(c)).length,
      counters: clients.map(c => c.padPending), limit: a.limits.MQTT_MAX_PENDING };
  });
  assert.equal(result.max, 7);
  assert.equal(result.current, 7);
  assert.ok(result.retired > 0);
  assert.equal(result.forgottenLive, 0);
  assert.ok(result.pending.every(n => n <= result.limit));
  assert.ok(result.counters.every(n => n === 0));
  assert.deepEqual(errors, []);
});

test('deleted page contents disappear from peers, snapshots, and reloads; legacy orphans are reclaimed', async t => {
  const { page, context, errors } = await room(t, 'DELETE');
  const peer = (await app.room(context, 'DELETE')).page;
  const sharedId = await page.evaluate(() => {
    const a = window.__padTest;a.newPage();a.pageText(a.activePage).insert(0, 'shared page');
    window.__offline = new a.Y.Doc();a.Y.applyUpdate(window.__offline, a.Y.encodeStateAsUpdate(a.ydoc));
    return a.activePage;
  });
  await peer.waitForFunction(id => window.__padTest.pageText(id).toString() === 'shared page', sharedId);
  await page.evaluate(id => window.__padTest.deletePage(id), sharedId);
  await peer.waitForFunction(id => !window.__padTest.pageOrder.toArray().includes(id) && window.__padTest.pageText(id).length === 0, sharedId);
  const lateLength = await page.evaluate(id => {
    const a = window.__padTest;
    window.__offline.getText('page:' + id).insert(0, 'late edit');
    a.Y.applyUpdate(a.ydoc, a.Y.encodeStateAsUpdate(window.__offline));window.__offline.destroy();
    return a.pageText(id).length;
  }, sharedId);
  assert.ok(lateLength > 0);
  await page.waitForFunction(id => window.__padTest.pageText(id).length === 0, sharedId);
  const deleted = await page.evaluate(async () => {
    const a = window.__padTest;
    for (let i = 0; i < 12; i++) {
      a.newPage(); const id = a.activePage, text = a.pageText(id);
      a.getUndoManager(id);
      a.ydoc.transact(() => text.insert(0, 'x'.repeat(256 * 1024)), a.origin);
      a.deletePage(id);
    }
    a.pageText('legacy-orphan').insert(0, 'z'.repeat(1024 * 1024));
    a.reclaimDeletedPageText(null, true);
    await a.snapshotRoomState({ syncLocal: true });
    return { snapshot: a.Y.encodeStateAsUpdate(a.ydoc).length,
      retained: [...a.ydoc.share.keys()].filter(k => k.startsWith('page:')).reduce((n, k) => n + a.pageText(k.slice(5)).length, 0),
      active: a.activePage, managers: a.undoManagers.size };
  });
  assert.equal(deleted.retained, 0);
  assert.ok(deleted.snapshot < 10000, `snapshot is ${deleted.snapshot} bytes`);
  await peer.waitForFunction(() => window.__padTest.pageOrder.length === 1);
  await page.reload();
  await page.waitForFunction(() => window.__padTest?.pageOrder.length);
  assert.equal(await page.locator('#editor').inputValue(), '');
  assert.deepEqual(errors, []);
});

test('text arriving before its page-order update is preserved during deletion cleanup', async t => {
  const { page, errors } = await room(t);
  await page.evaluate(() => {
    const a = window.__padTest;a.newPage();a.deletePage(a.activePage);
    a.pageText('pending-order').insert(0, 'keep until the page order arrives');
  });
  await new Promise(r => setTimeout(r, 250));
  assert.equal(await page.evaluate(() => window.__padTest.pageText('pending-order').toString()), 'keep until the page order arrives');
  await page.evaluate(() => { const a = window.__padTest;a.pageOrder.push(['pending-order']);a.setActivePage('pending-order'); });
  assert.equal(await page.locator('#editor').inputValue(), 'keep until the page order arrives');
  assert.deepEqual(errors, []);
});

test('late transport startup cannot recreate resources after stop', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest;
    await a.natsState.nc.close();a.natsState.nc = null;
    for (const sub of [a.natsState.presence, a.natsState.signal, a.natsState.relay]) sub.unsubscribe();
    let finish, entered;
    const ready = new Promise(r => entered = r), pending = new Promise(r => finish = r);
    a.natsState.module = { wsconnect: () => { entered();return pending; } };
    const nats = a.startNats();await ready;
    a.stopNostrRelay();const nostr = a.startNostrRelay();
    const pools = window.__mockNostr.length;
    a.stopMesh();
    let closed = 0;finish({ close: async () => closed++ });
    const started = await Promise.all([nats, nostr]);
    return { started, closed, nc: !!a.natsState.nc, pool: !!a.nostrRelayState.pool, newPools: window.__mockNostr.length - pools };
  });
  assert.deepEqual(result.started, [false, false]);
  assert.equal(result.closed, 1);
  assert.equal(result.nc, false);
  assert.equal(result.pool, false);
  assert.equal(result.newPools, 0);
  assert.deepEqual(errors, []);
});

test('received media survives cache eviction and an actual page reload', async t => {
  const { page, errors } = await room(t, 'MEDIA');
  const hash = await page.evaluate(async () => {
    const a = window.__padTest;
    const data = Uint8Array.from(atob('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aWQAAAABJRU5ErkJggg=='), c => c.charCodeAt(0));
    const blob = new Blob([data], { type: 'image/png' }), hash = await a.sha256Blob(blob);
    a.mediaMap.set('received', { page: a.activePage, hash, name: 'pixel.png', type: blob.type, size: blob.size, mode: 'freeform', w: 120, h: 90 });
    const half = Math.floor(data.length / 2);
    await a.receiveAssetChunk(a.assetChunkFrame(hash, 1, 2, data.slice(half)));
    await a.receiveAssetChunk(a.assetChunkFrame(hash, 0, 2, data.slice(0, half)));
    await a.snapshotRoomState({ syncLocal: true });return hash;
  });
  await page.waitForFunction(() => document.querySelector('.mediaItem img')?.naturalWidth === 1);
  await page.reload();
  await page.waitForFunction(() => document.querySelector('.mediaItem img')?.naturalWidth === 1);
  assert.equal(await page.evaluate(hash => window.__padTest.hasMediaBlob(hash), hash), true);
  assert.equal(await page.evaluate(() => window.__padTest.mediaRx.size), 0);
  assert.deepEqual(errors, []);
});

test('bounded undo retains recent undo/redo and releases old Yjs text and destroy listeners', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest, id = a.activePage, text = a.pageText(id), um = a.getUndoManager(id);
    for (let i = 0; i < 500; i++) {
      um.stopCapturing();
      a.ydoc.transact(() => { if (text.length) text.delete(0, text.length); text.insert(0, String(i).padEnd(1024, 'x')); }, a.origin);
      await Promise.resolve();
    }
    const count = um.undoStack.length;
    const retained = [...a.ydoc.store.clients.values()].flat().reduce((n, item) => n + (item.content?.str?.length || 0), 0);
    a.undoActivePage(); const previous = text.toString().slice(0, 3);
    a.redoActivePage(); const current = text.toString().slice(0, 3);
    for (let i = 0; i < 30; i++) { a.newPage(); a.getUndoManager(a.activePage); }
    const managers = a.undoManagers.size, destroyListeners = a.ydoc._observers.get('destroy').size;
    const large = a.pageText(a.activePage), largeUm = a.getUndoManager(a.activePage);
    largeUm.stopCapturing();a.ydoc.transact(() => large.insert(0, 'a'.repeat(2 * 1024 * 1024)), a.origin);
    await Promise.resolve();
    return { count, retained, previous, current, managers, destroyListeners, largeHistory: largeUm.undoStack.length, limits: a.limits };
  });
  assert.equal(result.count, result.limits.UNDO_MAX_ITEMS);
  assert.ok(result.retained < 150 * 1024, `${result.retained} historical characters remain`);
  assert.equal(result.previous, '498');
  assert.equal(result.current, '499');
  assert.ok(result.managers <= result.limits.UNDO_MAX_MANAGERS);
  assert.ok(result.destroyListeners <= result.managers + 3);
  assert.equal(result.largeHistory, 0);
  assert.deepEqual(errors, []);
});

test('media caching is bounded, concurrent URL requests share one allocation, and reload uses IndexedDB', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest, blob = new Blob([new Uint8Array(1024 * 1024)], { type: 'image/png' });
    for (let i = 0; i < 48; i++) await a.putMediaBlob('hash-' + i, blob);
    const warm = window.PADMedia.cache();
    const hash = 'hash-0';
    a.mediaMap.set('media-0', { page: a.activePage, hash, type: 'image/png', mode: 'freeform', w: 120, h: 90 });
    let created = 0, revoked = 0;
    const create = URL.createObjectURL, revoke = URL.revokeObjectURL;
    URL.createObjectURL = function(...args) { created++; return create.apply(this, args); };
    URL.revokeObjectURL = function(...args) { revoked++; return revoke.apply(this, args); };
    const urls = await Promise.all(Array.from({ length: 20 }, () => a.mediaUrl(hash)));
    a.revokeUnusedMediaUrls();
    const visible = window.PADMedia.cache();
    a.mediaMap.delete('media-0');a.revokeUnusedMediaUrls();
    const empty = window.PADMedia.cache();
    const disk = await a.hasMediaBlob(hash), restored = await a.getMediaBlob(hash);
    URL.createObjectURL = create;URL.revokeObjectURL = revoke;
    return { warm, visible, empty, created, revoked, unique: new Set(urls).size, disk, bytes: restored.size };
  });
  assert.ok(result.warm.bytes <= result.warm.maxBytes);
  assert.ok(result.warm.memory.length <= 32);
  assert.equal(result.created, 1);
  assert.equal(result.unique, 1);
  assert.deepEqual(result.visible.memory, ['hash-0']);
  assert.equal(result.empty.bytes, 0);
  assert.equal(result.empty.urls.length, 0);
  assert.equal(result.revoked, 1);
  assert.equal(result.disk, true);
  assert.equal(result.bytes, 1024 * 1024);
  assert.deepEqual(errors, []);
});

test('media presence scans do not load off-page blobs or run again for pointer updates', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest;
    for (let i = 0; i < 12; i++) {
      await a.putMediaBlob('cached-' + i, new Blob(['asset']));
      a.mediaMap.set('off-' + i, { page: 'another-page', hash: 'cached-' + i, type: 'image/png' });
    }
    a.revokeUnusedMediaUrls();
    await Promise.all(Array.from({ length: 20 }, () => a.requestMissingMediaAssets()));
    for (let i = 0; i < 100; i++) a.awareness.setLocalStateField('mouse', { x: i / 100, y: 0, t: Date.now() });
    await new Promise(r => setTimeout(r, 200));
    return window.PADMedia.cache();
  });
  assert.equal(result.bytes, 0);
  assert.equal(result.memory.length, 0);
  assert.equal(result.reads, 0);
  assert.deepEqual(errors, []);
});

test('slow snapshot writes coalesce, retain only the active copy, and persist the latest edit', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest;
    await a.snapshotRoomState();
    const writes = []; let release;
    const original = a.writer(async (room, update) => { writes.push(update); if (writes.length === 1) await new Promise(r => release = r); });
    const job = a.snapshotRoomState();
    const text = a.pageText(a.activePage);
    for (let i = 0; i < 200; i++) {
      a.ydoc.transact(() => text.insert(text.length, 'x'), a.origin);
      a.snapshotRoomState({ syncLocal: true });
    }
    const blockedWrites = writes.length;
    release();await job;
    const restored = new a.Y.Doc();a.Y.applyUpdate(restored, writes.at(-1));
    const length = restored.getText('page:' + a.activePage).length;restored.destroy();
    a.writer(original);await a.snapshotRoomState({ syncLocal: true });
    return { blockedWrites, writes: writes.length, length, busy: a.snapshotBusy };
  });
  assert.equal(result.blockedWrites, 1);
  assert.equal(result.writes, 2);
  assert.equal(result.length, 200);
  assert.equal(result.busy, false);
  await page.reload();
  await page.waitForFunction(() => window.__padTest?.pageOrder.length);
  assert.equal((await page.locator('#editor').inputValue()).length, 200);
  assert.deepEqual(errors, []);
});

test('expired peers release route maps, wrappers, pending probes and NATS retries; hints cannot keep them alive', async t => {
  const { page, errors } = await room(t);
  const result = await page.evaluate(async () => {
    const a = window.__padTest;
    const old = Date.now() - a.limits.ROUTE_STALE_MS * 3;
    for (let i = 0; i < 100; i++) {
      const actor = 'expired-' + i, addr = 'addr-' + i, w = a.getNknWrapper(addr, actor);
      w.lastSeen = old;
      const r = a.peerRoutes.get(actor).get(a.routeKey(w, 'nkn:' + addr));r.lastSeen = old;
      a.relayActors.set(actor, { lastSeen: old });
      a.routePending('pending-' + i, 'ping', r);
      await a.ensureNknRoute(addr, actor);
    }
    const healthy = a.getNknWrapper('healthy', 'healthy-actor');
    let closed = 0, retried = false;
    a.natsState.sessions.set('expired-nats', { lastSeen: old, retryTimer: setTimeout(() => retried = true, 100), pc: { close() { closed++; } } });
    a.prunePeerRoutes();
    await new Promise(r => setTimeout(r, 150));
    return { actors: [...a.peerRoutes.keys()], wrappers: a.nknState.routes.size, addressActors: a.nknState.actorByAddr.size,
      relayActors: a.relayActors.size, pending: a.routeBenchPending.size, sessions: a.natsState.sessions.size, closed, retried };
  });
  assert.deepEqual(result.actors, ['healthy-actor']);
  assert.equal(result.wrappers, 1);
  assert.equal(result.addressActors, 1);
  assert.equal(result.relayActors, 0);
  assert.equal(result.pending, 0);
  assert.equal(result.sessions, 0);
  assert.equal(result.closed, 1);
  assert.equal(result.retried, false);
  assert.deepEqual(errors, []);
});
