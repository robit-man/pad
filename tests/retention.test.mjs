import { before, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { startBrowser } from './browser.mjs';

let app;
before(async () => { app = await startBrowser(); });
after(async () => { await app?.close(); });

async function room(t, key, options) {
  const context = await app.browser.newContext();
  t.after(() => context.close());
  return { context, ...await app.room(context, key, options) };
}

test('idle sessions create no speculative peers and retain healthy sockets without rotating', async t => {
  const { page, errors }=await room(t);
  const result=await page.evaluate(async()=>{
    const a=window.__padTest,now=Date.now,base=now();
    for(const c of window.__mockMqtt)for(const done of c.pending.splice(0))done();
    window.__networkOptions.autoAck=true;
    try{for(let i=0;i<120;i++){Date.now=()=>base+i*15000;await a.meshSupervisorTick();await Promise.all([a.startNats(),a.startMqttRelay(),a.startNostrRelay()])}}finally{Date.now=now}
    const counts={mqtt:window.__mockMqtt.length,nostr:window.__mockSockets.length,nats:window.__mockNats.length,rtc:window.__rtcCreated};
    a.stopMesh();await Promise.resolve();
    return{counts,closed:window.__mockSockets.every(s=>s.readyState===3)&&window.__mockMqtt.every(c=>c.ended),wrappers:a.meshWrappers.size}
  });
  assert.deepEqual(result.counts,{mqtt:2,nostr:2,nats:1,rtc:0});assert.equal(result.closed,true);assert.equal(result.wrappers,0);assert.deepEqual(errors,[])
});

test('a sustained outage obeys global and per-endpoint backoff rather than SDK reconnect loops', async t => {
  const { page, errors }=await room(t,'OUTAGE',{failAll:true});
  const result=await page.evaluate(async()=>{
    const a=window.__padTest,now=Date.now,base=now();let peak=0;
    try{for(let seconds=0;seconds<=1800;seconds+=5){Date.now=()=>base+seconds*1000;await a.meshSupervisorTick();await Promise.all([a.startNats(),a.startMqttRelay(),a.startNostrRelay()]);await Promise.resolve();peak=Math.max(peak,a.mqttRelayState.clients.size+a.nostrRelayState.clients.size+Number(!!a.natsState.nc))}}finally{Date.now=now}
    const health=Object.values(window.ScratchMesh.endpointHealth());
    return{health,peak,rtc:window.__rtcCreated,times:[...window.__mockSockets.map(s=>s.time),...window.__mockMqtt.map(c=>c.time),...window.__mockNatsAttempts.map(x=>x.time)].sort((a,b)=>a-b),mqttOptions:window.__mockMqtt.map(c=>c.options.reconnectPeriod),natsOptions:window.__mockNatsAttempts.map(x=>x.options.reconnect)}
  });
  assert.ok(result.times.length<50,`${result.times.length} attempts in simulated 30 minute outage`);
  for(const time of result.times)assert.ok(result.times.filter(t=>t>=time&&t<time+60000).length<=5,'more than five connections per minute');
  assert.ok(result.mqttOptions.every(x=>x===0));assert.ok(result.natsOptions.every(x=>x===false));
  assert.ok(result.peak<=5);assert.equal(result.rtc,0);assert.ok(result.health.every(x=>!x.connected&&!x.connecting));assert.deepEqual(errors,[])
});

test('unacknowledged publications remain bounded and ICE uses at most four server URLs', async t => {
  const {page,errors}=await room(t);
  const result=await page.evaluate(async()=>{
    const a=window.__padTest;
    await Promise.all(Array.from({length:100},()=>a.publishMqttRelay('hello')));
    const pending=[...a.mqttRelayState.clients.values()].map(c=>c.pending.length);
    for(const c of a.mqttRelayState.clients.values())for(const done of c.pending.splice(0))done();
    window.ScratchMesh.setTurn(Array.from({length:10},(_,i)=>({urls:[`turn:turn${i}.invalid:3478`,`turns:turn${i}.invalid:5349`],username:'user',credential:'pass'})));
    return{pending,ice:a.rtcConfig().iceServers.flatMap(x=>x.urls),rtc:window.__rtcCreated}
  });
  assert.ok(result.pending.every(n=>n<=16));assert.ok(result.ice.length<=4);assert.equal(result.rtc,0);assert.deepEqual(errors,[])
});

test('real-peer upgrades cap both active WebRTC sessions and failed-peer retries',async t=>{
  const{page,errors}=await room(t);
  const result=await page.evaluate(async()=>{
    const a=window.__padTest,original=window.RTCPeerConnection;let created=0,closed=0;
    window.RTCPeerConnection=class{
      iceGatheringState='complete';connectionState='new';
      constructor(){created++}
      createDataChannel(){return{readyState:'connecting',bufferedAmount:0,close(){}}}
      async createOffer(){return{type:'offer',sdp:'test-sdp'}}
      async setLocalDescription(offer){this.localDescription=offer}
      close(){closed++;this.connectionState='closed'}
    };
    await Promise.all(Array.from({length:100},(_,i)=>a.makeNatsOffer('peer-'+i)));
    const active=a.natsState.sessions.size;
    for(const x of [...a.natsState.sessions.values()]){x.pc.connectionState='failed';x.pc.onconnectionstatechange()}
    await Promise.all(Array.from({length:100},(_,i)=>a.makeNatsOffer('peer-'+i)));
    window.RTCPeerConnection=original;
    return{created,closed,active,remaining:a.natsState.sessions.size}
  });
  assert.deepEqual(result,{created:4,closed:4,active:4,remaining:0});assert.deepEqual(errors,[])
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

test('late transport startup cannot recreate resources after stop',async t=>{
  const{page,errors}=await room(t);
  const result=await page.evaluate(async()=>{
    const a=window.__padTest,now=Date.now;
    await a.natsState.nc.close();a.natsState.nc=null;
    for(const sub of[a.natsState.presence,a.natsState.signal,a.natsState.relay])sub.unsubscribe();
    a.endpointReleased('wss://demo.nats.io:8443');Date.now=()=>now()+60001;
    let finish,entered;const ready=new Promise(r=>entered=r),pending=new Promise(r=>finish=r);
    a.natsState.module={wsconnect:()=>{entered();return pending}};
    const job=a.startNats();await ready;a.stopMesh();let closed=0;finish({close:async()=>closed++});const started=await job;Date.now=now;
    return{started,closed,nc:!!a.natsState.nc,sockets:window.__mockSockets.filter(s=>s.readyState!==3).length}
  });
  assert.equal(result.started,false);assert.equal(result.closed,1);assert.equal(result.nc,false);assert.equal(result.sockets,0);assert.deepEqual(errors,[])
});

test('received media survives cache eviction and an actual page reload', async t => {
  const { page, errors } = await room(t, 'MEDIA');
  const hash = await page.evaluate(async () => {
    const a = window.__padTest;
    const data = Uint8Array.from(atob((()=>{const canvas=document.createElement('canvas');canvas.width=canvas.height=1;return canvas.toDataURL('image/png').split(',')[1]})()), c => c.charCodeAt(0));
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

test('expired peers release route records, fallback wrappers, probes, and retries',async t=>{
  const{page,errors}=await room(t);
  const result=await page.evaluate(async()=>{
    const a=window.__padTest,old=Date.now()-a.limits.ROUTE_STALE_MS*3;
    for(let i=0;i<100;i++){const actor='expired-'+i,w=a.ensureRelayPeerRoute(actor),r=a.peerRoutes.get(actor).get(a.routeKey(w,w.name));r.lastSeen=old;a.relayActors.set(actor,{lastSeen:old,planes:{}});a.routePending('pending-'+i,'ping',r)}
    a.ensureRelayPeerRoute('healthy-actor');let closed=0,retried=false;
    a.natsState.sessions.set('expired-nats',{lastSeen:old,retryTimer:setTimeout(()=>retried=true,100),pc:{close(){closed++}}});
    a.prunePeerRoutes();await new Promise(r=>setTimeout(r,150));
    return{actors:[...a.peerRoutes.keys()],wrappers:a.meshWrappers.size,relayActors:a.relayActors.size,pending:a.routeBenchPending.size,sessions:a.natsState.sessions.size,closed,retried}
  });
  assert.deepEqual(result.actors,['healthy-actor']);assert.equal(result.wrappers,1);assert.equal(result.relayActors,0);assert.equal(result.pending,0);assert.equal(result.sessions,0);assert.equal(result.closed,1);assert.equal(result.retried,false);assert.deepEqual(errors,[])
});

test('encrypted relays synchronize text and media without WebRTC or shared browser storage',async t=>{
  const left=await app.browser.newContext(),right=await app.browser.newContext();t.after(()=>left.close());t.after(()=>right.close());
  const options={broker:true,noBroadcast:true,autoAck:true};
  const{page:a,errors:aErrors}=await app.room(left,'RELAY',options),{page:b,errors:bErrors}=await app.room(right,'RELAY',options);
  await a.locator('#editor').fill('text sent over relay');await a.locator('#editor').dispatchEvent('input');
  await b.waitForFunction(()=>document.querySelector('#editor').value==='text sent over relay');
  await a.evaluate(async()=>{
    const t=window.__padTest,data=Uint8Array.from(atob((()=>{const canvas=document.createElement('canvas');canvas.width=canvas.height=1;return canvas.toDataURL('image/png').split(',')[1]})()),c=>c.charCodeAt(0)),blob=new Blob([data],{type:'image/png'}),hash=await t.sha256Blob(blob);
    await t.putMediaBlob(hash,blob);t.mediaMap.set('relayed',{page:t.activePage,hash,name:'pixel.png',type:blob.type,size:blob.size,mode:'freeform',w:120,h:90})
  });
  await b.waitForFunction(()=>document.querySelector('.mediaItem img')?.naturalWidth===1);
  assert.equal(await b.evaluate(()=>window.__rtcCreated),0);assert.deepEqual(aErrors,[]);assert.deepEqual(bErrors,[])
});
