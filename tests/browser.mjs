import { readFile, access } from 'node:fs/promises';
import { createServer } from 'node:http';
import { build } from 'esbuild';
import { chromium } from 'playwright-core';

const root = new URL('../', import.meta.url);
const source = await readFile(new URL('index.html', root), 'utf8');
const bundles = new Map();
for (const [name, entry] of [['yjs', 'yjs'], ['awareness', 'y-protocols/awareness'], ['indexeddb', 'y-indexeddb']]) {
  const result = await build({
    stdin: { contents: `export * from '${entry}'`, resolveDir: root.pathname },
    bundle: true, write: false, format: 'esm', platform: 'browser',
    plugins: name === 'yjs' ? [] : [{ name: 'shared-yjs', setup(b) {
      b.onResolve({ filter: /^yjs$/ }, () => ({ path: 'https://esm.sh/yjs@13.6.27', external: true }));
    }}],
  });
  bundles.set(name, result.outputFiles[0].text);
}

const expose = `
window.__padTest={Y,ydoc,awareness,persistence,pageOrder,mediaMap,origin,
  newPage,deletePage,pageText,setActivePage,reclaimDeletedPageText,getUndoManager,undoActivePage,redoActivePage,trimUndoHistory,
  putMediaBlob,getMediaBlob,hasMediaBlob,mediaUrl,revokeUnusedMediaUrls,requestMissingMediaAssets,renderMediaPage,destroyMedia,
  snapshotRoomState,scheduleRoomSnapshot,flushRoomSnapshotForExit,joinStrategy,leaveStrategy,rotateIdleStrategies,
  startMqttRelay,publishMqttRelay,startNats,startNostrRelay,stopNostrRelay,stopMesh,meshSupervisorTick,prunePeerRoutes,registerPeerRoute,
  sha256Blob,assetChunkFrame,receiveAssetChunk,
  getNknWrapper,ensureNknRoute,routePending,routeKey,mediaMem,mediaUrls,mediaRx,mediaRequestAt,
  undoManagers,meshWrappers,meshStrategyJobs,meshRotationTimers,peerRoutes,peerPrimaryRoute,routePeerActors,routeBenchPending,relayActors,
  mqttRelayState,nknState,natsState,nostrRelayState,
  limits:{UNDO_MAX_ITEMS,UNDO_MAX_BYTES,UNDO_MAX_MANAGERS,MEDIA_CACHE_BYTES,MEDIA_CACHE_ITEMS,MQTT_RELAY_CLIENTS,MQTT_MAX_PENDING,ROUTE_STALE_MS},
  get activePage(){return activePage},get snapshotBusy(){return !!roomCacheWriteInFlight},
  writer(fn){const previous=writeRoomCache;writeRoomCache=fn;return previous},
  quiet(){clearInterval(meshSupervisorTimer);clearInterval(routeProbeTimer);for(const timer of meshRotationTimers.values())clearTimeout(timer);meshRotationTimers.clear()}
};`;
const html = source.replace(/<\/script>\s*<\/body>/, `${expose}</script></body>`);

const trystero = `
const occupied=new Map();
export function joinRoom(config,id){
  const key=config.appId+id,stats=window.__mockMesh;
  if(occupied.has(key)){stats.reused++;return occupied.get(key)}
  stats.joins++;stats.live++;
  const room={makeAction:()=>({send:async()=>true,onMessage:null}),leave:async()=>{
    stats.leaves++;await new Promise(r=>setTimeout(r,30));
    if(occupied.get(key)!==room)stats.races++;
    occupied.delete(key);stats.live--;
  }};occupied.set(key,room);return room
}`;
const mqtt = `
export function connect(url){
  const c={url,connected:true,ended:false,handlers:{},pending:[],
    on(event,fn){(this.handlers[event]??=[]).push(fn);return this},
    emit(event,...args){for(const fn of this.handlers[event]||[])fn(...args)},
    subscribe(topic,opts,cb){cb?.()},
    publish(topic,payload,opts,cb){this.pending.push(cb)},
    end(){this.ended=true;this.connected=false;this.emit('close')}
  };window.__mockMqtt.push(c);return c
}`;
const nats = `
export async function wsconnect(){
  const nc={closed:false,publish(){},subscribe(){let finish;return {
    [Symbol.asyncIterator](){return this},next(){return new Promise(r=>finish=r)},
    unsubscribe(){finish?.({done:true})}
  }},async close(){this.closed=true}};window.__mockNats.push(nc);return nc
}`;
const nostr = `
export const generateSecretKey=()=>new Uint8Array(32).fill(1),getPublicKey=()=> 'test-key';
export const finalizeEvent=(ev)=>({...ev,id:crypto.randomUUID()});
export class SimplePool{
  constructor(){this.closed=false;window.__mockNostr.push(this)}
  subscribeMany(urls,filter){if(Array.isArray(filter))throw new Error('Invalid nested Nostr filter');return {close:async()=>{this.subscriptionClosed=true}}}
  publish(){return [Promise.resolve()]}
  close(){this.closed=true}
}`;

export async function startBrowser() {
  let executablePath = process.env.CHROMIUM_PATH;
  if (!executablePath) {
    try { await access(chromium.executablePath()); }
    catch { try { await access('/snap/bin/chromium'); executablePath = '/snap/bin/chromium'; } catch {} }
  }
  const browser = await chromium.launch({ executablePath, headless: true, args: ['--disable-gpu', '--no-sandbox'] });
  const server = createServer((_, res) => { res.writeHead(200, { 'Content-Type': 'text/html' }); res.end(html); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  return {
    browser, base,
    async close() { await browser.close(); await new Promise(resolve => server.close(resolve)); },
    async room(context, key = 'TESTROOM') {
      const page = await context.newPage();
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.addInitScript(() => {
        window.__mockMesh = { joins: 0, leaves: 0, live: 0, reused: 0, races: 0 };
        window.__mockMqtt = []; window.__mockNats = []; window.__mockNostr = []; window.__mockNknSends = [];
        window.nkn = { MultiClient: class {
          addr = crypto.randomUUID();
          getSeed() { return 'seed'; }
          onMessage(fn) { this.message = fn; }
          onConnect(fn) { queueMicrotask(fn); }
          readyClientIDs() { return []; }
          async send(dest, raw) { window.__mockNknSends.push({ dest, raw }); }
          close() {}
        }};
      });
      await page.route('https://**/*', async route => {
        const url = route.request().url();
        let body = '';
        if (url.startsWith('https://esm.sh/yjs@')) body = bundles.get('yjs');
        else if (url.includes('/y-protocols@')) body = bundles.get('awareness');
        else if (url.includes('/y-indexeddb@')) body = bundles.get('indexeddb');
        else if (url.includes('/@trystero-p2p/')) body = trystero;
        else if (url.includes('/@nats-io/')) body = nats;
        else if (url.includes('/mqtt@')) body = mqtt;
        else if (url.includes('/nostr-tools@')) body = nostr;
        await route.fulfill({ contentType: 'application/javascript', body });
      });
      await page.goto(`${base}/index.html#${key}`);
      await page.waitForFunction(() => window.__padTest?.pageOrder.length &&
        [...window.__padTest.meshWrappers.keys()].filter(key => /^(nostr|mqtt|torrent):\d+$/.test(key)).length === 7);
      await page.evaluate(() => window.__padTest.quiet());
      return { page, errors };
    },
  };
}
