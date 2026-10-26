import { readFile, access } from 'node:fs/promises';
import { createServer } from 'node:http';
import { build } from 'esbuild';
import { chromium, firefox } from 'playwright-core';

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
  snapshotRoomState,scheduleRoomSnapshot,flushRoomSnapshotForExit,
  makeNatsOffer,handleNatsPresence,sealJson,startMqttRelay,publishMqttRelay,startNats,startNostrRelay,stopNostrRelay,stopMesh,meshSupervisorTick,prunePeerRoutes,registerPeerRoute,
  sha256Blob,assetChunkFrame,receiveAssetChunk,ensureRelayPeerRoute,routePending,routeKey,mediaMem,mediaUrls,mediaRx,mediaRequestAt,
  undoManagers,meshWrappers,peerRoutes,peerPrimaryRoute,routePeerActors,routeBenchPending,relayActors,
  mqttRelayState,natsState,nostrRelayState,endpointState,endpointReleased,beginEndpoint,endpointFailed,rtcConfig,
  limits:{UNDO_MAX_ITEMS,UNDO_MAX_BYTES,UNDO_MAX_MANAGERS,MEDIA_CACHE_BYTES,MEDIA_CACHE_ITEMS,MQTT_RELAY_CLIENTS,MQTT_MAX_PENDING,ROUTE_STALE_MS,CONNECT_ATTEMPTS_PER_MINUTE,CONNECT_RETRY_MS,RTC_MAX_PEERS,RTC_MAX_ICE_URLS},
  get activePage(){return activePage},get snapshotBusy(){return !!roomCacheWriteInFlight},
  get bootReady(){return !!meshRoomName&&!natsState.starting&&!mqttRelayState.starting&&!nostrRelayState.starting},
  writer(fn){const previous=writeRoomCache;writeRoomCache=fn;return previous},
  quiet(){clearInterval(meshSupervisorTimer);clearInterval(routeProbeTimer)}
};`;
const html=source.replace(/<\/script>\s*<\/body>/,`${expose}</script></body>`);
const mqtt=`
export function connect(url,options){
  const c={url,options,time:Date.now(),connected:false,ended:false,handlers:{},pending:[],
    on(event,fn){(this.handlers[event]??=[]).push(fn);return this},
    emit(event,...args){for(const fn of this.handlers[event]||[])fn(...args)},
    subscribe(topic,opts,cb){this.topic=topic;cb?.()},
    publish(topic,payload,opts,cb){this.pending.push(cb);window.__padTestMqttPublish?.(topic,payload);if(window.__networkOptions.autoAck)queueMicrotask(()=>{this.pending=this.pending.filter(fn=>fn!==cb);cb?.()})},
    end(){this.ended=true;this.connected=false;this.emit('close')}
  };window.__mockMqtt.push(c);queueMicrotask(()=>{if(window.__networkOptions.failAll)c.emit('error',new Error('offline'));else{c.connected=true;c.emit('connect')}});return c
}`;
const nats=`
export async function wsconnect(options){
  window.__mockNatsAttempts.push({time:Date.now(),options});if(window.__networkOptions.failAll)throw new Error('offline');
  let done;const closed=new Promise(r=>done=r);
  const nc={closedFlag:false,publish(){},subscribe(){let finish;return {
    [Symbol.asyncIterator](){return this},next(){return new Promise(r=>finish=r)},unsubscribe(){finish?.({done:true})}
  }},async flush(){},closed(){return closed},async close(){this.closedFlag=true;done()}};window.__mockNats.push(nc);return nc
}`;
const nostr=`
export const generateSecretKey=()=>new Uint8Array(32).fill(1),getPublicKey=()=> 'test-key';
export const finalizeEvent=ev=>({...ev,id:crypto.randomUUID()});
export const verifyEvent=()=>true;`;

export async function startBrowser(){
  const engine=process.env.TEST_BROWSER==='firefox'?firefox:chromium;
  let executablePath=engine===chromium?process.env.CHROMIUM_PATH:undefined;
  if(engine===chromium&&!executablePath){try{await access(chromium.executablePath())}catch{try{await access('/snap/bin/chromium');executablePath='/snap/bin/chromium'}catch{}}}
  const browser=await engine.launch({executablePath,headless:true,args:engine===chromium?['--disable-gpu','--no-sandbox']:[]});
  const server=createServer((_,res)=>{res.writeHead(200,{'Content-Type':'text/html'});res.end(html)});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${server.address().port}`,pages=new Set();
  return{browser,base,
    async close(){await browser.close();await new Promise(resolve=>server.close(resolve))},
    async room(context,key='TESTROOM',options={}){
      const page=await context.newPage(),errors=[];
      page.on('pageerror',error=>errors.push(error.message));pages.add(page);page.on('close',()=>pages.delete(page));
      await page.exposeFunction('__padTestMqttPublish',async(topic,payload)=>{
        if(!options.broker)return;
        await Promise.all([...pages].filter(other=>other!==page&&!other.isClosed()).map(other=>other.evaluate(({topic,payload})=>{for(const c of window.__mockMqtt||[])if(c.connected&&!c.ended&&c.topic===topic)c.emit('message',topic,payload)},{topic,payload}).catch(()=>{})))
      });
      await page.addInitScript(options=>{
        window.__networkOptions=options;window.__mockMqtt=[];window.__mockNats=[];window.__mockNatsAttempts=[];window.__mockSockets=[];
        window.__rtcCreated=0;const NativeRTC=window.RTCPeerConnection;
        window.RTCPeerConnection=class extends NativeRTC{constructor(...args){super(...args);window.__rtcCreated++}};
        if(options.noBroadcast)delete window.BroadcastChannel;
        window.WebSocket=class{
          static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;
          readyState=0;bufferedAmount=0;sent=[];
          constructor(url){this.url=url;this.time=Date.now();window.__mockSockets.push(this);queueMicrotask(()=>{if(options.failAll){this.onerror?.({});this.readyState=3}else{this.readyState=1;this.onopen?.({})}})}
          send(raw){this.sent.push(raw)}close(){this.readyState=3;this.onclose?.({})}
        }
      },options);
      await page.route('https://**/*',async route=>{
        const url=route.request().url();let body='';
        if(url.startsWith('https://esm.sh/yjs@'))body=bundles.get('yjs');
        else if(url.includes('/y-protocols@'))body=bundles.get('awareness');
        else if(url.includes('/y-indexeddb@'))body=bundles.get('indexeddb');
        else if(url.includes('trystero')||url.includes('nkn-sdk'))throw new Error('Speculative discovery SDK must not load');
        else if(url.includes('/@nats-io/'))body=nats;
        else if(url.includes('/mqtt@'))body=mqtt;
        else if(url.includes('/nostr-tools@'))body=nostr;
        await route.fulfill({contentType:'application/javascript',body})
      });
      await page.goto(`${base}/index.html#${key}`);
      await page.waitForFunction(()=>window.__padTest?.pageOrder.length&&window.__padTest.bootReady);
      await page.evaluate(()=>window.__padTest.quiet());return{page,errors}
    }
  }
}
