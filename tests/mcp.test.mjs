import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { after, before, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { startBrowser } from './browser.mjs';

const toolNames = ['pad_list_pages', 'pad_read_page', 'pad_create_page', 'pad_write_page', 'pad_navigate_page'];
const bridge = fileURLToPath(new URL('../mcp.mjs', import.meta.url));
let app;
before(async () => { app = await startBrowser(); });
after(async () => { await app?.close(); });

test('WebMCP tools edit the shared room and open stable page links', async t => {
  const context = await app.browser.newContext();
  const peerContext = await app.browser.newContext();
  t.after(() => Promise.all([context.close(), peerContext.close()]));
  const options = { webMcp: true, broker: true, autoAck: true, noBroadcast: true };
  const { page, errors } = await app.room(context, 'MCPTOOLS', options);
  const { page: peer } = await app.room(peerContext, 'MCPTOOLS', options);
  const call = (name, args) => page.evaluate(async ({ name, args }) => {
    await window.__toolsReady;
    return window.__registeredTools.get(name).execute(args);
  }, { name, args });
  assert.deepEqual(await page.evaluate(() => [...window.__registeredTools.keys()]), toolNames);
  const initial = await call('pad_list_pages');
  assert.equal(initial.room, 'MCPTOOLS');
  assert.equal(initial.activePageId, 'p0');

  const created = await call('pad_create_page', { text: '# Agent page\r\n😀', activate: false });
  assert.equal(created.label, 'Agent page');
  assert.equal(created.text, '# Agent page\n😀');
  assert.equal((await call('pad_read_page')).id, 'p0');
  await peer.waitForFunction(({ id, text }) => window.__padTest.pageText(id).toString() === text, created);

  await assert.rejects(call('pad_write_page', { pageId: created.id, mode: 'replace', text: 'lost' }), /requires expectedText/);
  const replaced = await call('pad_write_page', {
    pageId: created.id, mode: 'replace', text: '# Agent page\n😁', expectedText: created.text,
  });
  assert.equal(replaced.text, '# Agent page\n😁');
  await assert.rejects(call('pad_write_page', {
    pageId: created.id, text: 'stale', expectedText: created.text,
  }), /Page text changed/);

  assert.equal((await call('pad_navigate_page', { pageId: created.id })).url, created.url);
  assert.equal(page.url(), created.url);
  assert.equal(await page.locator('#editor').inputValue(), replaced.text);
  const appended = await call('pad_write_page', { pageId: created.id, text: '\nAppend' });
  assert.equal(await page.locator('#editor').inputValue(), appended.text);
  await peer.waitForFunction(({ id, text }) => window.__padTest.pageText(id).toString() === text, appended);

  await page.locator('#undoBtn').click();
  assert.equal(await page.locator('#editor').inputValue(), replaced.text);
  await page.locator('#redoBtn').click();
  assert.equal(await page.locator('#editor').inputValue(), appended.text);
  await call('pad_navigate_page', { pageId: 'p0' });
  await assert.rejects(call('pad_navigate_page', { pageId: 'missing' }), /Page not found/);

  await page.goto(created.url);
  await page.waitForFunction(() => window.PadMCP?.version === 1);
  assert.equal((await call('pad_read_page')).text, appended.text);
  await page.locator('#shareRoom').click();
  assert.equal(await page.locator('#shareLink').inputValue(), initial.url);
  assert.deepEqual(errors, []);
});

test('standard MCP clients can use stdio and HTTP against the same hosted room and profile', async t => {
  const profile = await mkdtemp(join(tmpdir(), 'pad-mcp-test-'));
  const env = { PAD_MCP_PROFILE: profile };
  if (app.executablePath) env.CHROMIUM_PATH = app.executablePath;
  const url = `${app.base}/offline/index.html#MCPCLI`;
  const stdio = new Client({ name: 'pad-test', version: '1.0.0' });
  let child, exit;
  t.after(async () => {
    await stdio.close();
    if (child && child.exitCode === null && child.signalCode === null) child.kill();
    await exit;
    await rm(profile, { recursive: true, force: true });
  });

  await stdio.connect(new StdioClientTransport({ command: process.execPath, args: [bridge, url], env, stderr: 'pipe' }));
  assert.deepEqual((await stdio.listTools()).tools.map(tool => tool.name), toolNames);
  const created = await stdio.callTool({ name: 'pad_create_page', arguments: { text: '# Persistent page' } });
  assert.equal(created.isError, undefined, created.content[0]?.text);
  await stdio.close();

  child = spawn(process.execPath, [bridge, '--http', '0', url], { env: { ...process.env, ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  exit = once(child, 'exit');
  const stderr = createInterface({ input: child.stderr });
  const [listening] = await Promise.race([
    once(stderr, 'line'),
    exit.then(([code]) => { throw new Error(`Pad MCP server exited with code ${code}`); }),
  ]);
  const endpoint = new URL(listening.match(/http:\/\/127\.0\.0\.1:\d+\/mcp/)?.[0]);
  const http = new Client({ name: 'pad-test', version: '1.0.0' });
  t.after(() => http.close());
  await http.connect(new StreamableHTTPClientTransport(endpoint));
  assert.deepEqual((await http.listTools()).tools.map(tool => tool.name), toolNames);
  const { id, text } = created.structuredContent;
  const result = await http.callTool({ name: 'pad_read_page', arguments: { pageId: id } });
  assert.equal(result.structuredContent.text, text);
  const written = await http.callTool({ name: 'pad_write_page', arguments: { pageId: id, text: '\nFrom HTTP' } });
  assert.equal(written.structuredContent.text, text + '\nFrom HTTP');
  const unknown = await http.callTool({ name: 'pad_read_page', arguments: { pageId: 'missing' } });
  assert.equal(unknown.isError, true);
  assert.match(unknown.content[0].text, /Page not found/);
  assert.equal((await fetch(endpoint, { headers: { Origin: 'https://other.example' } })).status, 403);
});
