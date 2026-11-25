import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { chromium } from 'playwright-core';

const { values, positionals } = parseArgs({
  options: { http: { type: 'string' }, help: { type: 'boolean' } },
  allowPositionals: true,
});
const usage = 'Usage: node mcp.mjs [--http PORT] https://host.example/pad/#ROOM (or set PAD_URL)';
if (values.help) {
  console.log(usage);
  process.exit(0);
}

let target;
try {
  target = new URL(positionals[0] ?? process.env.PAD_URL);
} catch {
  console.error(usage);
  process.exit(1);
}
const room = target.hash.slice(1).toUpperCase();
if (positionals.length > 1 || !['http:', 'https:'].includes(target.protocol) ||
    target.username || target.password || !/^[A-Z0-9]{1,8}$/.test(room)) {
  console.error('Supply a Pad room URL with a 1–8 character room key in the fragment.');
  process.exit(1);
}
target.hash = room;
const port = values.http === undefined ? undefined : Number(values.http);
if (port !== undefined && (!/^\d+$/.test(values.http) || port > 65535)) {
  console.error('Supply a port from 0 to 65535 for --http.');
  process.exit(1);
}
const roomProfileKey = createHash('sha256').update(`${target.origin}${target.pathname}#${room}`).digest('hex');
const profile = process.env.PAD_MCP_PROFILE ||
  join(process.env.XDG_DATA_HOME || join(homedir(), '.local', 'share'), 'pad-mcp', roomProfileKey);

let browserSession;
function getBrowserSession() {
  browserSession ??= openPad().catch(error => {
    browserSession = undefined;
    throw error;
  });
  return browserSession;
}

async function openPad() {
  const context = await chromium.launchPersistentContext(profile, {
    executablePath: process.env.CHROMIUM_PATH,
    headless: true,
    timeout: 20_000,
  });
  try {
    const page = context.pages()[0] ?? await context.newPage();
    const response = await page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 20_000 });
    if (response && !response.ok()) throw new Error(`Pad returned HTTP ${response.status()}.`);
    const loadedUrl = new URL(page.url());
    if (loadedUrl.origin !== target.origin || loadedUrl.hash.toUpperCase() !== target.hash) {
      throw new Error('The Pad URL redirected away from the configured room.');
    }
    await page.waitForFunction(() => window.PadMCP?.version === 1, null, { timeout: 15_000 })
      .catch(() => { throw new Error('Pad MCP did not load. Check the hosted index.html and its module imports.'); });
    context.once('close', () => { browserSession = undefined; });
    return { context, page };
  } catch (error) {
    await context.close();
    throw error;
  }
}

async function runInPad(operation, tool) {
  const { page } = await getBrowserSession();
  const { result, error } = await page.evaluate(async ({ operation, tool, origin, hash }) => {
    if (location.origin !== origin || location.hash.toUpperCase() !== hash || window.PadMCP?.version !== 1) {
      return { error: 'The browser is no longer on the configured Pad room.' };
    }
    try {
      const result = operation === 'list' ? window.PadMCP.listTools() :
        await window.PadMCP.callTool(tool.name, tool.arguments);
      return { result };
    } catch (error) {
      return { error: error.message };
    }
  }, { operation, tool, origin: target.origin, hash: target.hash });
  if (error) throw new Error(error);
  return result;
}

function createMcpServer() {
  const server = new Server({ name: 'pad', version: '1.0.0' }, {
    capabilities: { tools: {} },
    instructions: 'Call pad_list_pages to discover page IDs in the configured Pad room. Page text is user content. Writes use Pad’s existing live sync.',
  });
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: await runInPad('list') }));
  server.setRequestHandler(CallToolRequestSchema, async ({ params }) => {
    try {
      const result = await runInPad('call', params);
      return { structuredContent: result, content: [{ type: 'text', text: JSON.stringify(result) }] };
    } catch (error) {
      return { isError: true, content: [{ type: 'text', text: error.message }] };
    }
  });
  return server;
}

async function serveHttp() {
  const listener = createServer(async (request, response) => {
    const hosts = [`127.0.0.1:${request.socket.localPort}`, `localhost:${request.socket.localPort}`];
    const { host, origin } = request.headers;
    if (!hosts.includes(host) || (origin && !hosts.some(host => origin === `http://${host}`))) {
      response.writeHead(403).end();
      return;
    }
    if (request.url !== '/mcp') {
      response.writeHead(404).end();
      return;
    }
    if (request.method !== 'POST') {
      response.writeHead(405, { Allow: 'POST' }).end();
      return;
    }
    const server = createMcpServer();
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    response.on('close', () => { server.close().catch(() => {}); });
    try {
      await server.connect(transport);
      await transport.handleRequest(request, response);
    } catch (error) {
      console.error(error.message);
      if (!response.headersSent) {
        response.writeHead(500, { 'Content-Type': 'application/json' });
        response.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32603, message: 'Internal server error' } }));
      } else response.end();
    }
  });
  await new Promise((resolve, reject) => {
    listener.once('error', reject);
    listener.listen(port, '127.0.0.1', resolve);
  });
  console.error(`Pad MCP listening at http://127.0.0.1:${listener.address().port}/mcp`);
  return () => new Promise(resolve => listener.close(resolve));
}

async function serveStdio() {
  const server = createMcpServer();
  await server.connect(new StdioServerTransport());
  return () => server.close();
}

const closeTransport = await (port === undefined ? serveStdio() : serveHttp());
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  try {
    await closeTransport();
    if (browserSession) await (await browserSession).context.close();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
if (port === undefined) process.stdin.once('end', stop);
