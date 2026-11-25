# Pad

Open a hosted room URL such as `https://robit-man.github.io/pad/#SRKPUUNV`. The fragment identifies the room. Each page has a direct link in the form `?page=<id>#<room>`.

## Page tools

Open the room in a WebMCP-capable browser to make these tools available to its agent. Pad also exposes the same tools to browser automation through `window.PadMCP.listTools()` and `window.PadMCP.callTool(name, args)`.

| Tool | Arguments and behavior |
| --- | --- |
| `pad_list_pages` | Lists pages in sidebar order with IDs, labels and links. Includes the active page ID and live sync status. |
| `pad_read_page` | Reads text or Markdown. Omit `pageId` to read the active page. |
| `pad_create_page` | Creates a page with optional `text`. Pass `activate: false` to keep the current page open. |
| `pad_write_page` | Takes `pageId` and `text`. Appends by default. To replace, pass `mode: "replace"` and the exact `expectedText` returned by the last read. |
| `pad_navigate_page` | Takes `pageId`, selects it in the Pad tab and returns its link. |

Append adds text without inserting a separator. Pad normalizes line endings to LF. `expectedText` also works for conditional appends. A write fails if `expectedText` differs from the current text in the Pad tab, so the agent can read it again before retrying.

Pad saves in the browser profile and synchronizes with online room peers. If the room's existing content is only in another browser, keep that browser in the room while connecting the agent. Check the page listing and its sync status before editing a room that appears empty. After a write, leave the bridge running until the receiving peer shows the update.

## Standard MCP clients

Serve the updated `index.html` at the room URL, then install the bridge dependencies from this checkout with Node.js 20 or newer.

```sh
npm ci
npx playwright-core install chromium
```

The bridge opens the configured room in Chromium and keeps a persistent browser profile per hosted room. Set `PAD_MCP_PROFILE` to give a bridge process a dedicated profile, or `CHROMIUM_PATH` to use an existing Chromium executable. Only one Chromium process can use a profile at a time.

### Stdio

Add a server entry to your MCP client. Use the absolute path to this checkout and the hosted room URL.

```json
{
  "mcpServers": {
    "pad": {
      "command": "node",
      "args": ["/absolute/path/to/pad/mcp.mjs", "https://robit-man.github.io/pad/#SRKPUUNV"]
    }
  }
}
```

### Streamable HTTP

```sh
node mcp.mjs --http 7337 'https://robit-man.github.io/pad/#SRKPUUNV'
```

Point the client at `http://127.0.0.1:7337/mcp`. The bridge listens on loopback and scopes every call to the configured room. GitHub Pages serves the HTML and WebMCP tools, while the running bridge serves the standard MCP endpoint.

## Tests

```sh
npm test
```
