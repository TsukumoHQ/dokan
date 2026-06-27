#!/usr/bin/env node
/**
 * operator-news-bridge — exposes the operator-news stdio MCP over HTTP on :8791 so a
 * network-isolated dokan container (the #news curator, dokan script 451) can self-fetch the
 * curated feed at http://host.docker.internal:8791/curated/{daily,weekly}?limit=8.
 *
 * WHY: operator-news is a stdio MCP (host binary + host data dir). A dokan job container can't
 * run it or reach a host stdio MCP, so the curated items must arrive over HTTP. "Option A" host
 * fetch (cmo/cto, 2026-06-27). Long-term operator-news grows its own HTTP endpoint, this retires.
 *
 * DESIGN: operator-mcp cold-loads an ONNX embedding model on startup (~tens of seconds). The
 * curator (451) fetches with a 20s timeout, so a per-request cold spawn would time out. We keep
 * ONE long-lived operator-mcp child: the model loads ONCE at bridge boot (plus a warmup call),
 * then every /curated request reuses it and returns fast. The child is respawned if it dies.
 *
 * CONFIG (env from the launchd plist):
 *   OPERATOR_MCP_BIN   absolute path to operator-mcp                 (required)
 *   OPERATOR_DATA_DIR  operator-mcp data dir                         (optional passthrough)
 *   BRIDGE_PORT        listen port (default 8791)
 *   BRIDGE_HOST        bind address (default 0.0.0.0 — non-loopback so host.docker.internal works)
 * The launchd plist also sets WorkingDirectory to the operator-news dir (operator-mcp resolves a
 * RELATIVE onnx/model.onnx against cwd; launchd's default cwd `/` is read-only → must override).
 */
import { createServer } from "node:http";
import { spawn } from "node:child_process";

const BIN = process.env.OPERATOR_MCP_BIN;
const DATA_DIR = process.env.OPERATOR_DATA_DIR;
const PORT = parseInt(process.env.BRIDGE_PORT || "8791", 10);
const HOST = process.env.BRIDGE_HOST || "0.0.0.0";
const REQ_TIMEOUT_MS = 60_000; // per tools/call once the child is warm (warm calls are fast)

if (!BIN) { console.error("OPERATOR_MCP_BIN unset — refusing to start"); process.exit(2); }

// ---- one long-lived operator-mcp child, model loaded once ----
let child = null;
let ready = null;        // Promise that resolves once initialize completes
let nextId = 2;          // 1 is reserved for initialize
const pending = new Map(); // id -> {resolve, reject, timer}
let stdoutBuf = "";

function startChild() {
  child = spawn(BIN, [], {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, ...(DATA_DIR ? { OPERATOR_DATA_DIR: DATA_DIR } : {}) },
  });
  stdoutBuf = "";
  let initResolve, initReject;
  ready = new Promise((res, rej) => { initResolve = res; initReject = rej; });

  child.on("error", (e) => { console.error(`operator-mcp spawn error: ${e.message}`); initReject?.(e); teardown(e); });
  child.stderr.on("data", (d) => process.stderr.write(`[operator-mcp] ${d}`));

  child.stdout.on("data", (chunk) => {
    stdoutBuf += chunk.toString();
    let nl;
    while ((nl = stdoutBuf.indexOf("\n")) >= 0) {
      const line = stdoutBuf.slice(0, nl).trim();
      stdoutBuf = stdoutBuf.slice(nl + 1);
      if (!line) continue;
      let msg; try { msg = JSON.parse(line); } catch { continue; } // skip non-JSON noise
      if (msg.id === 1) {
        send({ jsonrpc: "2.0", method: "notifications/initialized" });
        initResolve?.(); initResolve = initReject = null;
      } else if (pending.has(msg.id)) {
        const p = pending.get(msg.id); pending.delete(msg.id); clearTimeout(p.timer);
        if (msg.error) p.reject(new Error(`tool error: ${JSON.stringify(msg.error)}`));
        else {
          const text = msg.result?.content?.find?.((c) => c.type === "text")?.text;
          if (!text) return p.reject(new Error("no text content in tool result"));
          try { p.resolve(JSON.parse(text)); } catch { p.resolve({ raw: text }); }
        }
      }
    }
  });

  child.on("close", (code, signal) => {
    console.error(`operator-mcp exited (code=${code} signal=${signal}); will respawn on next request`);
    teardown(new Error(`operator-mcp exited (code=${code} signal=${signal})`));
  });

  // begin handshake
  send({ jsonrpc: "2.0", id: 1, method: "initialize",
    params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "operator-news-bridge", version: "2.0" } } });
}

function teardown(err) {
  for (const [, p] of pending) { clearTimeout(p.timer); p.reject(err || new Error("operator-mcp gone")); }
  pending.clear();
  if (child) { try { child.kill("SIGTERM"); } catch {} }
  child = null; ready = null;
}

function send(msg) { try { child?.stdin.write(JSON.stringify(msg) + "\n"); } catch (e) { console.error(`stdin write failed: ${e.message}`); } }

async function ensureReady() {
  if (!child || !ready) startChild();
  await ready;
}

async function mcpCall(toolName, args) {
  await ensureReady();
  const id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`tool ${toolName} timeout after ${REQ_TIMEOUT_MS}ms`)); }, REQ_TIMEOUT_MS);
    pending.set(id, { resolve, reject, timer });
    send({ jsonrpc: "2.0", id, method: "tools/call", params: { name: toolName, arguments: args } });
  });
}

// ---- HTTP ----
const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const json = (code, obj) => { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(obj)); };

  if (url.pathname === "/health") return json(200, { ok: true, bin: BIN, warm: !!child });

  const m = url.pathname.match(/^\/curated\/(daily|weekly)$/);
  if (!m) return json(404, { error: "not found", routes: ["/curated/daily", "/curated/weekly", "/health"] });

  const period = m[1];
  const limit = Math.max(1, Math.min(50, parseInt(url.searchParams.get("limit") || "8", 10) || 8));
  const tool = period === "weekly" ? "get_curated_weekly" : "get_curated_daily";
  try { json(200, await mcpCall(tool, { limit })); }
  catch (e) { console.error(`[${period}] ${e.message}`); json(502, { error: "operator-news fetch failed", detail: e.message }); }
});

server.listen(PORT, HOST, async () => {
  console.log(`operator-news-bridge on http://${HOST}:${PORT} (bin=${BIN})`);
  // Warm the child + model at boot so the first real request is fast.
  try { await mcpCall("get_curated_daily", { limit: 1 }); console.log("warmup ok — operator-mcp loaded"); }
  catch (e) { console.error(`warmup failed (will retry on first request): ${e.message}`); }
});
