// agentic-loop: visualize OpenCode's agentic loop live in a local web app.
//
// Published OpenCode V2 Promise plugin.
//
// Fidelity:
//   - Core loop nodes are driven by REAL session HOOKS:
//       prompt        -> INPUT      (prompt admission)
//       context       -> CONTEXT    (assembled model request)
//       http.request  -> LLM        (primary agent-loop request)
//       http.response -> LLM        (primary agent-loop response)
//   - The highlight is a per-session loop-state reducer, not a direct
//     signal-to-node mapping. Tool/permission breakpoints use awaited hooks;
//     the public event stream only supplies observational state transitions.
//
// Only loop-relevant signals are forwarded to the UI; streaming deltas
// (session.text.delta, part updates, heartbeats, etc.) are filtered out.

import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { Plugin } from "@opencode/plugin";

const PORT = Number(process.env.AGENTIC_LOOP_PORT || 45999);
const DIAGRAM_FILE = "agentic-loop-lifecycle.mmd";
const PACKAGE_DIR = path.dirname(fileURLToPath(import.meta.url));

// Fallback diagram so the page renders even if the .mmd cannot be read.
const DEFAULT_DIAGRAM = [
  "flowchart TD",
  "  START([Session starts]) --> WAIT",
  "  subgraph OUTER[OUTER LOOP]",
  "    WAIT[Wait for prompt<br/><small>session.idle</small>]",
  "    WAIT --> INPUT[Handle input<br/><small>prompt hook</small>]",
  "    INPUT --> CONTEXT[Build initial context<br/><small>context hook</small>]",
  "    subgraph INNER[INNER LOOP]",
  "      CONTEXT --> TASK[Start task<br/><small>session.status busy</small>]",
  "      TASK -->|Prompt skill attached| PROMPT_SKILL[Load Agent Skill<br/><small>Prompt-provided · prompt.skills</small>]",
  "      PROMPT_SKILL -->|http.request| LLM{LLM Core<br/><small>http.request</small>}",
  "      TASK -->|No prompt skill<br/>HTTP request: messages + tools| LLM",
  "      LLM -->|read skill| SKILL[Load Agent Skill<br/><small>session.tool.input skill</small>]",
  "      SKILL --> LLM",
  "      LLM -->|tool call| TOOL[Choose tool<br/><small>session.tool.input.started</small>]",
  "      TOOL --> ACTION[Use tool<br/><small>session.tool.called</small>]",
  "      ACTION --> CHECK[Result<br/><small>session.step.streamed</small>]",
  "      CHECK --> LLM",
  "    end",
  "    LLM -->|final answer| RESPONSE[Return response<br/><small>message</small>]",
  "    RESPONSE --> REVIEW{Human review<br/><small>permission / question</small>}",
  "    REVIEW -->|Refine| INPUT",
  "    REVIEW -->|Accept| WAIT",
  "  end",
  "  linkStyle default stroke:#111111,stroke-width:2px",
  "  linkStyle 0 stroke:#c026d3,stroke-width:2.5px",
].join("\n");

let server = null;
let serverStarting = null;
let serverClosing = Promise.resolve();
let heartbeatTimer = null;
let webAppEnabled = false;
let diagram = DEFAULT_DIAGRAM;
let pageTemplate = "";
let mermaidBundle = "";
const sseClients = new Set();
let openedBrowser = false;
let debuggerMode = "off";
let activePause = null;
let pauseTail = Promise.resolve();
let pauseSequence = 0;

// Per-session loop state. Signals are reduced into a coherent flow instead of
// letting whichever event arrived last arbitrarily choose the highlighted node.
const loopStates = new Map();
let focusedSessionID;

// Debug: capture raw event stream truth to /tmp (only when AGENTIC_LOOP_DEBUG=1).
const DEBUG_FILE = "/tmp/agentic-loop-debug.json";
const DEBUG_ON = process.env.AGENTIC_LOOP_DEBUG === "1";
const DEBUG = { started: false, count: 0, error: null, types: {}, recent: [] };
function dumpDebug() {
  if (!DEBUG_ON) return;
  try {
    fs.writeFile(DEBUG_FILE, JSON.stringify(DEBUG, null, 2)).catch(() => {});
  } catch {
    /* ignore */
  }
}

function stateFor(sessionID) {
  const id = sessionID || "__unknown__";
  if (!loopStates.has(id)) {
    loopStates.set(id, {
      firstContext: true,
      stepUsedTool: false,
      promptSkills: [],
      pendingSkill: undefined,
      expectSkillActivation: false,
      skillsHandled: new Set(),
      currentNode: undefined,
      currentLabel: undefined,
    });
  }
  return loopStates.get(id);
}

function toolNode(tool) {
  const name = String(tool || "");
  return /skill/i.test(name)
    ? { node: "SKILL", label: "Load Agent Skill · " + name }
    : { node: "TOOL", label: "Choose tool · " + (name || "?") };
}

function broadcast(payload) {
  const frame = "data: " + JSON.stringify(payload) + "\n\n";
  for (const res of sseClients) {
    try {
      res.write(frame);
    } catch {
      sseClients.delete(res);
    }
  }
}

function setCurrentNode(sessionID, node, label, type, source, forceFocus = false, toolName) {
  if (forceFocus && sessionID) focusedSessionID = sessionID;
  if (!focusedSessionID && sessionID) focusedSessionID = sessionID;
  if (sessionID && focusedSessionID && sessionID !== focusedSessionID) return;
  const state = stateFor(sessionID);
  state.currentNode = node;
  state.currentLabel = label;
  broadcast({ kind: "signal", type, node, label, ts: Date.now(), source, sessionID, toolName });
}

function observeEvent(type, data) {
  const sessionID = data?.sessionID;
  if (type === "session.created") {
    if (!focusedSessionID || focusedSessionID === sessionID) {
      if (sessionID) focusedSessionID = sessionID;
      setCurrentNode(sessionID, "START", "Session starts", type, "event");
    }
    return;
  }
  if (!sessionID || (focusedSessionID && sessionID !== focusedSessionID)) return;
  const state = stateFor(sessionID);
  switch (type) {
    case "session.idle":
      setCurrentNode(sessionID, "WAIT", "Wait for prompt · idle", type, "event");
      return;
    case "session.status": {
      const status = String(
        (typeof data.status === "string" ? data.status : data.status?.type) || ""
      ).toLowerCase();
      if (status === "idle") {
        setCurrentNode(sessionID, "WAIT", "Wait for prompt · idle", type, "event");
      }
      return;
    }
    case "session.tool.input.started": {
      // Remember that this model step selected a tool, but do not move the
      // highlight yet. The awaited execute.before hook is the real breakpoint.
      state.stepUsedTool = true;
      return;
    }
    case "session.skill.activated": {
      const skill = { id: data.id, name: data.name };
      if (state.skillsHandled.has(String(skill.id || "")) || state.skillsHandled.has(String(skill.name || "")))
        return;
      if (state.expectSkillActivation) {
        // A skill-named tool already has an awaited execute.before breakpoint.
        state.expectSkillActivation = false;
      } else {
        state.pendingSkill = skill;
      }
      setCurrentNode(
        sessionID,
        "SKILL",
        "Load Agent Skill · " + (skill.name || skill.id || "activated"),
        type,
        "event",
      );
      return;
    }
    case "session.tool.progress":
      setCurrentNode(sessionID, "ACTION", "Use tool · running", type, "event");
      return;
    case "session.text.ended":
      if (!state.stepUsedTool) {
        setCurrentNode(sessionID, "RESPONSE", "Return response · final text", type, "event");
      }
      return;
    case "session.step.streamed":
      if (state.stepUsedTool && state.currentNode === "ACTION") {
        setCurrentNode(sessionID, "CHECK", "Result · step streamed", type, "event");
      } else if (!state.stepUsedTool && state.currentNode === "RESPONSE") {
        // The final text part has ended and its assistant step is complete.
        // Do not wait for a separate idle-status event to settle on WAIT.
        setCurrentNode(sessionID, "WAIT", "Wait for prompt · response complete", type, "event");
      }
      return;
    default:
      // Deliberately ignore unrelated events. They must not steal the highlight.
      return;
  }
}

function setDebuggerMode(mode) {
  debuggerMode = mode;
  if (mode !== "step" && activePause) activePause.release();
  broadcast({ kind: "debugger", state: mode, ts: Date.now() });
}

async function enableDebugger() {
  const started = await startServer();
  if (!started) return false;
  webAppEnabled = true;
  setDebuggerMode("step");
  if (!openedBrowser) openBrowser(`http://127.0.0.1:${PORT}`);
  return true;
}

async function disableDebugger() {
  setDebuggerMode("off");
  await stopServer();
}

// This is the actual debugger stop: awaited from an OpenCode hook, so that
// operation cannot proceed until Next/Continue (or debugger off) releases it.
async function breakpoint(type, node, label, sessionID, force = false) {
  const previousNode = stateFor(sessionID).currentNode;
  setCurrentNode(sessionID, node, label, type, "hook", type === "prompt");
  if (debuggerMode !== "step") return;
  // Several hooks/events can describe the same diagram node (e.g. context,
  // request, and response all belong to LLM). Step once per node transition,
  // not once per low-level signal; a later return to that node after CHECK is
  // still a new transition because the current node has changed in between.
  if (previousNode === node && !force) return;

  // Serialize breakpoints across sessions: one visible stop at a time.
  const previous = pauseTail;
  let finishTurn;
  const turn = new Promise((resolve) => { finishTurn = resolve; });
  pauseTail = previous.then(() => turn);
  await previous;

  if (debuggerMode !== "step") {
    finishTurn();
    return;
  }

  let release;
  const waiting = new Promise((resolve) => { release = resolve; });
  const id = ++pauseSequence;
  activePause = { id, sessionID, type, node, label, release };
  broadcast({ kind: "debugger", state: "paused", id, sessionID, type, node, label, ts: Date.now() });
  await waiting;
  if (activePause?.id === id) activePause = null;
  broadcast({ kind: "debugger", state: "resumed", ts: Date.now() });
  finishTurn();
}

async function debuggerControl(action) {
  if (action === "next") {
    if (debuggerMode === "step" && activePause) activePause.release();
    return { ok: true, mode: debuggerMode, released: Boolean(activePause) };
  }
  if (action === "continue") {
    setDebuggerMode("continue");
    return { ok: true, mode: debuggerMode };
  }
  if (action === "pause") {
    setDebuggerMode("step");
    return { ok: true, mode: debuggerMode };
  }
  if (action === "off") {
    await disableDebugger();
    return { ok: true, mode: debuggerMode };
  }
  if (action === "on") {
    const started = await enableDebugger();
    return { ok: started, mode: debuggerMode, error: started ? undefined : "Could not start debugger web app" };
  }
  return { ok: false, error: "Unknown debugger action" };
}

async function loadDiagram(startDir) {
  const candidates = [
    startDir && path.join(startDir, DIAGRAM_FILE),
    process.cwd() && path.join(process.cwd(), DIAGRAM_FILE),
    path.join(PACKAGE_DIR, DIAGRAM_FILE),
  ].filter(Boolean);
  for (const file of candidates) {
    try {
      const text = await fs.readFile(file, "utf8");
      if (text.trim().length > 0) return text;
    } catch {
      /* try next */
    }
  }
  return DEFAULT_DIAGRAM;
}

function openBrowser(url) {
  if (openedBrowser || process.platform !== "darwin") {
    openedBrowser = true;
    return;
  }
  openedBrowser = true;
  execFile("open", [url], () => {});
}

function buildPage(diagramText) {
  const mermaidSrc =
    process.env.AGENTIC_LOOP_MERMAID ||
    "/vendor/mermaid-11.17.2.min.js";
  return pageTemplate
    .replace("__MERMAID_SRC__", mermaidSrc)
    .replace("__DIAGRAM_JSON__", JSON.stringify(diagramText));
}

function startServer() {
  if (server?.listening) return Promise.resolve(true);
  if (serverStarting) return serverStarting;
  serverStarting = startServerOnce().finally(() => { serverStarting = null; });
  return serverStarting;
}

async function startServerOnce() {
  await serverClosing;
  if (server?.listening) return true;
  return new Promise((resolve) => {
    const instance = http.createServer((req, res) => {
      const url = (req.url || "/").split("?")[0];
      if (url === "/vendor/mermaid-11.17.2.min.js") {
        res.writeHead(200, { "Content-Type": "application/javascript; charset=utf-8", "Cache-Control": "public, max-age=3600" });
        res.end(mermaidBundle);
        return;
      }
      if (url === "/control" && req.method === "POST") {
        const origin = req.headers.origin;
        if (origin && origin !== `http://${req.headers.host}`) {
          res.writeHead(403);
          res.end("Forbidden");
          return;
        }
        let body = "";
        req.on("data", (chunk) => {
          body += chunk;
          if (body.length > 2048) req.destroy();
        });
        req.on("end", async () => {
          try {
            const input = JSON.parse(body || "{}");
            if (input.action === "off") {
              // Reply before closing the server; otherwise close() would wait
              // for this very control request to finish.
              setDebuggerMode("off");
              res.writeHead(200, { "Content-Type": "application/json" });
              res.end(JSON.stringify({ ok: true, mode: "off" }));
              void stopServer();
              return;
            }
            const result = await debuggerControl(input.action);
            res.writeHead(result.ok ? 200 : 400, { "Content-Type": "application/json" });
            res.end(JSON.stringify(result));
          } catch {
            res.writeHead(400, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ ok: false, error: "Invalid JSON" }));
          }
        });
        return;
      }
      if (url === "/events") {
        res.writeHead(200, {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
        });
        res.write("retry: 2000\n\n");
        sseClients.add(res);
        const state = focusedSessionID ? stateFor(focusedSessionID) : undefined;
        res.write("data: " + JSON.stringify({
          kind: "snapshot",
          mode: debuggerMode,
          sessionID: focusedSessionID,
          node: activePause?.node || state?.currentNode,
          label: activePause?.label || state?.currentLabel,
          pause: activePause ? { id: activePause.id, type: activePause.type, node: activePause.node, label: activePause.label } : undefined,
          ts: Date.now(),
        }) + "\n\n");
        req.on("close", () => sseClients.delete(res));
        return;
      }
      if (url === "/diagram") {
        res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
        res.end(diagram);
        return;
      }
      if (url === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, clients: sseClients.size }));
        return;
      }
      if (url === "/debug") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(DEBUG, null, 2));
        return;
      }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(buildPage(diagram));
    });
    server = instance;
    let settled = false;
    instance.on("error", (err) => {
      console.error("[agentic-loop] server error:", err?.message || err);
      webAppEnabled = false;
      if (server === instance) server = null;
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      heartbeatTimer = null;
      if (!settled) {
        settled = true;
        resolve(false);
      }
    });
    instance.listen(PORT, "127.0.0.1", () => {
      webAppEnabled = true;
      heartbeatTimer = setInterval(() => broadcast({ heartbeat: true, ts: Date.now() }), 15000);
      console.log(`[agentic-loop] web app at http://127.0.0.1:${PORT}`);
      if (!settled) {
        settled = true;
        resolve(true);
      }
    });
  });
}

async function stopServer() {
  webAppEnabled = false;
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  if (serverStarting) await serverStarting.catch(() => false);
  for (const res of sseClients) {
    try {
      res.end();
    } catch {
      /* ignore */
    }
  }
  sseClients.clear();
  const current = server;
  server = null;
  if (!current?.listening) return;
  serverClosing = new Promise((resolve) => current.close(() => resolve()));
  await serverClosing;
}

export default Plugin.define({
  id: "agentic-loop",
  async setup(ctx) {
    const dir = ctx?.location?.directory || process.cwd();
    diagram = await loadDiagram(dir);
    pageTemplate = await fs.readFile(path.join(PACKAGE_DIR, "web", "index.html"), "utf8");
    mermaidBundle = await fs.readFile(path.join(PACKAGE_DIR, "web", "vendor", "mermaid-11.17.2.min.js"), "utf8");
    const url = `http://127.0.0.1:${PORT}`;

    // --- Awaited hooks are genuine breakpoints; event-stream messages are not. ---
    await ctx.session.hook("prompt", async (e) => {
      const state = stateFor(e?.sessionID);
      if (state.currentNode === "WAIT") {
        // Idle itself is an event-only state, so the runtime is already
        // naturally waiting. Stop at the next awaited boundary before the
        // arriving prompt is admitted, then step into Handle input.
        await breakpoint("prompt", "WAIT", "Wait for prompt · prompt arrived", e?.sessionID, true);
      }
      state.firstContext = true;
      state.stepUsedTool = false;
      state.pendingSkill = undefined;
      state.expectSkillActivation = false;
      state.skillsHandled = new Set();
      await breakpoint("prompt", "INPUT", "Handle input · prompt hook", e?.sessionID);
      const skills = Array.isArray(e?.prompt?.skills) ? e.prompt.skills : [];
      state.promptSkills = [];
      for (const skill of skills) {
        const id = typeof skill === "string" ? skill : skill?.id;
        const name = typeof skill === "string" ? skill : skill?.name || skill?.id;
        if (id) state.skillsHandled.add(String(id));
        if (name) state.skillsHandled.add(String(name));
        state.promptSkills.push({ id, name });
      }
    });
    await ctx.session.hook("context", async (e) => {
      const state = stateFor(e?.sessionID);
      const initial = state.firstContext;
      const skill = state.pendingSkill;
      const promptSkills = state.promptSkills;
      state.pendingSkill = undefined;
      state.promptSkills = [];
      state.firstContext = false;
      if (initial) {
        await breakpoint("context", "CONTEXT", "Build initial context · first context hook this turn", e?.sessionID);
        await breakpoint("context", "TASK", "Start task · initial context ready", e?.sessionID);
        if (promptSkills.length) {
          const names = promptSkills.map((item) => item.name || item.id || "skill").join(", ");
          await breakpoint(
            "prompt.skill",
            "PROMPT_SKILL",
            "Load Agent Skill · prompt-provided: " + names,
            e?.sessionID,
          );
        }
      }
      if (skill) {
        await breakpoint(
          "session.skill.activated",
          "SKILL",
          "Load Agent Skill · " + (skill.name || skill.id || "activated"),
          e?.sessionID,
          true,
        );
        return;
      }
      if (!initial)
        await breakpoint("context", "LLM", "LLM Core · continuation context hook", e?.sessionID);
    });
    await ctx.session.hook("http.request", async (e) => {
      if (e?.kind === "primary")
        await breakpoint("http.request", "LLM", "LLM Core · http.request", e?.sessionID);
    });
    await ctx.session.hook("http.response", async (e) => {
      if (e?.kind === "primary")
        await breakpoint("http.response", "LLM", "LLM Core · http.response", e?.sessionID);
    });
    await ctx.tool.hook("execute.before", async (e) => {
      const state = stateFor(e?.sessionID);
      state.stepUsedTool = true;
      state.expectSkillActivation = /skill/i.test(String(e?.tool || ""));
      const mapped = toolNode(e?.tool);
      await breakpoint("tool.execute.before", mapped.node, mapped.label, e?.sessionID);
      // The breakpoint has resumed; the tool is about to execute.
      setCurrentNode(e?.sessionID, "ACTION", "Use tool · executing", "tool.execution", "hook", false, String(e?.tool || "unknown"));
    });
    await ctx.tool.hook("execute.after", async (e) => {
      await breakpoint("tool.execute.after", "CHECK", "Result · tool.execute.after", e?.sessionID);
    });
    await ctx.permission.hook("evaluate", async (e) => {
      if (e?.effect === "ask")
        await breakpoint("permission.evaluate", "REVIEW", "Human review · permission.evaluate", e?.sessionID);
    });

    // Slash command controls debugger mode without injecting a message into
    // the session transcript. No argument toggles; explicit on/off are accepted.
    await ctx.command.transform((editor) => {
      editor.add({
        name: "agentic-loop-debugger",
        description: "Turn the agentic loop debugger on/off (on, off, toggle)",
        execute: async ({ prompt }) => {
           const args = String(prompt?.text || "").trim().toLowerCase().split(/\s+/);
           const arg = args.find((part) => part === "on" || part === "off" || part === "toggle");
           const action = arg === "on" || arg === "off" ? arg : "toggle";
          const turnOn = action === "toggle" ? debuggerMode === "off" : action === "on";
          if (turnOn) {
            const started = await enableDebugger();
            if (!started) throw new Error(`Could not start debugger web app on port ${PORT}`);
            broadcast({ kind: "debugger", state: "step", message: "Debugger on · step mode", ts: Date.now() });
          } else {
            await disableDebugger();
          }
        },
      });
    });

    // --- Real event stream: lifecycle, tools, permissions ---
    const controller = new AbortController();
    DEBUG.started = true;
    (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          const type = event?.type;
          if (DEBUG_ON) {
            DEBUG.count++;
            DEBUG.types[type || "?"] = (DEBUG.types[type || "?"] || 0) + 1;
            DEBUG.recent.push({
              type: type || "?",
              keys: Object.keys(event || {}),
              propsKeys: Object.keys(event?.data || event?.properties || {}),
            });
            if (DEBUG.recent.length > 60) DEBUG.recent.shift();
            dumpDebug();
          }
          if (!type) continue;
          const data = event?.data || event?.properties || event?.payload || {};
          if (type === "session.created" && webAppEnabled) openBrowser(url);
          if (type === "session.step.started") {
            const state = stateFor(data.sessionID);
            state.stepUsedTool = false;
          }
          observeEvent(type, data);
        }
      } catch (err) {
        DEBUG.error = String(err?.message || err);
        dumpDebug();
        console.error("[agentic-loop] event subscribe error:", err?.message || err);
      }
    })();

    // Cleanup on plugin unload: stop the debugger and event stream.
    return async () => {
      setDebuggerMode("off");
      await stopServer();
      controller.abort();
    };
  },
});
