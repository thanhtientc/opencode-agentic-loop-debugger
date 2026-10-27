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
  // Client uses string concatenation (no template literals) to avoid ${} clashes.
  const html = String.raw`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>OpenCode Agentic Loop</title>
<script src="__MERMAID_SRC__"></script>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.4 -apple-system, system-ui, sans-serif; background:#0b1020; color:#e6e9f2; }
  header { padding:10px 16px; display:flex; align-items:center; gap:12px; border-bottom:1px solid #232a45; }
  header b { font-size:15px; }
  .dot { width:10px; height:10px; border-radius:50%; background:#64748b; }
  .dot.live { background:#22c55e; box-shadow:0 0 8px #22c55e; }
  .layout { display:grid; grid-template-columns: 1fr 340px; height: calc(100vh - 53px); }
  #diagram { overflow:auto; padding:16px; }
  #diagram .cluster-label, #diagram .cluster-label span,
  #diagram .cluster-label p, #diagram .cluster-label text {
    color:#000 !important; fill:#000 !important;
  }
  #log { border-left:1px solid #232a45; overflow:auto; padding:8px 10px; background:#0d1428; }
  #log h3 { margin:6px 2px; font-size:12px; text-transform:uppercase; letter-spacing:.05em; color:#94a3b8; }
  .row { padding:4px 6px; border-radius:6px; font-family:ui-monospace, SFMono-Regular, Menlo, monospace; font-size:11.5px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .row .t { color:#64748b; margin-right:6px; }
  .row .n { color:#38bdf8; }
  .row .ty { color:#a78bfa; }
  .row.hook .ty { color:#f472b6; }
  .controls { display:flex; align-items:center; gap:8px; margin-left:auto; }
  .controls button { border:1px solid #475569; background:#172033; color:#e6e9f2; border-radius:6px; padding:6px 12px; cursor:pointer; }
  .controls button.primary { background:#2563eb; border-color:#3b82f6; }
  .controls button:disabled { opacity:.4; cursor:not-allowed; }
  #debug-state { color:#fbbf24; font-size:12px; min-width:110px; text-align:right; }
  /* Robust highlight: a bold red box drawn AROUND the active node (the node
     itself stays solid; only the surrounding box pulses). */
  .hl {
    fill:none; stroke:#ef4444; stroke-width:3.5px; rx:6;
    vector-effect:non-scaling-stroke;
    filter: drop-shadow(0 0 5px rgba(239,68,68,.85));
    animation: hl 1.1s ease-in-out infinite;
  }
  .route-hl {
    fill:none; stroke:#ef4444; stroke-width:3px; rx:6;
    vector-effect:non-scaling-stroke;
    filter: drop-shadow(0 0 4px rgba(239,68,68,.7));
  }
  #diagram .edgePaths path.route-edge {
    stroke:#ef4444 !important; stroke-width:4px !important;
    filter: drop-shadow(0 0 3px rgba(239,68,68,.8));
  }
  @keyframes hl { 0%,100%{opacity:1} 50%{opacity:.55} }
  footer { color:#64748b; font-size:11px; padding:4px 16px; }
</style>
</head>
<body>
<header>
  <span id="dot" class="dot"></span>
  <b>OpenCode Agentic Loop</b>
  <span id="now" style="color:#94a3b8"></span>
  <div class="controls">
    <span id="debug-state">STEP MODE</span>
    <button id="next" class="primary" disabled>Next</button>
    <button id="continue">Continue</button>
    <button id="pause" disabled>Pause</button>
  </div>
</header>
<div class="layout">
  <div id="diagram"><pre id="src">Loading diagram…</pre></div>
  <div id="log"><h3>Live loop signals</h3><div id="rows"></div></div>
</div>
<footer>Awaited hooks stop OpenCode · final RESPONSE is a visual breakpoint after the response completes</footer>
<script>
  var DIAGRAM = __DIAGRAM_JSON__;
  var HOOKS = { "prompt":1, "prompt.skill":1, "context":1, "http.request":1, "http.response":1,
    "tool.execute.before":1, "tool.execute.after":1, "permission.evaluate":1 };
  // Logical node id -> substrings found in the node's rendered label text.
  // Matching by text is robust to Mermaid's internal DOM id scheme.
  var LABELS = [
    ["START", ["session starts"]],
    ["WAIT", ["wait for prompt"]],
    ["INPUT", ["handle input"]],
    ["CONTEXT", ["build initial context"]],
    ["TASK", ["start task"]],
    ["LLM", ["llm core"]],
    ["PROMPT_SKILL", ["prompt-provided"]],
    ["SKILL", ["load agent skill"]],
    ["TOOL", ["choose tool"]],
    ["ACTION", ["use tool"]],
    ["CHECK", ["result"]],
    ["RESPONSE", ["return response"]],
    ["REVIEW", ["human review"]]
  ];
  var EDGES = [
    ["START", "WAIT"], ["WAIT", "INPUT"], ["INPUT", "CONTEXT"],
    ["CONTEXT", "TASK"], ["TASK", "PROMPT_SKILL"], ["PROMPT_SKILL", "LLM"],
    ["TASK", "LLM"], ["LLM", "SKILL"], ["SKILL", "LLM"],
    ["LLM", "TOOL"], ["TOOL", "ACTION"], ["ACTION", "CHECK"],
    ["CHECK", "LLM"], ["LLM", "RESPONSE"], ["RESPONSE", "REVIEW"],
    ["REVIEW", "INPUT"], ["REVIEW", "WAIT"]
  ];
  mermaid.initialize({ startOnLoad:false, theme:"dark", flowchart:{ htmlLabels:true, curve:"basis" } });
  var nodeMap = {};
  var lastPulse = null;
  var svgEl = null;
  var currentHl = null;
  var routeEdges = [];
  var routeBoxes = [];
  var routeArrowId = "agentic-loop-route-arrow";
  var paused = false;
  var mode = "off";
  var visualPaused = false;
  var queuedMessages = [];

  mermaid.render("g1", DIAGRAM).then(function (r) {
    document.getElementById("diagram").innerHTML = r.svg;
    svgEl = document.querySelector("#diagram svg");
    // Build a map from logical id -> node <g> by reading each node's text.
    if (svgEl) {
      svgEl.querySelectorAll("g.node").forEach(function (g) {
        var text = (g.textContent || "").toLowerCase();
        for (var i = 0; i < LABELS.length; i++) {
          var subs = LABELS[i][1];
          for (var j = 0; j < subs.length; j++) {
            if (text.indexOf(subs[j]) !== -1) { nodeMap[LABELS[i][0]] = g; return; }
          }
        }
      });
    }
    if (lastPulse) applyPulse(lastPulse.id, lastPulse.label, true);
  }).catch(function (e) {
    document.getElementById("diagram").innerHTML = "<pre>"+String(e)+"</pre>";
  });

  // Map a node's on-screen box into the SVG's user coordinate system.
  // Robust to Mermaid's nested node transforms and any scaling/zoom.
  function svgBox(el) {
    if (!el || !svgEl.getScreenCTM) return null;
    var ctm = svgEl.getScreenCTM();
    if (!ctm) return null;
    var r = el.getBoundingClientRect();
    if (!r.width && !r.height) return null;
    var inv = ctm.inverse();
    var p = svgEl.createSVGPoint();
    p.x = r.left; p.y = r.top; var tl = p.matrixTransform(inv);
    p.x = r.right; p.y = r.bottom; var br = p.matrixTransform(inv);
    return { x: tl.x, y: tl.y, w: br.x - tl.x, h: br.y - tl.y };
  }

  function findRoute(from, to) {
    if (!from || !to || from === to) return null;
    var queue = [{ node:from, nodes:[from], edges:[] }];
    var visited = new Set([from]);
    while (queue.length) {
      var current = queue.shift();
      for (var i = 0; i < EDGES.length; i++) {
        var edge = EDGES[i];
        if (edge[0] !== current.node || visited.has(edge[1])) continue;
        var nodes = current.nodes.concat([edge[1]]);
        var edges = current.edges.concat([edge]);
        if (edge[1] === to) return { nodes:nodes, edges:edges };
        visited.add(edge[1]);
        queue.push({ node:edge[1], nodes:nodes, edges:edges });
      }
    }
    return null;
  }

  function edgeElement(from, to) {
    if (!svgEl) return null;
    return svgEl.querySelector('.edgePaths [data-id="L_'+from+'_'+to+'_0"]');
  }

  function clearRoute() {
    routeEdges.forEach(function (item) {
      item.edge.classList.remove("route-edge");
      if (item.marker) item.edge.setAttribute("marker-end", item.marker);
      else item.edge.removeAttribute("marker-end");
    });
    routeEdges = [];
    routeBoxes.forEach(function (box) { box.remove(); });
    routeBoxes = [];
  }

  function addRouteBox(node) {
    var b = svgBox(node);
    if (!b) return;
    var pad = 6;
    var rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("class", "route-hl");
    rect.setAttribute("x", b.x - pad);
    rect.setAttribute("y", b.y - pad);
    rect.setAttribute("width", b.w + pad * 2);
    rect.setAttribute("height", b.h + pad * 2);
    rect.setAttribute("rx", 6);
    svgEl.appendChild(rect);
    routeBoxes.push(rect);
  }

  function ensureRouteArrow() {
    if (svgEl.querySelector("#" + routeArrowId)) return;
    var defs = svgEl.querySelector("defs");
    if (!defs) {
      defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
      svgEl.insertBefore(defs, svgEl.firstChild);
    }
    var marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
    marker.setAttribute("id", routeArrowId);
    marker.setAttribute("viewBox", "0 0 10 10");
    marker.setAttribute("refX", "5");
    marker.setAttribute("refY", "5");
    marker.setAttribute("markerUnits", "userSpaceOnUse");
    marker.setAttribute("markerWidth", "8");
    marker.setAttribute("markerHeight", "8");
    marker.setAttribute("orient", "auto");
    var arrow = document.createElementNS("http://www.w3.org/2000/svg", "path");
    arrow.setAttribute("d", "M 0 0 L 10 5 L 0 10 z");
    arrow.setAttribute("fill", "#ef4444");
    arrow.setAttribute("stroke", "#ef4444");
    marker.appendChild(arrow);
    defs.appendChild(marker);
  }

  function highlightRoute(from, to) {
    var route = findRoute(from, to);
    if (!route) return;
    ensureRouteArrow();
    route.edges.forEach(function (pair) {
      var edge = edgeElement(pair[0], pair[1]);
      if (!edge) return;
      routeEdges.push({ edge:edge, marker:edge.getAttribute("marker-end") });
      edge.classList.add("route-edge");
      edge.setAttribute("marker-end", "url(#" + routeArrowId + ")");
    });
    // Outline intermediate nodes only; leave the previous/source node alone.
    // The destination receives the animated active-node outline below.
    route.nodes.slice(1, -1).forEach(function (nodeID) {
      if (nodeMap[nodeID]) addRouteBox(nodeMap[nodeID]);
    });
  }

  function applyPulse(id, label, scroll, previous) {
    if (previous !== id) {
      clearRoute();
      if (previous) highlightRoute(previous, id);
    }
    Object.keys(nodeMap).forEach(function (k) { nodeMap[k].classList.remove("pulse"); });
    if (currentHl) { currentHl.remove(); currentHl = null; }
    var n = nodeMap[id];
    if (n) {
      n.classList.add("pulse");
      // Draw a bold red rectangle AROUND the node, in SVG root coordinates.
      var b = svgBox(n);
      if (b) {
        var pad = 6;
        var r = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        r.setAttribute("class", "hl");
        r.setAttribute("x", b.x - pad);
        r.setAttribute("y", b.y - pad);
        r.setAttribute("width", b.w + pad * 2);
        r.setAttribute("height", b.h + pad * 2);
        r.setAttribute("rx", 6);
        svgEl.appendChild(r); // append to root so it sits on top, unclipped
        currentHl = r;
      }
      if (scroll) n.scrollIntoView({ block:"nearest", inline:"nearest" });
    }
    document.getElementById("now").textContent = label ? ("→ " + label) : "";
  }
  function pulse(id, label) {
    var previous = lastPulse && lastPulse.id;
    lastPulse = { id: id, label: label };
    if (svgEl) applyPulse(id, label, true, previous);
  }
  function log(ts, type, node, isHook, toolName) {
    var rows = document.getElementById("rows");
    var d = document.createElement("div");
    d.className = "row" + (isHook ? " hook" : "");
    var time = new Date(ts).toLocaleTimeString();
    var tool = type === "tool.execution" && toolName
      ? " · " + String(toolName).replace(/[&<>\"]/g, function (c) {
          return { "&":"&amp;", "<":"&lt;", ">":"&gt;", "\"":"&quot;" }[c];
        })
      : "";
    d.innerHTML = '<span class="t">'+time+'</span>'
      + '<span class="n">'+node+'</span> '
      + '<span class="ty">'+type+tool+(isHook?' ⚡hook':'')+'</span>';
    rows.insertBefore(d, rows.firstChild);
    while (rows.childElementCount > 400) rows.removeChild(rows.lastChild);
  }

  function setDebuggerState(state, message) {
    paused = state === "paused";
    mode = state === "running" ? "continue" :
      (state === "step" || paused ? "step" : state === "off" ? "off" : mode);
    document.getElementById("debug-state").textContent = message ||
      (paused ? "PAUSED" : mode === "continue" ? "CONTINUING" : mode === "off" ? "DEBUGGER OFF" : "STEP MODE");
    document.getElementById("next").disabled = !paused;
    document.getElementById("continue").disabled = mode === "off" || (mode === "continue" && !paused);
    document.getElementById("pause").disabled = mode === "off" || (mode === "step" && !paused);
  }

  async function control(action) {
    try {
      await fetch("/control", {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body:JSON.stringify({ action:action })
      });
    } catch (e) {
      document.getElementById("debug-state").textContent = "CONTROL ERROR";
    }
  }
  function releaseVisualPause() {
    if (!visualPaused) return;
    visualPaused = false;
    setDebuggerState(mode === "continue" ? "running" : "step",
      mode === "continue" ? "CONTINUING" : "STEP MODE · next hook");
    var pending = queuedMessages.splice(0);
    pending.forEach(handleLiveMessage);
  }
  document.getElementById("next").addEventListener("click", function () {
    if (visualPaused) releaseVisualPause();
    else control("next");
  });
  document.getElementById("continue").addEventListener("click", function () {
    if (visualPaused) releaseVisualPause();
    control("continue");
  });
  document.getElementById("pause").addEventListener("click", function () { control("pause"); });
  setDebuggerState("off", "DEBUGGER OFF · use /agentic-loop-debugger on");

  // SSE never reloads the page; reconnects are silent and state is preserved.
  function connect() {
    var es = new EventSource("/events");
    es.onopen = function () { document.getElementById("dot").classList.add("live"); };
    es.onerror = function () {
      document.getElementById("dot").classList.remove("live");
      es.close(); setTimeout(connect, 2000);
    };
    es.onmessage = function (ev) {
      if (!ev.data) return;
      var m = JSON.parse(ev.data);
      if (m.heartbeat) return;
      if (visualPaused) {
        queuedMessages.push(m);
        return;
      }
      handleLiveMessage(m);
    };
  }

  function handleLiveMessage(m) {
      if (m.kind === "snapshot") {
        if (m.pause) setDebuggerState("paused", "PAUSED · " + m.pause.node);
        else if (m.node === "RESPONSE" && m.mode === "step") {
          visualPaused = true;
          setDebuggerState("paused", "PAUSED · RESPONSE (visual breakpoint)");
        } else setDebuggerState(m.mode === "step" ? "step" : m.mode === "continue" ? "running" : "off");
        if (m.node) pulse(m.node, m.label);
        return;
      }
      if (m.kind === "debugger") {
        if (m.state === "paused") {
          setDebuggerState("paused", "PAUSED · " + m.node);
          if (m.node) pulse(m.node, m.label);
        } else if (m.state === "running") {
          setDebuggerState("running", "CONTINUING");
        } else if (m.state === "step") {
          setDebuggerState("step", "STEP MODE · next hook");
        } else if (m.state === "off") {
          setDebuggerState("off", "DEBUGGER OFF");
        } else if (m.state === "resumed") {
          setDebuggerState(mode === "continue" ? "running" : mode === "off" ? "off" : "step");
        }
        return;
      }
      // Passive event-stream signals update the log/highlight only when not
      // stopped at a real awaited hook breakpoint.
      if (m.node && !paused) pulse(m.node, m.label);
      if (m.node) log(m.ts, m.type, m.node, !!HOOKS[m.type], m.toolName);
      // There is no awaited V2 hook after a final assistant text event. The
      // session is already complete, so hold the debugger timeline here and
      // queue later WAIT/idle signals until the user presses Next.
      if (m.node === "RESPONSE" && mode === "step") {
        visualPaused = true;
        setDebuggerState("paused", "PAUSED · RESPONSE (visual breakpoint)");
        return;
      }
      if (m.node === "WAIT" && mode !== "off")
        setDebuggerState("idle", "WAITING FOR PROMPT");
  }
  connect();
</script>
</body>
</html>`;
  const mermaidSrc =
    process.env.AGENTIC_LOOP_MERMAID ||
    "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js";
  return html
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
