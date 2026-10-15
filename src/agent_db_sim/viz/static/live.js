/* agent-db-sim — live animated component view.
 *
 * Streams ``metrics.db`` sample rows over SSE and plays them back on its own
 * sim-clock: request dots ride the topology wiring, queue cylinders fill with
 * ``queue_depth`` levels, slot bars light with ``slot_busy`` occupancy, links
 * glow on network activity. Playback freezes on the run's final frame.
 *
 * Fidelity contracts (live-loop review):
 * 1. HUD token counters never look frozen. Counter *events* only fire at
 *    service start/end, so the view paces each request's billed tokens
 *    continuously at the endpoint's configured aggregate tok/s (exactly the
 *    saturation behaviour the scenario predicts), and snaps to the exact
 *    authoritative event totals when playback freezes at the run end.
 * 2. The page follows the server-current run: ``/live/state`` is polled; a
 *    new ``run_id`` (form *or* API launch) re-adopts the stream and rebuilds
 *    the scene from the new run's resolved config.
 * 3. A pill says which: ``live`` while the server run is running, ``replay``
 *    while catching up on an already-finished run, ``done``/``error`` at the
 *    end of playback.
 * 4. Closed-loop waiting is labelled, never blank: the model emits the first
 *    inference at t=0 and every later request the instant the previous
 *    round-trip completes, so an agent "waiting" IS thinking/querying — the
 *    agent badge shows exactly that with elapsed sim-time, the launch button
 *    mirrors the server run state machine, and a d3-wave panel streams the
 *    queue/occupancy level signals alongside the scene.
 */
(function () {
  "use strict";

  var NS = "http://www.w3.org/2000/svg";
  var svg = document.getElementById("topo");
  function $(id) { return document.getElementById(id); }
  function el(tag, attrs, parent) {
    var n = document.createElementNS(NS, tag);
    for (var k in (attrs || {})) n.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(n);
    return n;
  }
  function txt(parent, cls, x, y, s, anchor, id) {
    var a = { x: x, y: y, class: cls, "text-anchor": anchor || "start" };
    if (id) a.id = id;
    var t = el("text", a, parent);
    t.textContent = s;
    return t;
  }
  function titleOf(node, s) { el("title", {}, node).textContent = s; }
  function clamp(v, a, b) { return v < a ? a : v > b ? b : v; }
  function curve(x1, y1, x2, y2) {
    if (Math.abs(x2 - x1) >= Math.abs(y2 - y1)) {
      var mx = (x1 + x2) / 2;
      return "M" + x1 + " " + y1 + " C " + mx + " " + y1 + ", " + mx + " " + y2 +
        ", " + x2 + " " + y2;
    }
    var my = (y1 + y2) / 2;
    return "M" + x1 + " " + y1 + " C " + x1 + " " + my + ", " + x2 + " " + my +
      ", " + x2 + " " + y2;
  }

  var C = {
    idle: "#3a4252", queued: "#ffb454", processing: "#4cc9f0",
    completed: "#3fd68f", transfer: "#b28dff",
  };

  /* ------------------------------------------------------------------ */
  var S = {
    speed: 1, queue: [], qi: 0, cursor: null, lastT: 0,
    streamDone: false, playing: false, error: null, runId: null, until: 0,
    status: null, launchMode: "idle",   // idle | sending | waiting | running | done | error
    // authoritative event totals + continuously-paced display values:
    // events bill a request's tokens at service start (input) / completion
    // (output); the view drains the pending pool at the configured aggregate
    // tok/s so counters move every second of playback on any loaded run.
    counters: { in_auth: 0, out_auth: 0, in_disp: 0, out_disp: 0,
      in_pending: 0, out_pending: 0 },
    dots: [],
  };
  window.__live = S; // test/verification hook
  var MAX_AGENTS = 10, MAX_SLOTS = 16, MAX_NODES = 4;
  var M = null, SC = null;

  /* number formatting for the capability labels ---------------------- */
  function fmtTok(x) { return Math.round(x).toLocaleString("en-US"); }
  function fmtIops(x) {
    if (x >= 1e6) return trimZero(x / 1e6) + "M";
    if (x >= 1e3) return trimZero(x / 1e3) + "K";
    return trimZero(x);
  }
  function trimZero(x) {
    var s = (Math.round(x * 100) / 100).toFixed(2);
    return s.replace(/0+$/, "").replace(/\.$/, "");
  }

  /* ------------------------------------------------------------------ */
  /* topology model                                                      */
  /* ------------------------------------------------------------------ */
  function buildModel(cfg) {
    var db = cfg.db || {};
    var m = {
      agentCount: cfg.agents.count,
      llm: { name: "llm", slots: cfg.llm.slots, ingress: cfg.llm.ingress || "fifo",
        queue: 0, queueTarget: 0, busy: 0, busyTarget: 0, requests: 0,
        served: 0, agents: [] },
      db: { topology: db.topology, backend: db.backend,
        compute: [], storage: [],
        nodes: db.compute_nodes || 1,
        caps: {
          compute: db.compute || { slots: 1 },
          storage: db.storage || {},
          network: db.network || null,
        } },
      rates: { in: cfg.llm.input_tokens_per_s, out: cfg.llm.output_tokens_per_s },
    };
    var nComp = cfg.db.topology === "centralized" ? 1
      : clamp(cfg.db.compute_nodes, 1, MAX_NODES);
    var nSto = cfg.db.topology === "centralized" ? 1
      : clamp(cfg.db.storage_nodes || 1, 1, MAX_NODES);
    for (var i = 0; i < nComp; i++)
      m.db.compute.push({ name: "compute-" + i, stripe: 0, queue: 0,
        queueTarget: 0, busy: 0, busyTarget: 0, served: 0 });
    for (var j = 0; j < nSto; j++)
      m.db.storage.push({ name: "storage-" + j, queue: 0, queueTarget: 0,
        busy: 0, busyTarget: 0 });
    return m;
  }

  /* ------------------------------------------------------------------ */
  /* SVG primitives: queue cylinder + slot bars                          */
  /* ------------------------------------------------------------------ */
  function cylinder(parent, x, y, w, h, label) {
    var g = el("g", { class: "node cyl" }, parent);
    var ry = clamp(w / 5, 5, 9);
    var top = y + ry / 2, bottom = y + h - ry / 2;
    var bodyD = "M" + x + " " + top + " v" + (h - ry) + " a" + (w / 2) + " " +
      (ry / 2) + " 0 0 0 " + w + " 0 v" + -(h - ry);
    el("path", { class: "cyl-body", d: bodyD }, g);
    var fill = el("rect", { class: "cyl-fill", x: x + 2, y: bottom, width: w - 4,
      height: 0 }, g);
    var clipId = "clip" + Math.random().toString(36).slice(2, 9);
    var clip = el("clipPath", { id: clipId }, g);
    el("path", { d: bodyD + " a" + (w / 2) + " " + (ry / 2) + " 0 0 0 " + (-w) +
      " 0 z" }, clip);
    fill.setAttribute("clip-path", "url(#" + clipId + ")");
    el("ellipse", { class: "cyl-top", cx: x + w / 2, cy: top, rx: w / 2, ry: ry / 2 }, g);
    var val = txt(g, "n-val", x + w / 2, y + h + 14, "0", "middle");
    txt(g, "n-sub", x + w / 2, y + h + 29, label, "middle");
    return { g: g, fill: fill, val: val, top: top, bottom: bottom, bodyH: h - ry,
      max: 2 };
  }
  function setFill(cyl, depth) {
    var frac = clamp(depth / cyl.max, 0, 1);
    var hh = frac * cyl.bodyH;
    cyl.fill.setAttribute("y", cyl.bottom - hh);
    cyl.fill.setAttribute("height", Math.max(hh, 0.001));
    cyl.val.textContent = String(Math.round(depth * 10) / 10);
  }
  function slotRow(parent, x, y, n, w, gap, hgt) {
    var bars = [];
    var g = el("g", {}, parent);
    el("rect", { x: x - 3, y: y - 2, width: n * (w + gap) + 2, height: hgt + 4,
      rx: 3, fill: "#0c1220", stroke: "#1c2532" }, g);
    var bh = hgt - 8;
    for (var i = 0; i < n; i++) {
      var b = el("rect", { class: "slot", x: x + i * (w + gap), y: y + hgt - 6,
        width: w, height: 0.001, rx: 1.5 }, g);
      b._baseBottom = y + hgt - 6 + 0.001;
      b._bh = bh;
      bars.push(b);
    }
    return bars;
  }
  function setSlots(bars, busy) {
    for (var i = 0; i < bars.length; i++) {
      var frac = clamp(busy - i, 0, 1);
      var b = bars[i], hh = frac * b._bh;
      var bottom = b._baseBottom;
      b.setAttribute("height", Math.max(hh, 0.001));
      b.setAttribute("y", bottom - Math.max(hh, 0.001));
      b.classList.toggle("busy", frac > 0.03);
    }
  }

  /* ------------------------------------------------------------------ */
  /* scene                                                               */
  /* ------------------------------------------------------------------ */
  function renderScene() {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    var defs = el("defs", {}, svg);
    var grad = el("linearGradient", { id: "qgrad", x1: 0, y1: 0, x2: 0, y2: 1 }, defs);
    el("stop", { offset: "0%", "stop-color": "#ffd27a" }, grad);
    el("stop", { offset: "100%", "stop-color": "#ff9d2e" }, grad);
    var filt = el("filter", { id: "glow", x: "-80%", y: "-80%", width: "260%",
      height: "260%" }, defs);
    el("feGaussianBlur", { stdDeviation: 3, result: "b" }, filt);
    var mg = el("feMerge", {}, filt);
    el("feMergeNode", { in: "b" }, mg);
    el("feMergeNode", { in: "SourceGraphic" }, mg);

    var grid = el("g", { class: "gridlines" }, svg);
    for (var gx = 0; gx <= 1300; gx += 65) el("line", { x1: gx, y1: 0, x2: gx, y2: 720 }, grid);
    for (var gy = 0; gy <= 720; gy += 60) el("line", { x1: 0, y1: gy, x2: 1300, y2: gy }, grid);
    txt(svg, "frame-note", 16, 24, "live topology — agent ⇄ llm ⇄ db");

    /* agents */
    var shown = Math.min(M.agentCount, MAX_AGENTS);
    var ax = 46, ay = 100, aw = 150, ah = 46, agap = 12;
    var agG = el("g", {}, svg);
    txt(agG, "n-sub", ax, ay - 14, "agent pool · ×" + M.agentCount);
    for (var i = 0; i < shown; i++) {
      var y = ay + i * (ah + agap);
      var g = el("g", { class: "node agent" }, agG);
      var r = el("rect", { x: ax, y: y, width: aw, height: ah, rx: 8 }, g);
      txt(g, "n-title", ax + 11, y + 20, "agent-" + i);
      var badge = txt(g, "n-badge", ax + 11, y + 37, "");
      var dot = el("circle", { class: "state-dot", cx: ax + aw - 13,
        cy: y + ah / 2, r: 5, fill: C.idle }, g);
      titleOf(g, "agent-" + i + " — closed-loop: inference then grounded db query");
      M.llm.agents.push({ g: g, r: r, dot: dot, badge: badge,
        x: ax + aw, y: y + ah / 2, wait: null, flash: null });
    }
    if (M.agentCount > shown)
      txt(agG, "n-sub", ax, ay + shown * (ah + agap) + 4,
        "+" + (M.agentCount - shown) + " more (round-robin)");

    /* LLM endpoint */
    var lx = 356, ly = 130, lw = 230, lh = 420;
    var lg = el("g", { class: "node" }, svg);
    el("rect", { x: lx, y: ly, width: lw, height: lh, rx: 12, fill: "#16202e",
      stroke: "#232c3b" }, lg);
    txt(lg, "n-title", lx + 14, ly + 28, "llm.server");
    txt(lg, "n-sub", lx + 14, ly + 46, "cap · in " + fmtTok(M.rates.in) +
      " · out " + fmtTok(M.rates.out) + " tok/s");
    txt(lg, "n-sub", lx + 14, ly + 60, M.llm.slots + " slot" +
      (M.llm.slots === 1 ? "" : "s") + " · " + M.llm.ingress + " ingress");
    M.llm.cyl = cylinder(lg, lx + 26, ly + 76, 62, 164, "ingress");
    var nb = Math.min(M.llm.slots, MAX_SLOTS);
    txt(lg, "n-sub", lx + 108, ly + 92, "slots busy · " + M.llm.slots +
      (M.llm.slots > nb ? " (" + nb + " shown)" : ""));
    M.llm.bars = slotRow(lg, lx + 108, ly + 102, nb,
      clamp(Math.floor(110 / nb), 6, 22), 4, 138);
    M.llm.counterTxt = txt(lg, "n-val", lx + 14, ly + lh - 44, "in 0 · out 0",
      "start", "hud-tokens");
    M.llm.flowTxt = txt(lg, "n-val", lx + 14, ly + lh - 22, "req 0 · served 0",
      "start", "hud-flow");

    /* DB endpoint shell */
    var dbX = 640, dbY = 70, dbW = 634, dbH = 590;
    var net = M.db.caps.network || null;
    var cSlots = M.db.caps.compute.slots || 1;
    var head = "db endpoint · " + M.db.topology + " · " +
      M.db.compute.length + " compute node" + (M.db.compute.length === 1 ? "" : "s") +
      " × " + cSlots + " slot" + (cSlots === 1 ? "" : "s") +
      (net && M.db.topology !== "centralized" ?
        " · net " + trimZero(net.bandwidth) + " GB/s · pool " +
        (net.connections > 0 ? net.connections : "∞") +
        " · lat " + trimZero(net.latency) : " · no internal net");
    var dbg = el("g", {}, svg);
    el("rect", { x: dbX, y: dbY, width: dbW, height: dbH, rx: 16, fill: "none",
      stroke: "#1b2534", "stroke-dasharray": "3 7" }, dbg);
    txt(dbg, "n-sub", dbX + 14, dbY + 24, head);

    var cw = M.db.compute.length === 1 ? 190 : 128, ch = 250;
    var cx0 = dbX + 36, cy0 = dbY + 56;
    var cstep = M.db.compute.length > 1
      ? (dbW - 72 - cw) / (M.db.compute.length - 1) : 0;
    M.db.compute.forEach(function (c, i) {
      c.x = cx0 + i * cstep; c.y = cy0;
      var g = el("g", { class: "node" }, dbg);
      el("rect", { x: c.x, y: c.y, width: cw, height: ch, rx: 10, fill: "#151d2a",
        stroke: "#232c3b" }, g);
      txt(g, "n-title", c.x + 12, c.y + 26, c.name);
      txt(g, "n-sub", c.x + cw - 12, c.y + 26, "×" + cSlots + " slots", "end");
      c.cyl = cylinder(g, c.x + 16, c.y + 44, 50, 130, "ingress");
      var ns = Math.min(6, MAX_SLOTS);
      txt(g, "n-sub", c.x + 82, c.y + 56, "compute slots");
      c.bars = slotRow(g, c.x + 82, c.y + 66, ns, Math.max(6, (cw - 100) / ns - 4), 4, 108);
      c.servedTxt = txt(g, "n-val", c.x + 12, c.y + ch - 12, "served 0");
      titleOf(g, c.name + " — db query compute node · " + cSlots +
        " slots · " + (M.db.caps.compute.ingress || "fifo") + " ingress · issue ratio " +
        trimZero(M.db.caps.compute.issue_ratio || 0.1) + " · " +
        (M.db.caps.compute.phase_split || "three_phase"));
    });

    var sw = M.db.storage.length === 1 ? 190 : 128, sh = 230;
    var sy0 = cy0 + ch + 90;
    var sstep = M.db.storage.length > 1
      ? (dbW - 72 - sw) / (M.db.storage.length - 1) : 0;
    var sp = M.db.caps.storage || {};
    var capTxt = (sp.iops_ceiling !== undefined ?
      fmtIops(sp.iops_ceiling) + " ops/s · " + trimZero(sp.bandwidth_ceiling) + " GB/s" :
      "backend " + M.db.backend);
    M.db.storage.forEach(function (s, j) {
      s.x = dbX + 36 + j * sstep; s.y = sy0;
      var g = el("g", { class: "node" }, dbg);
      el("rect", { x: s.x, y: s.y, width: sw, height: sh, rx: 10, fill: "#151d2a",
        stroke: "#232c3b" }, g);
      txt(g, "n-title", s.x + 12, s.y + 26, s.name);
      txt(g, "n-sub", s.x + sw - 12, s.y + 26, M.db.backend, "end");
      s.cyl = cylinder(g, s.x + 16, s.y + 44, 50, 120, M.db.backend + " queue");
      txt(g, "n-sub", s.x + 82, s.y + 56, "service");
      s.bars = slotRow(g, s.x + 82, s.y + 66, 4, Math.max(6, (sw - 100) / 4 - 4), 4, 98);
      txt(g, "n-sub", s.x + 12, s.y + sh - 28, capTxt);
      s.busyTxt = txt(g, "n-val", s.x + 12, s.y + sh - 12, "idle");
      titleOf(g, s.name + " — " + M.db.backend + " storage node · " +
        (sp.base_latency !== undefined ? "base lat " + sp.base_latency +
          " · per-GB " + sp.per_gb_time + " · " : "") + capTxt +
        " (shared across in-flight ops)");
    });

    /* links */
    var edges = el("g", {}, svg);
    SC = { links: {}, dotsLayer: null };
    function link(id, d, label, px, py) {
      var p = el("path", { class: "edge", d: d, id: id }, edges);
      SC.links[id] = { path: p, glow: 0 };
      if (label) txt(edges, "linklabel", px, py, label, "middle");
    }
    var aMid = M.llm.agents.length
      ? M.llm.agents[Math.floor(M.llm.agents.length / 2)].y : ay + ah / 2;
    link("ag-llm", curve(ax + aw, aMid, lx, ly + lh / 2), "submit", (ax + aw + lx) / 2,
      aMid - 10);
    // return lane leaves the LLM left edge and arcs back to the agent column
    link("llm-ag", "M" + lx + " " + (ly + lh - 70) +
      " C " + ((ax + aw + lx) / 2) + " " + (ly + lh - 40) + ", " +
      ((ax + aw + lx) / 2) + " " + (aMid + ah + 20) + ", " + (ax + aw) + " " +
      (aMid + ah / 2), "return", (ax + aw + lx) / 2 - 6, ly + lh - 44);
    // agent ⇄ db grounded-query legs: two parallel arcs over the LLM block
    link("ag-db", "M" + (ax + aw) + " " + (ay - 30) +
      " C " + ((ax + aw + lx) / 2) + " " + (ay - 78) + ", " + (lx + lw + 60) + " " +
      (ly - 78) + ", " + dbX + " " + (ly - 30), "query", (lx + lw + dbX) / 2 - 40,
      ly - 92);
    link("db-ag", "M" + dbX + " " + (ly - 8) +
      " C " + (lx + lw + 60) + " " + (ly - 34) + ", " + ((ax + aw + lx) / 2) + " " +
      (ay - 30) + ", " + (ax + aw) + " " + (ay - 12), "result",
      (lx + lw + dbX) / 2 - 40, ly - 50);
    M.db.compute.forEach(function (c, i) {
      M.db.storage.forEach(function (s, j) {
        var midX = (c.x + cw / 2 - 20 + s.x + sw / 2 - 20) / 2;
        var midY = (c.y + ch + s.y) / 2;
        // all links share one net config — label the first, hint the rest
        var lbl = (net && M.db.topology !== "centralized" && i === 0 && j === 0)
          ? "net " + trimZero(net.bandwidth) + " GB/s · pool " +
            (net.connections > 0 ? net.connections : "∞") : null;
        link("link-" + i + "-" + j,
          curve(c.x + cw / 2 - 20, c.y + ch, s.x + sw / 2 - 20, s.y),
          lbl, lbl ? midX : 0, lbl ? midY - 4 : 0);
      });
    });
    SC.dotsLayer = el("g", {}, svg);
  }

  /* ------------------------------------------------------------------ */
  /* row -> model                                                        */
  /* ------------------------------------------------------------------ */
  /* metrics.db row shapes (see TraceWriter):
   *  level   : signal is the bare metric ("queue_depth","slot_busy",
   *            "conn_busy"); the component lives in tags.component
   *  edge    : signal is "{component}.{stage}"; state/agent/request in tags
   *  counter : signal is "tokens_input"/"tokens_output" (possibly prefixed)
   *            or bare "requests"/"served" with tags.component            */
  function shortName(name) { return name.replace(/^agent_db_sim\./, ""); }

  function classify(comp) {
    if (/link-(\d+)-(\d+)/.test(comp)) return { kind: "link" };
    if (/llm/.test(comp)) return { kind: "llm" };
    if (/\.storage$/.test(comp) || /storage-\d+/.test(comp)) {
      var sm = comp.match(/(?:compute|node)-(\d+)/);
      var jm = comp.match(/storage-(\d+)/);
      return { kind: "storage", idx: jm ? +jm[1] : (sm ? +sm[1] : 0) };
    }
    var m = comp.match(/(?:compute|node)-(\d+)/);
    if (m) return { kind: "compute", idx: +m[1] };
    return { kind: "other" };
  }

  function nodeOf(key) {
    if (key.kind === "llm") return M.llm;
    if (key.kind === "compute") return M.db.compute[key.idx % M.db.compute.length];
    if (key.kind === "storage") return M.db.storage[key.idx % M.db.storage.length];
    return null;
  }

  function applyRow(row) {
    var name = shortName(row[1]), kind = row[2], value = row[3], tags = row[4] || {};
    var segs = name.split("."), leaf = segs[segs.length - 1];
    var comp = kind === "edge" ? segs.slice(0, -1).join(".") : (tags.component || "");

    if (kind === "level") {
      var wvNum = Number(value) || 0;
      if (leaf === "queue_depth" || leaf === "slot_busy" ||
          leaf === "conn_busy" || leaf === "level") {
        waveAdd(waveLabel(comp, leaf), row[0], wvNum);
      }
      if (leaf === "conn_busy" || leaf === "level") {
        var lm = comp.match(/link-(\d+)-(\d+)/);
        if (lm && SC && SC.links) {
          var lk = SC.links["link-" + lm[1] + "-" + lm[2]];
          if (lk) lk.glow = Math.max(lk.glow, Number(value) || 1);
        }
        return;
      }
      var n = nodeOf(classify(comp));
      if (!n) return;
      var v = Number(value) || 0;
      if (leaf === "queue_depth") {
        n.queueTarget = v;
        if (n.cyl) n.cyl.max = Math.max(n.cyl.max, v);
      } else if (leaf === "slot_busy") n.busyTarget = v;
      else if (leaf === "requests") n.requests = v;
      else if (leaf === "served") n.served = v;
      return;
    }
    if (kind === "counter") {
      if (leaf === "tokens_input" || leaf === "tokens_output") {
        if (typeof value === "number") {
          // bill the request's tokens into the pending pool; paint() drains
          // it at the configured aggregate rate (see header note)
          if (leaf === "tokens_input") {
            S.counters.in_auth += value; S.counters.in_pending += value;
          } else {
            S.counters.out_auth += value; S.counters.out_pending += value;
          }
        }
        return;
      }
      var nc = nodeOf(classify(comp || segs.slice(0, -1).join(".")));
      if (!nc) return;
      if (leaf === "requests") nc.requests += Number(value) || 0;
      else if (leaf === "served") nc.served += Number(value) || 0;
      return;
    }
    if (kind === "edge") {
      var stage = leaf.replace(/_state$/, "");
      var state = String(tags.state !== undefined ? tags.state : value).replace(/^"|"$/g, "");
      var am = comp.match(/agent-(\d+)/);
      handleStage(stage, state, am ? +am[1] : null, classify(comp));
    }
  }

  /* ------------------------------------------------------------------ */
  /* agent states: the closed loop means an agent is NEVER idle while    */
  /* its request is in flight — it is WAITING (thinking: inference       */
  /* response pending; querying: db response pending). The badge labels  */
  /* that wait with live elapsed sim-time so the loop reads as work.     */
  /* ------------------------------------------------------------------ */
  function handleStage(stage, state, agentIdx, key) {
    var ag = agentIdx !== null
      ? M.llm.agents[agentIdx % M.llm.agents.length] : null;
    var now = S.cursor === null ? 0 : S.cursor;
    if (stage === "inference" && ag) {
      if (state === "submitted") { spawnDot("ag-llm", C.transfer); ag.wait = { kind: "llm", t0: now }; ag.flash = null; }
      else if (state === "received") { spawnDot("llm-ag", C.completed); ag.wait = null; ag.flash = { text: "✓ llm", until: now + 0.8 }; }
    } else if (stage === "db" && ag) {
      if (state === "submitted") { spawnDot("ag-db", C.processing); ag.wait = { kind: "db", t0: now }; ag.flash = null; }
      else if (state === "received") { spawnDot("db-ag", C.completed); ag.wait = null; ag.flash = { text: "✓ db", until: now + 0.8 }; }
    } else if (stage === "compute_issue" && state === "completed" &&
               key.kind === "compute" && M.db.compute.length) {
      var c = M.db.compute[key.idx % M.db.compute.length];
      var j = c.stripe++ % M.db.storage.length;
      spawnDot("link-" + (key.idx % M.db.compute.length) + "-" + j, C.queued);
    }
  }

  function paintAgents() {
    var now = S.cursor === null ? 0 : S.cursor;
    for (var i = 0; i < M.llm.agents.length; i++) {
      var ag = M.llm.agents[i];
      var color = C.idle, label = "", lcolor = "#5b6b81";
      if (ag.wait) {
        var secs = Math.max(0, now - ag.wait.t0);
        if (ag.wait.kind === "llm") { color = C.transfer; label = "thinking · " + secs.toFixed(1) + "s"; }
        else { color = C.processing; label = "querying · " + secs.toFixed(1) + "s"; }
        lcolor = color;
      } else if (ag.flash && now < ag.flash.until) {
        color = C.completed; label = ag.flash.text; lcolor = C.completed;
      }
      ag.dot.setAttribute("fill", color);
      if (color === C.idle) ag.r.removeAttribute("stroke");
      else ag.r.setAttribute("stroke", color);
      if (ag.badge.textContent !== label) ag.badge.textContent = label;
      ag.badge.setAttribute("fill", lcolor);
    }
  }

  /* ------------------------------------------------------------------ */
  /* wave panel: every level signal (queue_depth / slot_busy / conn_busy  */
  /* / staging level) folds into a live d3-wave waveform from the SAME    */
  /* SSE rows the scene plays — works identically for live runs and        */
  /* replay, because replay streams the full row log through applyRow.     */
  /* ------------------------------------------------------------------ */
  var WV = { by: {}, order: [], dirty: false, open: true, lastRender: 0 };
  window.__wave = WV; // wave-panel fold state (verification hook)
  var WAVE_THROTTLE_MS = 900;

  function waveLabel(comp, leaf) {
    var c = shortName(comp)
      .replace(/^database\.topology\./, "")
      .replace(/^db\./, "");
    if (leaf === "queue_depth") return c.replace(/\.ingress$/, "") + ".queue";
    if (leaf === "slot_busy") return c + ".busy";
    if (leaf === "conn_busy") return c + ".conns";
    if (leaf === "level") return c + ".staging";
    return c + "." + leaf;
  }

  function waveAdd(name, t, value) {
    if (typeof value !== "number" || !isFinite(value)) return;
    var s = WV.by[name];
    if (!s) { s = WV.by[name] = { rows: [], isReal: false }; WV.order.push(name); }
    if (name.slice(-7) === ".staging") s.isReal = true;
    var rows = s.rows, last = rows[rows.length - 1];
    if (last) {
      if (t < last.t) return;
      if (t === last.t) { last.value = value; WV.dirty = true; return; }
      if (last.value === value) return;   // same level: extend the last plateau
    }
    rows.push({ t: t, value: value });
    WV.dirty = true;
  }

  function waveState(msg) {
    var el2 = $("wave-state");
    if (el2) el2.textContent = msg;
  }

  function waveGraphFn() {  // vendored d3-wave UMD extends window.d3; older
    if (window.d3wave && window.d3wave.WaveGraph) return window.d3wave.WaveGraph;
    if (window.d3 && window.d3.WaveGraph) return window.d3.WaveGraph;
    return null;             // builds referenced it as window.d3wave — accept both
  }

  function renderWave() {
    var host = $("livewave");
    if (!host) return;
    if (!WV.open || !host.clientWidth) { WV.dirty = true; return; } // defer
    WV.dirty = false;
    WV.lastRender = performance.now();
    var wgf = waveGraphFn();
    if (!wgf) {
      waveState("vendor JS missing (d3 / d3-wave)");
      return;
    }
    if (!WV.order.length) { waveState("no level signals streamed yet"); return; }
    var tMax = 0, pts = 0;
    WV.order.forEach(function (n) {
      var rows = WV.by[n].rows;
      pts += rows.length;
      if (rows.length) tMax = Math.max(tMax, rows[rows.length - 1].t);
    });
    tMax = Math.max(tMax, S.cursor || 0, 1);     // >=1: d3 zoom-extent guard
    // this d3-wave build renders arbitrary level values through its enum row:
    // every plateau is a box labelled with the value — ideal for queue-depth
    // and occupancy waveforms. Root is a collapsible 'label' group.
    // d3-wave labels axis ticks assuming VCD timescale quanta (ps base);
    // sim time is seconds — scale by 1e12 so its unit ladder labels "s".
    var TS = 1e12;
    var children = WV.order.map(function (name) {
      var sig = WV.by[name];
      var rows = sig.rows;
      var data = rows.map(function (r, i) {
        var nextT = (i + 1 < rows.length) ? rows[i + 1].t : tMax;
        var v = sig.isReal ? Math.round(r.value * 100) / 100 : Math.round(r.value);
        return [r.t * TS, String(v), Math.max((nextT - r.t) * TS, 0)];
      });
      return { name: name, type: { name: "enum" }, data: data };
    });
    var w = Math.max(host.clientWidth - 8, 320);
    var h = (children.length + 1) * 25 + 56;     // row 20 + ypadding 5 per signal
    host.innerHTML = "";
    var svg = d3.select(host).append("svg")
      .attr("width", w).attr("height", h)
      .style("width", w + "px").style("height", h + "px");
    try {
      var wg = new wgf(svg);
      wg.bindData({ name: "queue signals", type: { name: "label" },
        children: children });
    } catch (e) {
      waveState("wave render failed: " + e);
      return;
    }
    waveState(WV.order.length + " queue signals · " + pts + " samples · t≤" +
      tMax.toFixed(2) + (S.playing ? " · streaming" : " · final"));
  }

  function resetWave() {
    WV.by = {}; WV.order = []; WV.dirty = false;
    var host = $("livewave");
    if (host) host.innerHTML = "";
    waveState("waiting for level signals…");
  }

  /* ------------------------------------------------------------------ */
  /* dots                                                                */
  /* ------------------------------------------------------------------ */
  var DOT_MIN_MS = 520;   // minimum visible travel wall-time per request dot
  var DOT_PER_LINK = 14;  // pile-up cap so fast churn stays readable

  function spawnDot(linkId, color) {
    if (!SC || !SC.links[linkId]) return;
    var path = SC.links[linkId].path;
    // never shorter than the minimum travel: fast-churn runs must show
    // continuous traffic, not one-frame blinks between screenshots
    var dur = Math.max(DOT_MIN_MS, 1600 / S.speed);
    var live = 0;
    for (var i = 0; i < S.dots.length; i++)
      if (S.dots[i].link === linkId) live++;
    if (live >= DOT_PER_LINK) {  // retire this link's oldest dot
      for (i = 0; i < S.dots.length; i++)
        if (S.dots[i].link === linkId) {
          var dead = S.dots.splice(i, 1)[0];
          if (dead.c.parentNode) dead.c.parentNode.removeChild(dead.c);
          break;
        }
    }
    var c = el("circle", { class: "req-dot", r: 4.5, fill: color,
      filter: "url(#glow)" }, SC.dotsLayer);
    S.dots.push({ c: c, path: path, link: linkId, len: path.getTotalLength(),
      t0: performance.now(), dur: dur });
    if (S.dots.length > 300) {
      var old = S.dots.shift();
      if (old.c.parentNode) old.c.parentNode.removeChild(old.c);
    }
    SC.links[linkId].glow = Math.max(SC.links[linkId].glow, 0.7);
  }

  /* ------------------------------------------------------------------ */
  /* playback loop                                                       */
  /* ------------------------------------------------------------------ */
  var lastFrame = 0, rafId = null;
  function frame(now) {
    rafId = null;
    if (!S.playing) return;
    var dt = Math.min((now - lastFrame) / 1000, 0.25); lastFrame = now;
    if (S.cursor === null && S.queue.length) S.cursor = S.queue[0][0];
    if (S.cursor !== null) {
      var target = S.cursor + dt * S.speed;
      if (target > S.lastT) target = S.lastT;
      while (S.qi < S.queue.length && S.queue[S.qi][0] <= target) applyRow(S.queue[S.qi++]);
      S.cursor = target;
      if (S.streamDone && S.cursor >= S.lastT) { freezeAtEnd(); return; }
    }
    paint(dt);
    rafId = requestAnimationFrame(frame);
  }
  function startPlayback() {
    if (S.playing) return;
    S.playing = true; lastFrame = performance.now();
    if (rafId === null) rafId = requestAnimationFrame(frame);
  }

  function ease(cur, tgt, dt) { return cur + (tgt - cur) * (1 - Math.exp(-3.2 * dt)); }

  function paint(dt) {
    paintAgents();
    [M.llm].concat(M.db.compute, M.db.storage).forEach(function (n) {
      if (!n) return;
      n.queue = ease(n.queue, n.queueTarget, dt);
      if (n.cyl) {
        // adaptive scale: snap up on new peaks, drift down (~8 s) so an
        // early burst doesn't permanently flatten later queueing
        if (n.queue > n.cyl.max) n.cyl.max = n.queue;
        else n.cyl.max = Math.max(
          n.queue,
          n.cyl.max + (Math.max(n.queueTarget, 1) - n.cyl.max) * (1 - Math.exp(-dt / 8))
        );
        setFill(n.cyl, n.queue);
      }
      n.busy = ease(n.busy, n.busyTarget, dt);
      setSlots(n.bars, n.busy);
    });
    // token counters: drain the billed-pending pool at the endpoint's
    // configured aggregate tok/s so the HUD moves continuously on playback
    // even when events only bill at service boundaries (see header note).
    var c = S.counters;
    var simDt = dt * S.speed;
    var din = c.in_pending > 0
      ? Math.min(c.in_pending, (M.rates.in > 0 ? M.rates.in * simDt : c.in_pending))
      : 0;
    var dout = c.out_pending > 0
      ? Math.min(c.out_pending, (M.rates.out > 0 ? M.rates.out * simDt : c.out_pending))
      : 0;
    c.in_disp += din; c.in_pending -= din;
    c.out_disp += dout; c.out_pending -= dout;
    if (M.llm.flowTxt) M.llm.flowTxt.textContent =
      "req " + Math.round(M.llm.requests || 0) + " · served " + Math.round(M.llm.served || 0);
    if (M.llm.counterTxt) M.llm.counterTxt.textContent =
      "in " + Math.round(c.in_disp) + " · out " + Math.round(c.out_disp);
    M.db.compute.forEach(function (cc) {
      if (cc.servedTxt) cc.servedTxt.textContent = "served " + Math.round(cc.served || 0);
    });
    M.db.storage.forEach(function (s) {
      if (s.busyTxt) s.busyTxt.textContent =
        (s.busyTarget > 0.05 ? "writing" : s.queueTarget > 0.05 ? "queued" : "idle");
    });
    var now = performance.now();
    S.dots = S.dots.filter(function (d) {
      var f = (now - d.t0) / d.dur;
      if (f >= 1) { if (d.c.parentNode) d.c.parentNode.removeChild(d.c); return false; }
      var pt = d.path.getPointAtLength(d.len * f);
      d.c.setAttribute("transform", "translate(" + pt.x + "," + pt.y + ")");
      d.c.setAttribute("opacity", f > 0.82 ? String((1 - f) / 0.18) : "1");
      return true;
    });
    for (var k in SC.links) {
      var lk = SC.links[k];
      lk.path.classList.toggle("hot", lk.glow > 0.05);
      lk.glow *= Math.exp(-1.5 * dt);
    }
    $("clock").textContent = S.cursor === null ? "0.00" : S.cursor.toFixed(2);
    if (S.until > 0)
      $("bar").style.width = (clamp(S.cursor || 0, 0, S.until) / S.until * 100).toFixed(1) + "%";
    if (WV.dirty && WV.open && now - WV.lastRender > WAVE_THROTTLE_MS) renderWave();
  }

  function freezeAtEnd() {
    S.playing = false;
    if (!M) { setPill("idle"); return; }   // never played a frame (idle stream)
    // playback caught the whole run: show the exact authoritative totals
    var c = S.counters;
    c.in_disp = c.in_auth; c.out_disp = c.out_auth;
    c.in_pending = 0; c.out_pending = 0;
    paint(0.001);
    renderWave();  // final waveform frame — no signal left unpainted
    setPill(S.error ? "error" : "done");
    $("clock").textContent = S.lastT.toFixed(2);
    $("bar").style.width = "100%";
    setLaunch(S.error ? "error" : "again",
      S.error ? S.error : "run complete · frozen at the last frame");
    if (S.donePayload && S.donePayload.report) showFinish(S.donePayload);
  }

  /* ------------------------------------------------------------------ */
  /* launch button state machine — the click must never feel lost:       */
  /* LAUNCH → ⏳ LAUNCHING… → ◉ RUNNING… → ▶ LAUNCH AGAIN, and every     */
  /* failure (409 / 422 / transport / run error) shows inline next to    */
  /* the button and re-enables it. /live/state is the truth: even a dead */
  /* SSE stream can never leave the button stuck disabled.               */
  /* ------------------------------------------------------------------ */
  var LAUNCH_TXT = "▶ launch live run";
  function setMsg(text, cls) {
    var m = $("form-msg");
    m.textContent = text || "";
    m.className = "msg" + (cls ? " " + cls : "");
  }
  function setLaunch(mode, msg, msgCls) {
    S.launchMode = mode;
    var b = $("launch");
    b.classList.remove("sending", "running");
    if (mode === "sending") {
      b.disabled = true; b.classList.add("sending"); b.textContent = "⏳ launching…";
    } else if (mode === "running") {
      b.disabled = true; b.classList.add("running"); b.textContent = "◉ running…";
    } else if (mode === "again") {
      b.disabled = false; b.textContent = "▶ launch again";
    } else {  // idle | error
      b.disabled = false; b.textContent = LAUNCH_TXT;
    }
    if (msg !== undefined) setMsg(msg, msgCls === undefined ?
      (mode === "error" ? "" : "ok") : msgCls);
  }

  /* ------------------------------------------------------------------ */
  /* EXPORT: header button — visible for the current live run (and      */
  /* after DONE). Plain anchor to GET /live/export?run_id=…; the server  */
  /* sets Content-Disposition: attachment, so the click just downloads.  */
  /* Zips whatever exists at click time: safe even mid-run (metrics.db   */
  /* is snapshotted server-side through sqlite's online-backup API).     */
  /* ------------------------------------------------------------------ */
  var EXPORT_BASE = "/live/export?run_id=";
  function setExport(runId) {
    var b = $("export");
    if (!runId) {
      if (!b.classList.contains("hidden")) b.classList.add("hidden");
      return;
    }
    var href = EXPORT_BASE + encodeURIComponent(runId);
    if (b.getAttribute("href") !== href) b.setAttribute("href", href);
    b.classList.remove("hidden");
  }

  /* ------------------------------------------------------------------ */
  /* SSE                                                                 */
  /* ------------------------------------------------------------------ */
  var es = null;

  function resetPlayback() {
    S.queue = []; S.qi = 0; S.cursor = null; S.lastT = 0;
    S.streamDone = false; S.playing = false; S.error = null;
    S.donePayload = null;
    S.dots.forEach(function (d) {
      if (d.c.parentNode) d.c.parentNode.removeChild(d.c);
    });
    S.dots = [];
    S.counters = { in_auth: 0, out_auth: 0, in_disp: 0, out_disp: 0,
      in_pending: 0, out_pending: 0 };
    resetWave();
    $("finish").classList.add("hidden");
    $("bar").style.width = "0%";
  }

  function applyInit(d) {
    // every init (fresh connect, run switch, EventSource reconnect replays
    // from row 0) reseats the scene from *this* run's resolved config
    S.runId = d.run_id; S.until = d.until || 0; S.status = d.status || "running";
    setExport(d.run_id);
    resetPlayback();
    M = buildModel(d.config);
    renderScene();
    updatePill();
    if (S.status === "running" || S.status === "starting") {
      setLaunch("running", "streaming live run " + d.run_id);
    } else if (S.status === "error") {
      setLaunch("error", d.error || "live run failed");
    } else {
      setLaunch("again", "replaying finished run " + d.run_id);
    }
    startPlayback();
  }

  function connectStream() {
    if (es) return;
    es = new EventSource("/live/stream");
    es.addEventListener("init", function (e) {
      var d = JSON.parse(e.data);
      // A zombie auto-reconnect of a stream we already drained (done) must
      // never reseat the scene mid-replay — that reads like the run restarting
      // itself. Fresh runs (different run_id) still adopt normally.
      if (S.streamDone && d.run_id === S.runId) {
        if (es) { es.close(); es = null; }
        return;
      }
      applyInit(d);
    });
    es.addEventListener("delta", function (e) {
      var d = JSON.parse(e.data);
      (d.rows || []).forEach(function (r) {
        S.queue.push(r);
        if (r[0] > S.lastT) S.lastT = r[0];
      });
      startPlayback();
    });
    es.addEventListener("done", function (e) {
      var d = JSON.parse(e.data);
      S.streamDone = true;
      if (d.error) S.error = d.error;
      S.donePayload = d;
      if (es) { es.close(); es = null; }
      // the server run is over. If init/deltas/done all landed before the
      // first animation frame (fast runs do exactly this), the queue still
      // holds unplayed rows: PLAY them — freezing here would leave the scene
      // stuck on an empty t=0 frame and look like nothing launched.
      if (S.cursor === null && S.queue.length) {
        startPlayback();
        updatePill();
      } else if (!S.playing || S.cursor >= S.lastT) {
        freezeAtEnd();
      } else {
        updatePill();
        setLaunch(d.error ? "error" : "again",
          d.error || "server run finished — page catching up (REPLAY)");
      }
    });
    es.addEventListener("error", function (e) {
      // named SSE "error" event (no run yet) carries a detail payload
      if (e.data) {
        try {
          var d = JSON.parse(e.data);
          if (d.detail) setMsg(d.detail);
        } catch (err) { /* transport-level retry errors have no data */ }
      }
    });
  }

  function showFinish(d) {
    var f = $("finish");
    f.classList.remove("hidden");
    f.innerHTML = "run <b>" + d.run_id + "</b> complete · t=" +
      Number(d.t_max || 0).toFixed(2) + "<br><a href='" + d.report +
      "'>open report</a> · <a href='" + d.vcd + "'>raw VCD</a> · " +
      "<a href='" + EXPORT_BASE + encodeURIComponent(d.run_id) +
      "' download>⬇ export zip</a>";
  }

  /* ------------------------------------------------------------------ */
  /* pill: LIVE only when the SERVER run is running; REPLAY while the    */
  /* page catches up on a run the server already finished                */
  /* ------------------------------------------------------------------ */
  function updatePill() {
    if (S.error) return setPill("error");
    if (S.streamDone) {
      var caught = !S.playing || S.cursor === null || S.cursor >= S.lastT;
      return setPill(caught ? "done" : "replay");
    }
    if (S.status === "done" || S.status === "error") return setPill("replay");
    if (S.status === "running" || S.status === "starting") return setPill("live");
    setPill("idle");
  }

  function setPill(cls) {
    var p = $("pill"); p.className = "pill " + cls; p.textContent = cls;
  }

  /* ------------------------------------------------------------------ */
  /* follow the server-current run                                       */
  /* ------------------------------------------------------------------ */
  var STATE_POLL_MS = 2000;

  function pollState() {
    fetch("/live/state").then(function (r) { return r.json(); }).then(function (st) {
      if (!st || !st.run_id) {
        if (!es && !S.runId) setPill("idle");
        if (es === null && S.runId) {   // server restarted: its slot is gone —
          S.runId = null;               // never leave a stale run blocking the UI
          setExport(null);
          setLaunch("idle", "server idle — launch a run");
        }
        return;
      }
      if (st.run_id !== S.runId) {   // form-OR-API-launched new run: re-adopt
        setExport(st.run_id);        // EXPORT follows the server-current run
        if (es) { es.close(); es = null; }
        connectStream();
        return;
      }
      S.status = st.status;          // same run: keep pill AND the launch
      updatePill();                  // button honest even if the stream died
      if (st.status === "running" || st.status === "starting") {
        if (S.launchMode !== "running") setLaunch("running", "live run " + st.run_id + " streaming");
      } else if (st.status === "error") {
        if (S.launchMode !== "error") setLaunch("error", st.error || "live run failed");
      } else if (S.launchMode === "sending" || S.launchMode === "running") {
        // server finished while the page still shows launching/running (the
        // done event may never have arrived): unblock the button NOW —
        // waiting for the replay to catch up at 1× would look like a stuck UI
        setLaunch("again", "server run finished — page catching up (REPLAY)");
      }
    }).catch(function () { /* server briefly unreachable; retry next tick */ });
  }

  /* ------------------------------------------------------------------ */
  /* form + boot                                                         */
  /* ------------------------------------------------------------------ */
  var OVR = ["agents.count", "llm.slots", "llm.input_tokens_per_s",
    "llm.output_tokens_per_s", "db.topology", "db.storage.backend",
    "db.compute_nodes", "db.storage_nodes", "sim.until"];

  function loadScenarios() {
    fetch("/live/scenarios").then(function (r) { return r.json(); }).then(function (d) {
      var sel = $("scenario");
      sel.innerHTML = "";
      (d.scenarios || []).forEach(function (s) {
        var o = document.createElement("option");
        o.value = s.path;
        o.textContent = s.path + (s.until ? " · until " + s.until : "");
        sel.appendChild(o);
      });
      if (!sel.children.length) {
        var o2 = document.createElement("option");
        o2.value = ""; o2.textContent = "no scenarios found";
        sel.appendChild(o2); return;
      }
      var want = ["smoke.yaml", "sanity/sanity_single.yaml", "agents_smoke.yaml"];
      for (var i = 0; i < want.length; i++) {
        var opt = sel.querySelector('option[value="' + want[i] + '"]');
        if (opt) { sel.value = want[i]; break; }
      }
    });
  }

  $("runform").addEventListener("submit", function (ev) {
    ev.preventDefault();
    var scenario = $("scenario").value;
    if (!scenario) {   // picker still loading / no scenarios: say so locally
      setLaunch("error", "pick a scenario first (the picker is still loading?)");
      return;
    }
    var overrides = {};
    OVR.forEach(function (k) {
      var v = $(k).value;
      if (v !== "") overrides[k] = v;
    });
    setLaunch("sending", "contacting server…");
    fetch("/live/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenario: scenario, overrides: overrides }),
    }).then(function (r) {
      return r.json().then(function (j) { return { code: r.status, j: j }; });
    }).then(function (res) {
      if (res.code === 202) {
        // the new run's init event reseats the scene/queue/counters and
        // flips the button to RUNNING; until then it stays LAUNCHING…
        if (es) { es.close(); es = null; }
        connectStream();
        setLaunch("sending", "launched " + (res.j.run_id || "run") +
          " · adopting stream…");
      } else {
        // 409 (one run at a time) / 422 (bad scenario or override): inline,
        // next to the button, and clickable again
        setLaunch("error", (res.j && res.j.detail) || ("HTTP " + res.code));
      }
    }).catch(function (err) {
      setLaunch("error", "launch request failed: " + err);
    });
  });

  $("speed").addEventListener("click", function (ev) {
    var b = ev.target.closest("button"); if (!b) return;
    S.speed = +b.getAttribute("data-s");
    [].forEach.call($("speed").children, function (c) { c.classList.remove("on"); });
    b.classList.add("on");
  });

  $("wavetoggle").addEventListener("click", function () {
    WV.open = !WV.open;
    $("wavepanel").classList.toggle("collapsed", !WV.open);
    document.body.classList.toggle("wave-open", WV.open);
    this.textContent = WV.open ? "▾ hide" : "▸ show";
    if (WV.open) renderWave();
  });

  /* standby scene: the stage must never read as "broken/stuck" before the
     first run — draw the frame + an explicit waiting message at t=0. */
  function renderStandby() {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    var grid = el("g", { class: "gridlines" }, svg);
    for (var gx = 0; gx <= 1300; gx += 65) el("line", { x1: gx, y1: 0, x2: gx, y2: 720 }, grid);
    for (var gy = 0; gy <= 720; gy += 60) el("line", { x1: 0, y1: gy, x2: 1300, y2: gy }, grid);
    txt(svg, "frame-note", 16, 24, "live topology — agent ⇄ llm ⇄ db");
    txt(svg, "standby", 650, 350, "no live run yet", "middle");
    txt(svg, "standby-sub", 650, 374,
      "pick a scenario · press launch · the scene rebuilds from the run config",
      "middle");
  }

  loadScenarios();
  renderStandby();
  document.body.classList.add("wave-open");   // panel starts open
  pollState();
  setInterval(pollState, STATE_POLL_MS);
})();
