// Generate a KSTD-styled (kstd.thriving.dev) Excalidraw scene for the WorkTask
// Kafka Streams topology by instantiating the official KSTD component library.
//
// Setup: download the library once:
//   curl -sL https://raw.githubusercontent.com/thriving-dev/kafka-streams-topology-design/HEAD/lib/kafka-streams-topology-design_v1.0.0.excalidrawlib -o /tmp/kstd.excalidrawlib
// Run:
//   node docs/architecture/generate-kstd-topology.mjs /tmp/kstd.excalidrawlib docs/architecture/worktask-streams-topology-kstd.excalidraw
//
// KSTD conventions: header (title) upper-left, legend upper-right, left -> right data flow.
// Topics use the Apache Kafka(R) logo variant ("Topic Default"), drawn OUTSIDE the
// sub-topology boxes; source/sink processor nodes are KStream (KS) nodes INSIDE the boxes;
// each topic carries a "Key-Value Record Typed" above its connecting arrow. The read model
// (PostgreSQL) and the read-only GraphQL query API sit OUTSIDE the topology, to the right
// (the GraphQL API reads the DB via a dashed RPC/Other access arrow).
import fs from "node:fs";

const libPath = process.argv[2] || "/tmp/kstd.excalidrawlib";
const outPath = process.argv[3] || "docs/architecture/worktask-streams-topology-kstd.excalidraw";

const lib = JSON.parse(fs.readFileSync(libPath, "utf8"));
const items = lib.libraryItems || lib.library || [];
const byName = (n) => {
  const it = items.find((i) => i.name === n);
  if (!it) throw new Error("library item not found: " + n);
  return it;
};

let counter = 0;
const newId = () => `wtk-${(counter++).toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

const out = [];

// Instantiate a library item with its top-left corner at (tx, ty).
// opts: { resize:{w,h}, setText:[[match,replacement],...], moveLabelTo:[dx,dy] }
// Returns a handle with bounding box + edge-midpoint anchors L/R/T/B and center.
function place(name, tx, ty, opts = {}) {
  const item = byName(name);
  const els = JSON.parse(JSON.stringify(item.elements));
  let minx = Infinity, miny = Infinity;
  for (const e of els) {
    if (typeof e.x === "number") { minx = Math.min(minx, e.x); miny = Math.min(miny, e.y); }
  }
  const dx = tx - minx, dy = ty - miny;

  const idMap = {}, gidMap = {};
  for (const e of els) idMap[e.id] = newId();
  const instGroup = newId();

  for (const e of els) {
    if (typeof e.x === "number") { e.x += dx; e.y += dy; }
    e.id = idMap[e.id];
    if (e.containerId && idMap[e.containerId]) e.containerId = idMap[e.containerId];
    if (Array.isArray(e.boundElements))
      e.boundElements = e.boundElements.map((b) => (idMap[b.id] ? { ...b, id: idMap[b.id] } : b));
    if (e.startBinding && idMap[e.startBinding.elementId])
      e.startBinding = { ...e.startBinding, elementId: idMap[e.startBinding.elementId] };
    if (e.endBinding && idMap[e.endBinding.elementId])
      e.endBinding = { ...e.endBinding, elementId: idMap[e.endBinding.elementId] };
    e.groupIds = (e.groupIds || []).map((g) => (gidMap[g] || (gidMap[g] = newId())));
    e.groupIds.push(instGroup);
  }

  if (opts.resize) {
    let cont = null, area = -1;
    for (const e of els) {
      if (e.type === "rectangle") {
        const a = (e.width || 0) * (e.height || 0);
        if (a > area) { area = a; cont = e; }
      }
    }
    if (cont) { cont.width = opts.resize.w; cont.height = opts.resize.h; }
    if (opts.moveLabelTo) {
      const t = els.find((e) => e.type === "text");
      if (t && cont) { t.x = cont.x + opts.moveLabelTo[0]; t.y = cont.y + opts.moveLabelTo[1]; }
    }
  }

  if (opts.setText) {
    for (const [match, repl] of opts.setText) {
      const t = els.find((e) => e.type === "text" && (e.text || "").includes(match));
      if (t) { t.text = repl; t.originalText = repl; }
    }
  }

  const mainRect = () => {
    let c = null, area = -1;
    for (const e of els) if (e.type === "rectangle") { const a = (e.width || 0) * (e.height || 0); if (a > area) { area = a; c = e; } }
    return c;
  };
  const mainText = () => els.find((e) => e.type === "text" && (e.text || "").length);

  // Trim a glyph's text box to its content so edge anchors land on the visible text
  // (Topic Default reserves a wide text box; short labels otherwise leave a right-edge gap).
  if (opts.fitText) {
    const t = mainText();
    if (t) {
      const maxLine = Math.max(...t.text.split("\n").map((s) => s.length));
      t.width = Math.min(t.width, maxLine * 9 + 6);
    }
  }
  // Center the primary label inside the (resized) main rectangle.
  if (opts.centerLabel) {
    const cont = mainRect(), t = mainText();
    if (cont && t) {
      const lines = t.text.split("\n").length, th = t.fontSize * 1.25 * lines;
      t.textAlign = "center"; t.verticalAlign = "middle"; t.width = cont.width - 12; t.height = th;
      t.x = cont.x + 6; t.y = cont.y + (cont.height - th) / 2;
    }
  }
  if (opts.dashedBorder) { const c = mainRect(); if (c) c.strokeStyle = "dashed"; }
  if (opts.textFont) for (const e of els) if (e.type === "text") e.fontSize = opts.textFont;

  out.push(...els);

  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (const e of els) {
    if (typeof e.x !== "number") continue;
    x0 = Math.min(x0, e.x); y0 = Math.min(y0, e.y);
    x1 = Math.max(x1, e.x + (e.width || 0)); y1 = Math.max(y1, e.y + (e.height || 0));
  }
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
  const handle = { x0, y0, x1, y1, cx, cy, els, L: [x0, cy], R: [x1, cy], T: [cx, y0], B: [cx, y1] };

  // Transparent hit-box covering the whole glyph, grouped with it, so arrows can bind to a
  // single bindable element that moves with the glyph (composite KSTD glyphs have no single rect).
  if (opts.bindable) {
    const r = {
      id: newId(), type: "rectangle", x: x0, y: y0, width: x1 - x0, height: y1 - y0, angle: 0,
      strokeColor: "transparent", backgroundColor: "transparent", fillStyle: "solid", strokeWidth: 1,
      strokeStyle: "solid", roughness: 0, opacity: 100, groupIds: [instGroup], frameId: null,
      roundness: null, seed: counter++, version: 1, versionNonce: counter++, isDeleted: false,
      boundElements: [], updated: 1, link: null, locked: false,
    };
    out.push(r);
    handle.bindEl = r; handle.bindId = r.id;
  }
  return handle;
}

// Set a label's box width (and optionally font) so its text fits neatly; `center` re-centers
// a centered label around its current center.
function fitLabel(h, sub, { width, fontSize, center = false } = {}) {
  const t = h.els.find((e) => e.type === "text" && (e.text || "").includes(sub));
  if (!t) return;
  if (fontSize) t.fontSize = fontSize;
  if (width) { if (center) { const c = t.x + t.width / 2; t.x = c - width / 2; } t.width = width; }
}

function caption(text, x, y, fontSize = 13, color = "#1e1e1e", align = "center", width = 140) {
  out.push({
    id: newId(), type: "text", x, y, width, height: fontSize * 1.25 * text.split("\n").length,
    angle: 0, strokeColor: color, backgroundColor: "transparent", fillStyle: "solid",
    strokeWidth: 1, strokeStyle: "solid", roughness: 1, opacity: 100, groupIds: [], frameId: null,
    roundness: null, seed: counter++, version: 1, versionNonce: counter++, isDeleted: false,
    boundElements: null, updated: 1, link: null, locked: false, text, fontSize, fontFamily: 1,
    textAlign: align, verticalAlign: "top", baseline: fontSize - 4, containerId: null,
    originalText: text, lineHeight: 1.25,
  });
}

// Sharp orthogonal flow arrow between absolute points a and b (NOT elbowed — unbound
// elbowed arrows drop their arrowheads). `both` => double-headed; `dashed` => RPC/Other.
function flowArrow(a, b, opts = {}) {
  const { dashed = false, color = "#1e1e1e", both = false, midX = null, midY = null,
    from = null, to = null } = opts;
  const [ax, ay] = a, [bx, by] = b;
  const near = (p, q) => Math.abs(p - q) < 2;
  const pts = [[0, 0]];
  if (midX != null) {
    pts.push([midX - ax, 0], [midX - ax, by - ay]);
    if (!near(midX, bx)) pts.push([bx - ax, by - ay]);
  } else if (midY != null) {
    pts.push([0, midY - ay], [bx - ax, midY - ay]);
    if (!near(midY, by)) pts.push([bx - ax, by - ay]);
  } else if (near(ay, by)) {
    pts.push([bx - ax, 0]);
  } else if (near(ax, bx)) {
    pts.push([0, by - ay]);
  } else {
    pts.push([bx - ax, 0], [bx - ax, by - ay]);
  }
  let w = 0, h = 0;
  for (const [px, py] of pts) { w = Math.max(w, Math.abs(px)); h = Math.max(h, Math.abs(py)); }
  const id = newId();
  const arrow = {
    id, type: "arrow", x: ax, y: ay, width: w, height: h, angle: 0, strokeColor: color,
    backgroundColor: "transparent", fillStyle: "solid", strokeWidth: 2,
    strokeStyle: dashed ? "dashed" : "solid", roughness: 1, opacity: 100, groupIds: [], frameId: null,
    roundness: null, seed: counter++, version: 1, versionNonce: counter++, isDeleted: false,
    boundElements: null, updated: 1, link: null, locked: false, points: pts, lastCommittedPoint: null,
    startBinding: null, endBinding: null, startArrowhead: both ? "arrow" : null, endArrowhead: "arrow",
    elbowed: false,
  };
  // Real Excalidraw binding so the arrow follows its endpoints when elements move.
  if (from && from.bindId) {
    arrow.startBinding = { elementId: from.bindId, focus: 0, gap: 6 };
    from.bindEl.boundElements.push({ id, type: "arrow" });
  }
  if (to && to.bindId) {
    arrow.endBinding = { elementId: to.bindId, focus: 0, gap: 6 };
    to.bindEl.boundElements.push({ id, type: "arrow" });
  }
  out.push(arrow);
}

const KS = (cx, cy) => place("KStream", cx - 30, cy - 30, { bindable: true });
const kvTyped = (cx, cy, kv, types) =>
  place("Key-Value Record Typed", cx - 53, cy - 31,
    { textFont: 13.5, setText: [["k: userId", kv], ["string, avro", types]] });

// ===========================================================================
// Top band: title (upper-left) + legend (upper-right)
// ===========================================================================
place("Header", 80, 40, {
  setText: [
    ["Kafka Streams Topology Design", "Kafka Streams Topology Design"],
    ["Diagram Title", "WorkTask Service — work.tasks.worktask"],
    ["DD/MM/YYYY", "2026-06-14 - v1   ·   application.id = worktask-streams"],
  ],
});
place("Legend Landscape", 1560, 30, {});

// ===========================================================================
// Topology container (wraps only the Kafka Streams parts; read side is outside, right)
// ===========================================================================
place("Topology", 40, 540, { resize: { w: 1960, h: 520 }, moveLabelTo: [20, 16],
  setText: [["my-topology", "worktask-streams"]] });

// --- source topic (Kafka-logo, outside the sub-topology, far left) ---
const cmdTopic = place("Topic Default", 80, 728, { fitText: true, bindable: true, setText: [["fx.course.v2", "…worktask.command\n(cleanup=delete)"]] });

// --- sub-0: command -> events + materialized state ---
place("Sub-topology", 510, 580, { resize: { w: 750, h: 440 }, moveLabelTo: [16, 12],
  setText: [["sub-0", "sub-0"]] });
caption("command → events + materialized state", 528, 620, 13, "#495057", "left", 460);

const cmdSource = KS(560, 780);
caption("command-source", 500, 818, 11, "#1e1e1e", "center", 120);

const pcCtx = place("Processor Context", 600, 672, { resize: { w: 400, h: 300 }, moveLabelTo: [16, 12],
  setText: [["MyProcessor", "StateTransitionProcessor"]] });
fitLabel(pcCtx, "StateTransitionProcessor", { width: 300 });
const proc = place("process", 763, 733, { bindable: true });   // center (799,780)
const st1 = place("State Store", 650, 870, { bindable: true, setText: [["store-name", "worktask-store"]] });
const st2 = place("State Store", 840, 870, { bindable: true, setText: [["store-name", "subject-active-index"]] });
fitLabel(st1, "worktask-store", { width: 176, fontSize: 14, center: true });
fitLabel(st2, "subject-active-index", { width: 176, fontSize: 14, center: true });

const evtSink = KS(1200, 600);
const cmpSink = KS(1200, 780);
const dltSink = KS(1200, 960);
caption("event-sink", 1140, 636, 11, "#1e1e1e", "center", 120);
caption("compact-sink", 1140, 816, 11, "#1e1e1e", "center", 120);
caption("dead-letter-sink", 1140, 996, 11, "#1e1e1e", "center", 120);

// --- sink topics (Kafka-logo, outside the sub-topology, right) ---
const evtTopic = place("Topic Default", 1350, 549, { fitText: true, bindable: true, setText: [["fx.course.v2", "…worktask.event\n(cleanup=delete)"]] });
const cmpTopic = place("Topic Default", 1350, 729, { fitText: true, bindable: true, setText: [["fx.course.v2", "…worktask.compact\n(cleanup=compact)"]] });
const dltTopic = place("Topic Default", 1350, 909, { fitText: true, bindable: true, setText: [["fx.course.v2", "…worktask.dead-letter\n(cleanup=delete)"]] });

// --- Key-Value Record Typed, above each topic's connecting arrow ---
kvTyped(412, 740, "k: subjectId\nv: Command", "string, avro");    // command -> command-source
kvTyped(1290, 560, "k: subjectId\nv: Event", "string, avro");     // event-sink -> event
kvTyped(1290, 740, "k: workTaskId\nv: WorkTask", "string, avro"); // compact-sink -> compact
kvTyped(1290, 920, "k: subjectId\nv: rawCmd", "string, bytes");   // dead-letter-sink -> dlt

// --- sub-1: read-model sub-topology, to the right of the main topology ---
place("Sub-topology", 1640, 690, { resize: { w: 300, h: 180 }, moveLabelTo: [16, 12],
  setText: [["sub-0", "sub-1"]] });
caption("compact → read model", 1656, 728, 12, "#495057", "left", 280);

const cmpSource = KS(1700, 780);
caption("compact-source", 1640, 818, 11, "#1e1e1e", "center", 120);
const dbsink = place("process", 1794, 733, { bindable: true }); // center (1830,780)
caption("database-sink", 1770, 832, 11, "#1e1e1e", "center", 120);

// --- read side (OUTSIDE the topology, far right): PostgreSQL + GraphQL query API ---
const pg = place("External DB", 2031, 734, { bindable: true, setText: [["ext. DB", "PostgreSQL"]] });
const gql = place("API (alt)", 2205, 738, { resize: { w: 150, h: 84 }, dashedBorder: true,
  centerLabel: true, bindable: true, setText: [["API", "GraphQL\nAPI"]] });
caption("/graphql · read-only query API", 2150, 836, 11, "#1e1e1e", "center", 260);

// --- watermark (bottom-right) ---
place("KSTD Watermark", 1980, 1120, {});

// ===========================================================================
// Flow arrows (solid = topology data flow; dashed = RPC/Other), edge-midpoint anchors
// ===========================================================================
flowArrow(cmdTopic.R, cmdSource.L, { from: cmdTopic, to: cmdSource });
flowArrow(cmdSource.R, proc.L, { from: cmdSource, to: proc });

flowArrow(proc.B, st1.T, { both: true, midX: st1.cx, from: proc, to: st1 });   // process <-> worktask-store
flowArrow(proc.B, st2.T, { both: true, midX: st2.cx, from: proc, to: st2 });   // process <-> subject-active-index

flowArrow(proc.R, evtSink.L, { midX: 1075, from: proc, to: evtSink });
flowArrow(proc.R, cmpSink.L, { from: proc, to: cmpSink });
flowArrow(proc.R, dltSink.L, { midX: 1085, from: proc, to: dltSink });

flowArrow(evtSink.R, evtTopic.L, { from: evtSink, to: evtTopic });
flowArrow(cmpSink.R, cmpTopic.L, { from: cmpSink, to: cmpTopic });
flowArrow(dltSink.R, dltTopic.L, { from: dltSink, to: dltTopic });

flowArrow(cmpTopic.R, cmpSource.L, { from: cmpTopic, to: cmpSource });
flowArrow(cmpSource.R, dbsink.L, { from: cmpSource, to: dbsink });
flowArrow(dbsink.R, pg.L, { from: dbsink, to: pg });          // database-sink -> PostgreSQL
flowArrow(gql.L, pg.R, { dashed: true, from: gql, to: pg });  // GraphQL API -> PostgreSQL (read-only query)

// ===========================================================================
const scene = {
  type: "excalidraw",
  version: 2,
  source: "claude-code-excalidraw-skill + KSTD (kstd.thriving.dev)",
  elements: out,
  appState: { gridSize: 20, viewBackgroundColor: "#ffffff" },
  files: {},
};
fs.writeFileSync(outPath, JSON.stringify(scene, null, 2));
console.log("wrote", outPath, "with", out.length, "elements");
