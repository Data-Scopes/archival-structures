"""Step 23a: the user's folio-crop page (UA-061).  Run from the AS root:
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/build_folio_page.py
Reads folio_sample.tsv; writes folio_items.json and folio_page.html in this directory. Crops (NA IIIF, network) are
cached in iiif_cache/. The page is blind: scan id, inventory and crop only (no HTR text, no page-number text, no flags).
Crop band: page top to max(32% of page height, bottom of a Laypa page-number line in the top half + 40 px), at most 50%."""
import csv, gzip, json, os, sys
from pathlib import Path
sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools")
import checkpage, handprint

D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
LAYER = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
KEY = "as_phase17_23a_folio_v1"
CACHE = str(D / "iiif_cache")
csv.field_size_limit(10**9)


def tsv(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


items = list(csv.DictReader(open(D / "folio_sample.tsv"), delimiter="\t"))
assert len(items) == 240
sizes, pn_bottom = {}, {}
for inv in sorted({it["inv"] for it in items}):
    for s in tsv(LAYER / "scans" / f"{inv}.scans.tsv.gz"):
        if s["page_w"] and s["page_h"]:
            sizes[s["scan_id"]] = (int(float(s["page_w"])), int(float(s["page_h"])))
    tagged = {(m["scan_id"], m["region_id"]) for m in tsv(LAYER / "regions_meta" / f"{inv}.tsv.gz") if m["region_tag"] == "page-number"}
    for l in tsv(LAYER / "lines" / f"{inv}.lines.tsv.gz"):
        if (l["scan_id"], l["region_id"]) in tagged and l["y"]:
            pn_bottom.setdefault(l["scan_id"], []).append(int(float(l["y"])) + int(float(l["h"])))
need = sorted({it["scan_id"] for it in items})
bases = handprint.na_iiif_bases(need, CACHE)
img = {s: handprint.image_size(bases[s], CACHE, s) for s in need if s in bases}
out = []
for it in items:
    s = it["scan_id"]
    c = zoom = None
    if s in bases and img.get(s) and s in sizes:
        w, h = sizes[s]
        y1 = int(0.32 * h)
        y1 = min(int(0.5 * h), max([y1] + [b + 40 for b in pn_bottom.get(s, []) if b <= 0.5 * h]))
        c = handprint.crop(bases[s], (0, 0, w, y1), sizes[s], img[s], pad=(0, 0), display_w=1000, full_w=2400)
        if c:  # for the in-page zoom: IIIF base, image size and the crop's height in image pixels (the crop spans the full width)
            zoom = dict(base=bases[s], iw=img[s][0], ih=img[s][1], ch=min(round(y1 * img[s][0] / w), img[s][1]))
    out.append(dict(id=it["item_id"], inv=it["inv"], scan=s, scan_num=int(it["scan_num"]), side_layout=it["side_layout"],
                    url=c["url"] if c else None, full=c["full"] if c else None, zoom=zoom))
json.dump(out, open(D / "folio_items.json", "w"), indent=1, ensure_ascii=False)
print(len(out), "items;", sum(1 for o in out if o["url"] is None), "without crop")

CSS = checkpage.BASE_CSS + """
.top{position:sticky;top:0;background:var(--bg);padding:6px 0;border-bottom:1px solid var(--line);z-index:5}
.top button,.nav button{margin:2px 4px 2px 0;padding:3px 10px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
input[type=text]{font:inherit;padding:3px 6px;border:1px solid var(--line);border-radius:4px;background:var(--card);color:var(--fg)}
#num{font-size:1.4rem;width:8em}.crop img{max-width:100%;height:auto;display:block;margin:6px 0;background:rgba(127,127,127,.12)}
.vp{position:relative;overflow:hidden;width:100%;margin:6px 0 2px;background:rgba(127,127,127,.12);cursor:grab;touch-action:none;user-select:none}
.vp.drag{cursor:grabbing}.vp img{position:absolute;left:0;top:0;max-width:none;pointer-events:none}
.vpbar{font-size:.9rem;color:var(--muted)}.vpbar button{margin:2px 4px 2px 0;padding:1px 8px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
.row{margin:6px 0}.row label{display:inline-block;min-width:15em;color:var(--muted)}.st{font-weight:600}
dl dt{font-weight:600;margin-top:6px}dl dd{margin:0 0 0 1em}details{margin:8px 0}
"""
JS = r"""
const store = new CheckStore("%KEY%");
let cur = 0;
for (let i = 0; i < ITEMS.length; i++) if (!store.get(ITEMS[i].id).status) { cur = i; break; }
function stOf(it){ return store.get(it.id).status || ""; }
function answered(){ return ITEMS.filter(it => stOf(it)).length; }
function render(){
  const it = ITEMS[cur], s = store.get(it.id);
  const crop = it.zoom ? `<div class="vp" id="vp"><img id="lp" alt=""><img id="lc" alt="top of ${esc(it.scan)}" src="${esc(it.url)}"><img id="lh" alt=""></div>
    <div class="vpbar"><button type="button" id="zin" title="zoom in">+</button><button type="button" id="zout" title="zoom out">&minus;</button><button type="button" id="ztop">top of page</button><button type="button" id="zpage">whole page</button>
      wheel or pinch zooms, drag pans, double-click resets &nbsp; <a href="${esc(it.full)}" target="_blank" rel="noopener">larger crop</a> <span id="vpmsg"></span></div>`
    : it.url ? `<div class="crop"><a href="${esc(it.full)}" target="_blank" rel="noopener" title="larger"><img alt="top of ${esc(it.scan)}" src="${esc(it.url)}" onerror="this.replaceWith(Object.assign(document.createElement('div'),{textContent:'image not available on the server; open the scan link'}))"></a></div>`
    : `<div class="crop">no crop; open the scan link</div>`;
  document.getElementById("item").innerHTML = `<div class="card"><b>${cur+1}/${ITEMS.length}</b> &nbsp; inventory ${esc(it.inv)}
    ${scanLine([it.scan])}${crop}
    <div class="row"><label>folio number, front sheet, top right</label><input id="num" type="text" autocomplete="off" value="${esc(s.number||"")}">
      <span class="st">${esc(s.status ? "saved: " + s.status : "")}</span></div>
    <div class="row"><label>left page number (optional)</label><input id="left" type="text" size="10" value="${esc(s.left_number||"")}"></div>
    <div class="row"><label>number on a sheet beneath (optional)</label><input id="beneath" type="text" size="10" value="${esc(s.beneath_number||"")}"></div>
    <div class="row"><label>note (optional)</label><input id="note" type="text" size="50" value="${esc(s.note||"")}"></div>
    <div class="nav"><button type="button" id="prev">&larr; previous</button><button type="button" id="next">next &rarr;</button>
      <button type="button" id="bn">no number (n)</button><button type="button" id="bu">unreadable (u)</button></div></div>`;
  document.getElementById("prog").textContent = answered() + " / " + ITEMS.length + " answered";
  for (let k = 1; k <= 3; k++) if (ITEMS[cur+k] && ITEMS[cur+k].url) (new Image()).src = ITEMS[cur+k].url;
  const f = document.getElementById("num"); f.focus(); f.setSelectionRange(f.value.length, f.value.length);
  if (it.zoom) initZoom(it.zoom, f);
  document.getElementById("prev").onclick = () => go(-1); document.getElementById("next").onclick = () => go(1);
  document.getElementById("bn").onclick = () => mark("none"); document.getElementById("bu").onclick = () => mark("unreadable");
  f.addEventListener("input", () => { const v = f.value.trim(); store.set(it.id, v ? {status: "number", number: v} : {number: "", status: (store.get(it.id).status === "number" ? "" : store.get(it.id).status)}); upd(); });
  for (const [id, key] of [["left","left_number"],["beneath","beneath_number"],["note","note"]])
    document.getElementById(id).addEventListener("input", e => store.set(it.id, {[key]: e.target.value}));
}
function upd(){ const it = ITEMS[cur]; const s = store.get(it.id); document.querySelector(".st").textContent = s.status ? "saved: " + s.status : "";
  document.getElementById("prog").textContent = answered() + " / " + ITEMS.length + " answered"; }
// In-page zoom. The view is (x, y) = image pixel at the viewport's top left and s = screen px per image px. Three layers:
// the whole page at 1000 px (loaded on the first move), the crop, and a sharp IIIF region of the current view, fetched
// 250 ms after the view stops changing (never wider than the region itself, so the server never upscales).
let ZT = 0;
function initZoom(z, f){
  const vp = document.getElementById("vp"), lp = document.getElementById("lp"), lc = document.getElementById("lc"), lh = document.getElementById("lh");
  const msg = document.getElementById("vpmsg"), dpr = window.devicePixelRatio || 1, token = ++ZT;
  const W = vp.clientWidth, H = Math.round(Math.min(W * z.ch / z.iw, window.innerHeight * 0.7));
  vp.style.height = H + "px";
  const sMin = Math.min(W / z.iw, H / z.ih), sMax = 3, s0 = W / z.iw;
  let V = {x: 0, y: 0, s: s0}, hiReg = null, timer = 0;
  lc.onerror = () => { msg.textContent = "image not available on the server; open the scan link"; };
  const place = (el, r) => { el.style.left = (r[0] - V.x) * V.s + "px"; el.style.top = (r[1] - V.y) * V.s + "px";
                             el.style.width = r[2] * V.s + "px"; el.style.height = r[3] * V.s + "px"; };
  function clamp(){ V.s = Math.max(sMin, Math.min(sMax, V.s)); const w = W / V.s, h = H / V.s;
    V.x = w >= z.iw ? (z.iw - w) / 2 : Math.max(0, Math.min(z.iw - w, V.x));
    V.y = h >= z.ih ? (z.ih - h) / 2 : Math.max(0, Math.min(z.ih - h, V.y)); }
  function apply(){
    clamp();
    if (!lp.src && (V.s < s0 * 0.999 || V.y + H / V.s > z.ch || V.y > 0)) lp.src = z.base + "/full/1000,/0/default.jpg";
    place(lp, [0, 0, z.iw, z.ih]); place(lc, [0, 0, z.iw, z.ch]); if (hiReg) place(lh, hiReg);
    clearTimeout(timer); timer = setTimeout(sharpen, 250);
  }
  function sharpen(){
    const x0 = Math.max(0, Math.floor(V.x)), y0 = Math.max(0, Math.floor(V.y));
    const x1 = Math.min(z.iw, Math.ceil(V.x + W / V.s)), y1 = Math.min(z.ih, Math.ceil(V.y + H / V.s)), w = x1 - x0, h = y1 - y0;
    if (w < 1 || h < 1) return;
    const need = V.s * dpr;  // device px per image px
    if (y1 <= z.ch && need <= 1000 / z.iw * 1.05) return;  // the crop is already sharp enough here
    const rw = Math.max(1, Math.min(w, Math.round(w * need), 3000)), r = [x0, y0, w, h], im = new Image();
    im.onload = () => { if (token !== ZT) return; lh.src = im.src; hiReg = r; place(lh, r); };
    im.src = z.base + "/" + x0 + "," + y0 + "," + w + "," + h + "/" + rw + ",/0/default.jpg";
  }
  function zoomAt(px, py, k){ const ix = V.x + px / V.s, iy = V.y + py / V.s; V.s = Math.max(sMin, Math.min(sMax, V.s * k));
    V.x = ix - px / V.s; V.y = iy - py / V.s; apply(); }
  const top = () => { V = {x: 0, y: 0, s: s0}; apply(); };
  vp.addEventListener("wheel", e => { e.preventDefault(); const b = vp.getBoundingClientRect();
    const dy = e.deltaY * (e.deltaMode === 1 ? 40 : e.deltaMode === 2 ? 800 : 1);
    zoomAt(e.clientX - b.left, e.clientY - b.top, Math.exp(-dy * (e.ctrlKey ? 0.01 : 0.002))); }, {passive: false});
  let drag = null;
  vp.addEventListener("mousedown", e => e.preventDefault());  // keep the cursor in the number field
  vp.addEventListener("pointerdown", e => { e.preventDefault(); drag = {px: e.clientX, py: e.clientY}; vp.setPointerCapture(e.pointerId); vp.classList.add("drag"); });
  vp.addEventListener("pointermove", e => { if (!drag) return; V.x -= (e.clientX - drag.px) / V.s; V.y -= (e.clientY - drag.py) / V.s;
    drag = {px: e.clientX, py: e.clientY}; apply(); });
  const end = () => { drag = null; vp.classList.remove("drag"); };
  vp.addEventListener("pointerup", end); vp.addEventListener("pointercancel", end);
  vp.addEventListener("dblclick", e => { e.preventDefault(); top(); f.focus(); });
  const btn = (id, fn) => { const b = document.getElementById(id); b.addEventListener("mousedown", e => e.preventDefault()); b.onclick = () => { fn(); f.focus(); }; };
  btn("zin", () => zoomAt(W / 2, H / 2, 1.6)); btn("zout", () => zoomAt(W / 2, H / 2, 1 / 1.6)); btn("ztop", top);
  btn("zpage", () => { V = {x: 0, y: 0, s: sMin}; apply(); });
  apply();
}
function go(d){ cur = Math.max(0, Math.min(ITEMS.length - 1, cur + d)); render(); }
function mark(st){ store.set(ITEMS[cur].id, {status: st, number: ""}); go(1); }
document.addEventListener("keydown", e => {
  const t = e.target, inNum = t && t.id === "num";
  if (e.key === "Enter" && inNum) { e.preventDefault(); if (stOf(ITEMS[cur])) go(1); else { document.getElementById("hint").textContent = "type a number, or press n / u"; } }
  else if (inNum && (e.key === "n" || e.key === "N")) { e.preventDefault(); mark("none"); }
  else if (inNum && (e.key === "u" || e.key === "U")) { e.preventDefault(); mark("unreadable"); }
  else if (inNum && e.key === "ArrowLeft") { e.preventDefault(); go(-1); }
  else if (inNum && e.key === "ArrowRight") { e.preventDefault(); go(1); }
});
document.getElementById("exp").onclick = () => store.exportJSON("folio_verdicts.json", "%KEY%", ITEMS.map(it => ({id: it.id, inv: it.inv, scan_id: it.scan, scan_num: it.scan_num, side_layout: it.side_layout, ...store.get(it.id)})));
render();
""".replace("%KEY%", KEY)

html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>23a folio crops</title><style>{CSS}</style></head><body><main>
<h1>Step 23a: read the folio number at the top of 240 scans</h1>
<p>Each item is a crop of the top of one scan (Nationaal Archief image server; internet needed). Type the <b>folio number written on the
front sheet in the top right corner</b>. On a two-page scan (an opening) that is the <b>right-hand page</b>. Zoom in on the crop with the
mouse wheel or a pinch and drag to pan (sharper detail loads as you zoom); <i>whole page</i> shows the full scan if the number sits lower. About 30 to 40 minutes in all.</p>
<dl>
<dt>Number</dt><dd>The folio number as written (e.g. <code>288</code>, <code>1036</code>). Press <code>Enter</code> to save and go on. Digits only, as written; add a letter if one is written (<code>288a</code>).</dd>
<dt>No number (<code>n</code>)</dt><dd>The sheet is clearly there but carries no number (a cover, a blank sheet, a page with a number only on the left).</dd>
<dt>Unreadable (<code>u</code>)</dt><dd>A number seems to be there but you cannot read it, or the crop does not show the page top.</dd>
<dt>Left page number</dt><dd>Optional. On an opening whose left page is also numbered, the number at the top of the left page.</dd>
<dt>Number on a sheet beneath</dt><dd>Optional. A smaller sheet lies on a larger one and the larger sheet's number shows: type that number here, and the number of the front sheet above.</dd>
<dt>Keys</dt><dd><code>Enter</code> save and next; <code>n</code> no number; <code>u</code> unreadable; <code>&larr;</code> and <code>&rarr;</code> previous and next item.</dd>
</dl>
<details><summary>Export</summary>When done (or to pause), press Export and save <code>folio_verdicts.json</code> in
<code>reports/data/phase-17-voc-rules-v1/step-23a/</code>. Answers are also kept in this browser.</details>
<div class="top"><span id="prog"></span> &nbsp; <button id="exp" type="button">Export</button> <span id="hint" class="ids"></span></div>
<div id="item"></div></main>
<script>{checkpage.HELPERS_JS}
const ITEMS = {json.dumps(out, ensure_ascii=False)};
{JS}</script></body></html>"""
(D / "folio_page.html").write_text(html)
for p in (D / "folio_page.html", D / "folio_items.json", D / "iiif_cache"):
    os.chmod(p, os.stat(p).st_mode | 0o020)
print("wrote folio_page.html")
