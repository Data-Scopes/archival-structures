"""Step 22b: check page for the miss reads classed `gold_error` (up to 20; here all of them).  Run from the AS root:
    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/build_check_page.py
Writes check_items.json and check_page.html in this directory; image crops (NA IIIF, network) are cached in iiif_cache/. Question per item: which scan carries folio N?"""
import csv, json, sys
from pathlib import Path
sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools")
sys.path.insert(0, ".")
import checkpage
from archival_structures.analysis.index_links import read_volume_inputs

D = Path("reports/data/phase-16-voc-generalisation/step-22b")
LAYER = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
KEY = "as_phase16_22b_gold_errors_v1"
rows = [r for r in csv.DictReader(open(D / "misses_read.tsv"), delimiter="\t") if r["category"] == "gold_error"][:20]
items = []
for r in rows:
    inv = r["inv"]
    v = read_volume_inputs(LAYER, inv, number_tags=("page-number",))
    g = int(r["gold_scan"]); p = int(r["pred_scan"])
    sid = lambda n: f"NL-HaNA_1.04.02_{inv}_{n:04d}"
    ctx = []
    for n in sorted(set(range(g - 3, g + 4)) | set(range(p - 1, p + 2))):
        txt = [t for s in v.sides if s[0] == sid(n) for t in v.numbers.get(s, [])]
        ctx.append(dict(scan=sid(n), numbers=txt, tag="gold" if n == g else "predicted" if n == p else ""))
    items.append(dict(id=r["read_id"], inv=inv, folio=r["folio"], gold_scan=sid(g), pred_scan=sid(p), note=r["note"], ctx=ctx))

# --- image crops (added 2026-11-01 for UA-059, at the user's request): NA IIIF crops of the top of the page, where VOC
# folio numbers are written, for the scan before the gold, the gold scan, the scan after it, and the predicted scan.
# The band runs from the page top to 32% of its height, extended down to the lowest line of a Laypa `page-number`
# region in the top half (capped at 50%). Boxes are PageXML pixels. Display aids only: no gold file changes.
import gzip
import handprint
CACHE = str(D / "iiif_cache")
def tsv(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))
sizes, pn_bottom = {}, {}
for inv in sorted({it["inv"] for it in items}):
    for s in tsv(LAYER / "scans" / f"{inv}.scans.tsv.gz"):
        if s["page_w"] and s["page_h"]:
            sizes[s["scan_id"]] = (int(float(s["page_w"])), int(float(s["page_h"])))
    tagged = {(m["scan_id"], m["region_id"]) for m in tsv(LAYER / "regions_meta" / f"{inv}.tsv.gz") if m["region_tag"] == "page-number"}
    for l in tsv(LAYER / "lines" / f"{inv}.lines.tsv.gz"):
        if (l["scan_id"], l["region_id"]) in tagged and l["y"]:
            b = int(float(l["y"])) + int(float(l["h"]))
            pn_bottom.setdefault(l["scan_id"], []).append(b)
def crop_scans(it):
    g = int(it["gold_scan"][-4:]); p = int(it["pred_scan"][-4:]); sid = lambda n: f"NL-HaNA_1.04.02_{it['inv']}_{n:04d}"
    out = [(sid(g - 1), "scan before the gold"), (sid(g), "gold scan"), (sid(g + 1), "scan after the gold")]
    if p not in (g - 1, g, g + 1):
        out.append((sid(p), "predicted scan"))
    else:
        out = [(s, lab + " = predicted scan" if s == sid(p) else lab) for s, lab in out]
    return out
need = sorted({s for it in items for s, _ in crop_scans(it)})
bases = handprint.na_iiif_bases(need, CACHE)
img = {s: handprint.image_size(bases[s], CACHE, s) for s in need if s in bases}
for it in items:
    crops = []
    for s, lab in crop_scans(it):
        c = None
        if s in bases and img.get(s) and s in sizes:
            w, h = sizes[s]
            y1 = int(0.32 * h)
            y1 = min(int(0.5 * h), max([y1] + [b + 40 for b in pn_bottom.get(s, []) if b <= 0.5 * h]))
            c = handprint.crop(bases[s], (0, 0, w, y1), sizes[s], img[s], pad=(0, 0), display_w=1000, full_w=2400)
        crops.append(dict(c, scan=s, label=lab) if c else dict(scan=s, label=lab, url=None))
    it["crops"] = crops
json.dump(items, open(D / "check_items.json", "w"), indent=1, ensure_ascii=False)

CSS = checkpage.BASE_CSS + """
.btns button{margin:2px 4px 2px 0;padding:3px 10px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
.btns button.on{background:var(--accent);color:#fff}textarea{width:100%;box-sizing:border-box;font:inherit}
.crop{margin:6px 0;padding:6px;border:1px solid var(--line);border-radius:6px}.crop img{max-width:100%;height:auto;display:block;margin:4px 0;background:rgba(127,127,127,.12)}.crop .h{font-size:.85rem}
.crop.gold{border-color:var(--uns);border-width:2px}
table{border-collapse:collapse;font-size:.88rem}td{padding:2px 8px;border-bottom:1px solid var(--line)}tr.gold td{background:color-mix(in srgb,var(--uns) 22%,transparent)}
"""
JS = r"""
const store = new CheckStore("%KEY%");
const OPTS = ["gold scan", "predicted scan", "another scan", "cannot tell"];
function cropHtml(c){
  const head = `<div class="h">${esc(c.label)}: ${scanHtml(c.scan)}${c.url ? ` · <a href="${esc(c.full)}" target="_blank" rel="noopener">larger</a>` : ""}</div>`;
  if (!c.url) return `<div class="crop">${head}<div>no crop available; open the scan link</div></div>`;
  return `<div class="crop${c.label.startsWith("gold") ? " gold" : ""}">${head}
    <img loading="lazy" alt="top of ${esc(c.scan)}" src="${esc(c.url)}" onerror="this.replaceWith(Object.assign(document.createElement('div'),{textContent:'image not available on the server; open the scan link'}))"></div>`; }
function render(){
  document.getElementById("items").innerHTML = ITEMS.map((it,n) => { const s = store.get(it.id);
    return `<div class="card"><b>${n+1}/${ITEMS.length}</b> &nbsp; inventory ${esc(it.inv)} ${idLine([["read_id",it.id],["folio",it.folio]])}
    ${scanLine([it.gold_scan, it.pred_scan])}
    ${it.crops.map(cropHtml).join("")}
    <details><summary>Page numbers the text layer read on nearby scans</summary>
    <table>${it.ctx.map(c => `<tr class="${c.tag}"><td>${scanHtml(c.scan)}</td><td>${esc(c.tag)}</td><td>${esc(c.numbers.join(" | ") || "-")}</td></tr>`).join("")}</table></details>
    <div class="q"><b>Which scan carries folio ${esc(it.folio)} (the page with that number, or the opening that starts there)?</b>
    <span class="btns">${OPTS.map(o => `<button type="button" class="${s.v===o?'on':''}" data-id="${it.id}" data-v="${o}">${o}</button>`).join("")}</span></div>
    <input type="text" size="16" data-scan="${it.id}" placeholder="scan number if another" value="${esc(s.scan||"")}">
    <textarea rows="2" data-note="${it.id}" placeholder="note (optional)">${esc(s.note||"")}</textarea></div>`; }).join("");
  document.getElementById("prog").textContent = ITEMS.filter(it => store.get(it.id).v).length + " / " + ITEMS.length + " answered";
}
document.addEventListener("click", e => { const b = e.target.closest("button[data-v]"); if (b) { store.set(b.dataset.id, {v: b.dataset.v}); render(); } });
document.addEventListener("input", e => { const t = e.target;
  if (t.dataset.note) store.set(t.dataset.note, {note: t.value}); if (t.dataset.scan) store.set(t.dataset.scan, {scan: t.value}); });
document.getElementById("exp").onclick = () => store.exportJSON("check_verdicts.json", KEY, ITEMS.map(it => ({id: it.id, inv: it.inv, folio: it.folio, ...store.get(it.id)})));
render();
""".replace("%KEY%", KEY)

html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>22b gold check</title><style>{CSS}</style></head><body><main>
<h1>Step 22b: which scan carries the folio?</h1>
<p>Each item is a Generale Missiven anchor (folio N at a scan) where the folio map disagreed with the gold and the printed numbers
near the gold scan suggested the gold itself might be off. For every item, open the scans and decide which scan carries folio N.
Each item shows image crops of the top of the page (where VOC folio numbers are written) for the scan before the gold, the gold scan (yellow frame), the scan after it and the scan the map predicted; crops come from the Nationaal Archief image server and need an internet connection. If the number sits lower on the page, use <i>larger</i> or the scan link. The fold-out lists the page numbers the text layer read on nearby scans (gold row yellow).
Choose <i>gold scan</i> when the gold is right, <i>predicted scan</i> when the map is right, <i>another scan</i> (and type its number),
or <i>cannot tell</i>. The result only says how much of the miss rate is gold noise; it blocks nothing. When done, press Export and
save <code>check_verdicts.json</code> in <code>reports/data/phase-16-voc-generalisation/step-22b/</code>.</p>
<div class="top"><span id="prog"></span> &nbsp; <button id="exp" type="button">Export</button></div>
<div id="items"></div></main>
<script>{checkpage.HELPERS_JS}
const ITEMS = {json.dumps(items, ensure_ascii=False)};
const KEY = "{KEY}";
{JS}</script></body></html>"""
(D / "check_page.html").write_text(html)
print(len(items), "items")
