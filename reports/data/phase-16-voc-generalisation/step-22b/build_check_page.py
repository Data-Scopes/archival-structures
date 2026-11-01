"""Step 22b: check page for the miss reads classed `gold_error` (up to 20; here all of them).  Run from the AS root:
    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/build_check_page.py
Writes check_items.json and check_page.html in this directory. Question per item: which scan carries folio N?"""
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
json.dump(items, open(D / "check_items.json", "w"), indent=1, ensure_ascii=False)

CSS = checkpage.BASE_CSS + """
.btns button{margin:2px 4px 2px 0;padding:3px 10px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
.btns button.on{background:var(--accent);color:#fff}textarea{width:100%;box-sizing:border-box;font:inherit}
table{border-collapse:collapse;font-size:.88rem}td{padding:2px 8px;border-bottom:1px solid var(--line)}tr.gold td{background:color-mix(in srgb,var(--uns) 22%,transparent)}
"""
JS = r"""
const store = new CheckStore("%KEY%");
const OPTS = ["gold scan", "predicted scan", "another scan", "cannot tell"];
function render(){
  document.getElementById("items").innerHTML = ITEMS.map((it,n) => { const s = store.get(it.id);
    return `<div class="card"><b>${n+1}/${ITEMS.length}</b> &nbsp; inventory ${esc(it.inv)} ${idLine([["read_id",it.id],["folio",it.folio]])}
    ${scanLine([it.gold_scan, it.pred_scan])}
    <table>${it.ctx.map(c => `<tr class="${c.tag}"><td>${scanHtml(c.scan)}</td><td>${esc(c.tag)}</td><td>${esc(c.numbers.join(" | ") || "-")}</td></tr>`).join("")}</table>
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
The table shows the page numbers the text layer read on the scans around the gold scan (yellow) and around the scan the map predicted.
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
