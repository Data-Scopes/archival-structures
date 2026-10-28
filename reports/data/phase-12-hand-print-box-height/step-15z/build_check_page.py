"""Step 15z: build check_page.html (and titles.tsv). Blind page: for each of the 37 read volumes the inventory number, the EAD
title and its 5 read scans (viewer links) with the first 8 lines of each scan's largest region. No predicted mode, no box
height, no agent label. Each scan also shows an IIIF crop (Nationaal Archief iipsrv, no upscaling: width at most 760 / 2000)
of the bounding box of the same largest region the text comes from, padded by PAD_X/PAD_Y, beside the text; no line boxes are
drawn, and a scan without lines gets a note instead. The IIIF bases come from na_iiif_map.py (na_dims_check.tsv); where the NA image
is r = na_w / xml_w times the PageXML size (status "scaled"), the padded box is multiplied by r, rounded and clamped to the image.
A scan not in the NA list or with a different aspect ratio gets no crop. The user answers "mainly handwritten or printed?" per volume and exports check_verdicts.json.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/build_check_page.py
"""
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools")
import checkpage  # noqa: E402

IIIF = "{base}/{x},{y},{w},{h}/{sw},/0/default.jpg"
PAD_X, PAD_Y, DISPLAY_W, FULL_W = 60, 30, 760, 2000
D = Path("reports/data/phase-12-hand-print-box-height")
L = Path("data/text_layer_v2_dev/15z-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4/lines")
spec = importlib.util.spec_from_file_location("r15w", D / "review/review_15w.py")
r15 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r15)
reads = pd.read_csv(D / "planning/reads_15z.tsv", sep="\t", dtype={"inv": str})
chk = pd.read_csv(D / "step-15z/na_dims_check.tsv", sep="\t", dtype=str).set_index("scan_id")
titles = r15.ead_titles(set(reads.inv))
pd.DataFrame({"inv": list(titles), "title": list(titles.values())}).to_csv(D / "step-15z/titles.tsv", sep="\t", index=False)

items = []
for inv in sorted(set(reads.inv), key=lambda s: int(s)):
    lines = pd.read_csv(L / f"{inv}.lines.tsv.gz", sep="\t", usecols=["scan_id", "region_id", "line_idx", "x", "y", "w", "h", "text"], dtype={"text": str})
    lines["text"] = lines["text"].fillna("")
    scans = []
    for row in reads[reads.inv == inv].sort_values("read_id").itertuples():
        d = lines[lines.scan_id == row.scan_id]
        if d.empty:
            scans.append({"scan": row.scan_id, "lines": []})
            continue
        big = d.groupby("region_id")["text"].apply(lambda s: s.str.len().sum()).idxmax()
        reg = d[d.region_id == big].sort_values("line_idx")
        top = reg["text"].tolist()[:8]
        x0, y0 = max(int(reg.x.min()) - PAD_X, 0), max(int(reg.y.min()) - PAD_Y, 0)
        cw, ch = int((reg.x + reg.w).max()) + PAD_X - x0, int((reg.y + reg.h).max()) + PAD_Y - y0
        c = chk.loc[row.scan_id]
        if c.status not in ("ok", "scaled"):
            scans.append({"scan": row.scan_id, "lines": top})
            continue
        r = int(c.na_w) / int(c.xml_w) if c.status == "scaled" else 1.0
        NW, NH = int(c.na_w), int(c.na_h)
        sx, sy = min(round(x0 * r), NW - 1), min(round(y0 * r), NH - 1)
        sw_, sh_ = min(round(cw * r), NW - sx), min(round(ch * r), NH - sy)
        kw = dict(base=c.iiif_base, x=sx, y=sy, w=sw_, h=sh_)
        scans.append({"scan": row.scan_id, "lines": top, "url": IIIF.format(sw=min(DISPLAY_W, sw_), **kw),
                      "full": IIIF.format(sw=min(FULL_W, sw_), **kw), "ar": round(sh_ / sw_, 4)})
    items.append({"inv": inv, "title": titles.get(inv, "(no title found)"), "scans": scans})

CSS = """
.q{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px;margin:14px 0}
.lbl{font-weight:600}.ex{font-family:ui-monospace,Menlo,monospace;font-size:.82rem;background:rgba(127,127,127,.12);padding:6px 8px;border-radius:4px;margin:4px 0;white-space:pre-wrap}
.scan{border-top:1px dashed var(--line);margin-top:8px;padding-top:6px}
pre.t{margin:4px 0;font-size:.82rem;white-space:pre-wrap;word-break:break-word}
.btns button{font:inherit;padding:4px 12px;margin:0 6px 0 0;border:1px solid var(--line);border-radius:6px;background:transparent;color:var(--fg);cursor:pointer}
.btns button.on{background:var(--accent);color:var(--bg);border-color:var(--accent)}
.note{width:100%;box-sizing:border-box;margin-top:6px;font:inherit;background:var(--bg);color:var(--fg);border:1px solid var(--line);border-radius:4px;padding:4px}
.prog{position:sticky;top:0;background:var(--bg);padding:6px 0;border-bottom:1px solid var(--line);z-index:2}
.title{font-weight:600}
.pair{display:grid;grid-template-columns:minmax(0,2fr) minmax(0,3fr);gap:14px;align-items:start}
@media (max-width:800px){.pair{grid-template-columns:1fr}}
.crop{position:relative;margin:4px 0;border:1px solid var(--line);border-radius:4px;overflow:hidden;background:#ddd}
.crop img{display:block;width:100%;height:auto}
.crop .cap{position:absolute;right:0;top:0;font-size:.65rem;background:rgba(0,0,0,.55);color:#fff;padding:1px 5px;z-index:3}
.crop .cap a{color:#fff}
.noimg{font-size:.85rem;color:var(--muted);border:1px dashed var(--line);border-radius:6px;padding:10px}
"""
HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hand or print check, 37 volumes</title>
<style>__BASE____CSS__</style></head><body><main>
<h1>Is this volume's text mainly handwritten or printed?</h1>
<div class="card">
<p><b>The question.</b> For each of the 37 volumes below, look at the five scans (each scan shows a crop of its largest text region next to the
first lines of that region as text; click "larger" on the crop or the scan ID to open it bigger, the scan ID in the Nationaal Archief viewer) and decide whether the volume's text is
<b>mainly handwritten</b> or <b>mainly printed</b>. Judge the scans in the viewer, not only the text snippets.</p>
<p><span class="lbl">Handwritten</span>: written by hand (a clerk's index, a minute book, a handwritten extract or copy).
Cues: ragged lines, abbreviations such as <code>Reg:</code>, <code>miss:</code>, <code>Ed. Mo.</code>, marginal dates, corrections.<br>
<span class="lbl">Printed</span>: typeset text (a printed placcaat, resolution, pamphlet). Cues: regular lines of similar length,
page numbers or signatures such as <code>( 83 )</code> or <code>A 2</code>, catchwords, running heads, a printed title page.<br>
<span class="lbl">Mixed</span>: a volume with both kinds in substantial amounts (for example printed forms filled in by hand, or handwritten notes
between printed copies). <span class="lbl">Unsure</span>: you cannot tell.</p>
<div class="ex">Example, handwritten:  "13 Jan: Miss: van der meer met eenige bylagen r:
18 Aug: S. M. Miss: Hop. 2. ordres aan vander Maergegeve"</div>
<div class="ex">Example, printed:  "( 28 )
is, dat indien men gewagt had, tot dat de Vyand
eene brusque en environnante attacque zoude heb-"</div>
<p>Use the note field for a reason or an exception (for example "scans 1 and 4 handwritten, 2, 3, 5 printed").
Your answers are saved in this browser. When done, press <b>Export</b> and save <code>check_verdicts.json</code> in
<code>reports/data/phase-12-hand-print-box-height/step-15z/</code>. <b>Import</b> reads an earlier export back in.</p>
</div>
<div class="prog"><span id="prog"></span> <button type="button" id="exp">Export</button>
<label>Import <input type="file" id="imp" accept=".json"></label></div>
<div id="items"></div>
</main>
<script>
__HELPERS__
const ITEMS = __ITEMS__;
const store = new CheckStore("as_phase12_15z_hand_print_v1");
const CHOICES = ["Handwritten", "Printed", "Mixed", "Unsure"];
function cropHtml(s){ if (!s.url) return `<div class="noimg">No text region on this scan; open it in the viewer.</div>`;
  return `<div class="crop" style="aspect-ratio:${(1 / s.ar).toFixed(4)}"><img loading="lazy" alt="crop of ${esc(s.scan)}" src="${esc(s.url)}" onerror="noImg(this)">
    <span class="cap">${esc(s.scan.split("_").slice(-1)[0])} · <a href="${esc(s.full)}" target="_blank" rel="noopener">larger</a></span></div>`; }
function noImg(img){ const c = img.parentNode; c.outerHTML = `<div class="noimg">This crop is not available on the image server. Open the scan in the Nationaal Archief viewer with the scan link above.</div>`; }
function render(){
  const root = document.getElementById("items"); root.innerHTML = "";
  ITEMS.forEach(it => {
    const st = store.get(it.inv);
    const div = document.createElement("div"); div.className = "q"; div.id = "v" + it.inv;
    let h = `<div class="title">Inventory ${esc(it.inv)}</div>` + idLine([["inventory", it.inv]])
      + `<div>${esc(it.title)}</div>`;
    it.scans.forEach((s, i) => {
      h += `<div class="scan"><div class="ids">scan ${i+1}: ${scanHtml(s.scan)}</div><div class="pair"><pre class="t">${esc(s.lines.join("\\n"))}</pre><div>${cropHtml(s)}</div></div></div>`;
    });
    h += `<div class="btns">` + CHOICES.map(c => `<button type="button" data-inv="${it.inv}" data-c="${c}" class="${st.choice === c ? "on" : ""}">${c}</button>`).join("")
      + `</div><textarea class="note" rows="2" placeholder="note (optional)" data-inv="${it.inv}">${esc(st.note || "")}</textarea>`;
    div.innerHTML = h; root.appendChild(div);
  });
  progress();
}
function progress(){ const n = ITEMS.filter(it => store.get(it.inv).choice).length; document.getElementById("prog").textContent = `${n} of ${ITEMS.length} answered`; }
document.addEventListener("click", e => { const b = e.target.closest("button[data-c]"); if (!b) return;
  store.set(b.dataset.inv, {inv: b.dataset.inv, choice: b.dataset.c}); render(); });
document.addEventListener("input", e => { const t = e.target.closest("textarea.note"); if (!t) return; store.set(t.dataset.inv, {inv: t.dataset.inv, note: t.value}); });
document.getElementById("exp").onclick = () => store.exportJSON("check_verdicts.json", "as_phase12_15z_hand_print_v1",
  ITEMS.map(it => ({inv: it.inv, ...store.get(it.inv)})));
document.getElementById("imp").onchange = e => { if (e.target.files[0]) store.importJSON(e.target.files[0], x => x.inv, render); };
render();
</script></body></html>
"""
page = (HTML.replace("__BASE__", checkpage.BASE_CSS).replace("__CSS__", CSS).replace("__HELPERS__", checkpage.HELPERS_JS)
        .replace("__ITEMS__", json.dumps(items, ensure_ascii=False)))
(D / "step-15z/check_page.html").write_text(page, encoding="utf-8")
(D / "step-15z/check_page_items.json").write_text(json.dumps(items, ensure_ascii=False, indent=1), encoding="utf-8")
print(len(items), "items;", sum(len(i["scans"]) for i in items), "scans;", len(page) // 1024, "KiB")
