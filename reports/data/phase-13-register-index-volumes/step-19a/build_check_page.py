"""Build the blind check page of 30 side labels (step 19a).

Run from the AS root: PYTHONPATH=. .venv/bin/python reports/data/phase-13-register-index-volumes/step-19a/build_check_page.py
Writes check_items.json and check_page.html next to this file. The page carries no agent verdicts.
Draw: random.Random(20261116); 10 from S1, 10 from S2, 10 from S3h+S3a together, each stratum's
rows sorted by read_id. Item order on the page is then shuffled with the same generator.
"""
import csv
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools")
import checkpage  # noqa: E402
import handprint  # noqa: E402

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from show_side import load_row, side_lines, listing  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402
from archival_structures.parsers.ead_tree import parse_ead  # noqa: E402
from archival_structures.parsers.ead_views import unit_of_description_frame  # noqa: E402

PLAN = HERE.parent / "planning"
rows = list(csv.DictReader(open(PLAN / "reads_sides.tsv"), delimiter="\t"))
strata = {"S1": [], "S2": [], "S3": []}
for r in sorted(rows, key=lambda r: int(r["read_id"])):
    strata["S3" if r["stratum"] in ("S3h", "S3a") else r["stratum"]].append(r)
rng = random.Random(20261116)
draw = []
for s in ("S1", "S2", "S3"):
    draw += rng.sample(strata[s], 10)
rng.shuffle(draw)

fa = parse_ead(Path("data/metadata/eads/NL-HaNA_1.01.02.xml"))
fr = unit_of_description_frame(fa)
meta = {str(r.inventory_number): (" ".join(str(r.title).split()), str(r.unitdate)) for r in fr.itertuples()}

WORDS = {"verso": "verso = left page of the opening", "recto": "recto = right page of the opening",
         "single": "single = the whole scan"}
CACHE = HERE / "na_cache"
CACHE.mkdir(exist_ok=True)
TL = Path("data/text_layer_v2_dev")


def line_heights(row):
    base = TL / row["layer"] / "1.01.02" / "NL-HaNA_1.01.02_text_layer_v4"
    return {r["line_id"]: int(float(r["h"])) for r in read_tsv_gz(base / "lines" / f"{row['inv']}.lines.tsv.gz")
            if r["scan_id"] == row["scan_id"]}


def xml_size(row):
    base = TL / row["layer"] / "1.01.02" / "NL-HaNA_1.01.02_text_layer_v4"
    for r in read_tsv_gz(base / "scans" / f"{row['inv']}.scans.tsv.gz"):
        if r["scan_id"] == row["scan_id"]:
            return int(r["page_w"]), int(r["page_h"])
    return None


prep = []
for r in draw:
    row = load_row("sides", r["read_id"])
    lines = listing(side_lines(row))
    hs = line_heights(row)
    for l in lines:
        l["h"] = hs.get(l["line_id"], 30)
    prep.append((r, row, lines))
bases = handprint.na_iiif_bases([row["scan_id"] for _, row, _ in prep], str(CACHE))


def box(l):
    return (int(l["x"]), int(l["y"]), int(l["w"]), int(l["h"]))


def strip_scale(c):
    return None if c is None else {"url": c["url"], "full": c["full"], "ar": c["ar"]}


items = []
n_nocrop = 0
for r, row, lines in prep:
    title, date = meta.get(row["inv"], ("(title not found)", ""))
    shown = [l for l in lines if len(l["text"].strip()) >= 8][:3]
    if not shown:
        shown = lines[:3]
    cl = cs = None
    base = bases.get(row["scan_id"])
    xs = xml_size(row)
    if base and xs and lines:
        size = handprint.image_size(base, str(CACHE), row["scan_id"])
        if size:
            cl = handprint.crop(base, handprint.region_box([box(l) for l in shown]), xs, size)
            cs = handprint.crop(base, handprint.region_box([box(l) for l in lines]), xs, size, display_w=760)
    if cl is None or cs is None:
        n_nocrop += 1
    items.append(dict(id=f"side{int(r['read_id']):02d}", read_id=int(r["read_id"]), inv=row["inv"], title=title,
                      date=date, scan_id=row["scan_id"], page=row["page"], page_words=WORDS[row["page"]],
                      first_lines=[f"col {l['col']}: {l['text']}" for l in shown],
                      crop_lines=strip_scale(cl), crop_side=strip_scale(cs)))
print("items without both crops:", n_nocrop)
json.dump(items, open(HERE / "check_items.json", "w"), indent=1, ensure_ascii=False)

CSS = """
.btns{display:flex;gap:8px;margin:8px 0}
.btns button{font:600 .9rem system-ui;padding:6px 14px;border:1px solid var(--line);border-radius:6px;background:transparent;color:var(--fg);cursor:pointer}
.btns button.on.i{background:var(--ok);color:#fff;border-color:var(--ok)}
.btns button.on.n{background:var(--bad);color:#fff;border-color:var(--bad)}
.btns button.on.u{background:var(--uns);color:#fff;border-color:var(--uns)}
.crop{margin:6px 0}.crop img{max-width:100%;height:auto;border:1px solid var(--line);border-radius:6px;display:block}.cap{font-size:.8rem;color:var(--muted)}.noimg{font-size:.9rem;color:var(--muted);border:1px dashed var(--line);border-radius:6px;padding:8px;margin:6px 0}
.first{font-family:ui-monospace,Menlo,monospace;font-size:.82rem;background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:6px 8px;margin:6px 0;white-space:pre-wrap}
textarea{width:100%;box-sizing:border-box;font:inherit;background:var(--bg);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:6px}
h1{font-size:1.25rem}.q{font-weight:600}.title{margin:2px 0}.bar{position:sticky;top:0;background:var(--bg);padding:8px 0;border-bottom:1px solid var(--line);z-index:2}
"""
JS = r"""
const ITEMS = __ITEMS__;
const store = new CheckStore("as_phase13_19a_sides_v1");
function render(){
  const root = document.getElementById("items"); root.innerHTML = "";
  ITEMS.forEach((it, n) => {
    const v = store.get(it.id);
    const cropHtml = (c, cap) => c ? `<div class="crop"><div class="cap">${cap} (<a href="${esc(c.full)}" target="_blank" rel="noopener">larger</a>)</div><a href="${esc(c.full)}" target="_blank" rel="noopener"><img loading="lazy" src="${esc(c.url)}" alt="${esc(cap)}" style="aspect-ratio:1/${c.ar}"></a></div>` : "";
    const noImg = (!it.crop_lines || !it.crop_side) ? `<div class="noimg">no image crop (use the NA link)</div>` : "";
    const d = document.createElement("div"); d.className = "card"; d.id = it.id;
    d.innerHTML = `<div class="title"><b>${n+1}/${ITEMS.length}</b> &middot; inventory ${esc(it.inv)} &middot; ${esc(it.date)}</div>
      <div class="title">${esc(it.title)}</div>
      ${idLine([["inventory", it.inv], ["item", it.id]])}
      ${scanLine([it.scan_id])}
      <div class="ids">page side to judge: <b>${esc(it.page)}</b> (${esc(it.page_words)})</div>
      ${noImg}${cropHtml(it.crop_lines, "the lines quoted below")}
      <div class="first">First lines of the side (to find it on the image):\n${it.first_lines.map(esc).join("\n")}</div>
      ${cropHtml(it.crop_side, "the whole page side asked about")}
      <div class="q">Is this page side an index (entries pointing to resolutions by date or page/folio number, under headings)?</div>
      <div class="btns">
        <button class="i ${v.verdict==="index"?"on":""}" data-v="index">Index</button>
        <button class="n ${v.verdict==="not index"?"on":""}" data-v="not index">Not index</button>
        <button class="u ${v.verdict==="unsure"?"on":""}" data-v="unsure">Unsure</button>
      </div>
      <textarea rows="2" placeholder="note (optional)">${esc(v.note||"")}</textarea>`;
    d.querySelectorAll(".btns button").forEach(b => b.onclick = () => { store.set(it.id, {verdict: b.dataset.v}); render(); document.getElementById(it.id).scrollIntoView({block:"nearest"}); });
    d.querySelector("textarea").oninput = e => { store.set(it.id, {note: e.target.value}); };
    root.appendChild(d);
  });
  const done = ITEMS.filter(it => store.get(it.id).verdict).length;
  document.getElementById("prog").textContent = done + " / " + ITEMS.length + " judged";
}
function exportAll(){
  const out = ITEMS.map(it => ({id: it.id, read_id: it.read_id, inv: it.inv, scan_id: it.scan_id, page: it.page,
    verdict: store.get(it.id).verdict || null, note: store.get(it.id).note || ""}));
  store.exportJSON("check_verdicts.json", "as_phase13_19a_sides_v1", out);
}
document.getElementById("exp").onclick = exportAll;
render();
"""
HTML = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Phase 13 step 19a: index-side check</title>
<style>{checkpage.BASE_CSS}{CSS}</style></head><body><main>
<h1>Phase 13, step 19a: is this page side an index?</h1>
<p>30 page sides from the 1.01.02 text layer. For each, judge <b>only the named page side</b> on the scan (open the
link; <i>verso</i> = left page of the opening, <i>recto</i> = right page, <i>single</i> = the whole scan). The first
lines of the side are given to help you find it. The crops come from the Nationaal Archief images; judge the whole page side (lower crop), the upper crop only locates the quoted lines. Your judgement is compared with a reading made from the text alone.</p>
<ul>
<li><b>Index</b>: the main content of the side is a list of entries that each point to a resolution or document, by
date and/or a page/folio reference, usually under lemma headings (place, person, subject). Registers, repertoria,
<i>tafels</i> and <i>respecten</i> count. Example: <i>Reqt: P: Roos cums: ten eijnde &hellip; 41.</i> or
<i>17. April. Ry. voor den Consil de lon &hellip;</i> under a heading.</li>
<li><b>Not index</b>: running text (resolutions, minutes, letters, reports), lists without a reference per entry
(names, accounts), title pages, near-empty pages. Minutes in a half-page column
(<i>Ontfangen een missive van &hellip;</i>) are not index. A side that is partly index and partly running text
goes by the majority.</li>
<li><b>Unsure</b>: you cannot decide; say why in the note.</li>
</ul>
<p>Verdicts are kept in this browser automatically. When done, press <b>Export</b> and save the file as
<code>check_verdicts.json</code> in <code>reports/data/phase-13-register-index-volumes/step-19a/</code>.</p>
<div class="bar"><span id="prog"></span> &nbsp; <button id="exp" type="button">Export check_verdicts.json</button></div>
<div id="items"></div>
</main>
<script>{checkpage.HELPERS_JS}
{JS.replace("__ITEMS__", json.dumps(items, ensure_ascii=False))}</script></body></html>
"""
(HERE / "check_page.html").write_text(HTML)
print("items", len(items))
