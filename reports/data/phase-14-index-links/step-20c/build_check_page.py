"""Build the confirm page of 20 index links (step 20c).  Run from the AS root:
    .venv/bin/python reports/data/phase-14-index-links/step-20c/build_check_page.py
Reads entry_gold.tsv and the unit texts of the layer; writes check_items.json and check_page.html."""
import csv, json, random, subprocess, sys, tempfile, os, re
sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools")
import checkpage
from archival_structures.text_layer.io import read_tsv_gz

D = "reports/data/phase-14-index-links/step-20c"
L = "data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5"
rows = list(csv.DictReader(open(f"{D}/entry_gold.tsv"), delimiter="\t"))
pool = [r for r in rows if r["target_scan_id"] not in ("none", "") and r["folio_evidence"] != "unclear"]
pool.sort(key=lambda r: int(r["read_id"]))
pick = sorted(random.Random(20261130).sample(pool, 20), key=lambda r: int(r["read_id"]))

texts = {}
for inv in {r["inv"] for r in pick}:
    for u in read_tsv_gz(f"{L}/join/units/{inv}.tsv.gz"):
        texts[(u["scan_id"], u["unit_id"])] = u["text"]
scans = {inv: [s["scan_id"] for s in read_tsv_gz(f"{L}/scans/{inv}.scans.tsv.gz")] for inv in {r["inv"] for r in pick}}

items = []
for r in pick:
    sc = scans[r["inv"]]
    i = sc.index(r["target_scan_id"])
    items.append(dict(id=r["read_id"], inv=r["inv"], text=texts[(r["scan_id"], r["unit_id"])],
                      entry_scan=r["scan_id"], entry_side={"verso": "left page", "recto": "right page", "single": "whole scan"}[r["page"]],
                      locator=r["target_locator"], all_locators=r["locators"],
                      recto_scan=sc[i], verso_scan=sc[i + 1],
                      agent_content=r["content"], agent_evidence=r["evidence"]))
# --- image crops (added 2026-10-29 for UA-050, at the user's request): NA IIIF crops of the lines whose
# text the page shows: (1) the entry's lines, (2) the top lines of the target recto (folio number),
# (3) the lines on folio N that best match the agent's evidence. Boxes are PageXML pixels.
import handprint
from difflib import SequenceMatcher
CACHE = f"{D}/iiif_cache"
segs, boxes, sizes, sides = {}, {}, {}, {}
for inv in {r["inv"] for r in pick}:
    for g in read_tsv_gz(f"{L}/join/segments/{inv}.tsv.gz"):
        segs.setdefault((g["scan_id"], g["unit_id"]), []).append(g["line_id"])
    lay = {(r["scan_id"], r["line_id"]): r for r in read_tsv_gz(f"{L}/layout/lines/{inv}.tsv.gz")}
    for l in read_tsv_gz(f"{L}/lines/{inv}.lines.tsv.gz"):
        if not l["text"].strip():
            continue
        a = lay.get((l["scan_id"], l["line_id"]), {})
        b = dict(line_id=l["line_id"], box=(int(l["x"]), int(l["y"]), int(l["w"]), int(l["h"])), text=l["text"].strip(),
                 role=a.get("layout_role", ""), col=a.get("layout_column", ""))
        boxes[(l["scan_id"], l["line_id"])] = b
        sides.setdefault((l["scan_id"], a.get("page", "?")), []).append(b)
    for s in read_tsv_gz(f"{L}/scans/{inv}.scans.tsv.gz"):
        if s["page_w"] and s["page_h"]:
            sizes[s["scan_id"]] = (int(s["page_w"]), int(s["page_h"]))
need = sorted({it[k] for it in items for k in ("entry_scan", "recto_scan", "verso_scan")})
bases = handprint.na_iiif_bases(need, CACHE)
img = {sid: handprint.image_size(bases[sid], CACHE, sid) for sid in need if sid in bases}

def mk(scan, lines):
    if not lines or scan not in bases or not img.get(scan) or scan not in sizes:
        return None
    c = handprint.crop(bases[scan], handprint.region_box([b["box"] for b in lines]), sizes[scan], img[scan], display_w=900)
    return dict(c, scan=scan, text=[b["text"] for b in sorted(lines, key=lambda b: (b["box"][1], b["box"][0]))]) if c else None

def toks(t):
    return {w for w in re.findall(r"[^\W\d_]{3,}", t.lower())}

for it, r in zip(items, pick):
    ent = [boxes[(r["scan_id"], lid)] for lid in segs.get((r["scan_id"], r["unit_id"]), []) if (r["scan_id"], lid) in boxes]
    it["crop_entry"] = mk(r["scan_id"], ent)
    top = sorted(sides.get((it["recto_scan"], "recto"), []), key=lambda b: b["box"][1])[:3]
    if top and it["recto_scan"] in sizes:  # widen to the page's right edge and 300 px above: the number sits in the corner
        x0 = min(b["box"][0] for b in top); y0 = max(0, min(b["box"][1] for b in top) - 300)
        y1 = max(b["box"][1] + b["box"][3] for b in top)
        top = top + [dict(box=(x0, y0, sizes[it["recto_scan"]][0] - x0, y1 - y0), text="")]
    it["crop_number"] = mk(it["recto_scan"], top)
    if it["crop_number"]:
        it["crop_number"]["text"] = [t for t in it["crop_number"]["text"] if t]
    ev = re.sub(r"\([^)]*\)", "", it["agent_evidence"]).strip()
    best = None
    for scan, side in ((it["recto_scan"], "recto"), (it["verso_scan"], "verso")):
        lst = sorted(sides.get((scan, side), []), key=lambda b: (b["box"][1], b["box"][0]))
        for k, b in enumerate(lst):
            win = lst[k:k + 2]
            sc = len(toks(ev) & toks(" ".join(x["text"] for x in win))) + SequenceMatcher(None, ev.lower(), b["text"].lower()).ratio()
            if best is None or sc > best[0]:
                best = (sc, scan, lst[max(0, k - 1):k + 2])
    it["crop_evidence"] = mk(best[1], best[2]) if best and best[0] >= 1 else None
    # full text of both target pages, folded out on request (added 2026-10-29 for UA-050: handwriting is hard to read
    # in the crops). Order: main lines by column then height; margin ("extra") lines interleaved by height with
    # the nearest main column, flagged m=1. h=1 marks the lines of the evidence crop.
    hl = {(it["crop_evidence"]["scan"], b["line_id"]) for b in best[2]} if it["crop_evidence"] else set()
    for key, scan, side in (("page_recto", it["recto_scan"], "recto"), ("page_verso", it["verso_scan"], "verso")):
        lst = sides.get((scan, side), [])
        mx = {}
        for b in lst:
            if b["role"] == "main":
                mx.setdefault(b["col"], []).append(b["box"][0])
        mx = sorted((sum(v) / len(v), c) for c, v in mx.items())
        rank = {c: k for k, (_, c) in enumerate(mx)}
        def grp(b):
            if b["role"] == "main" or not mx:
                return rank.get(b["col"], 0)
            return min(range(len(mx)), key=lambda k: abs(mx[k][0] - b["box"][0]))
        it[key] = [dict(t=b["text"], g=grp(b), m=int(b["role"] != "main"), h=int((scan, b["line_id"]) in hl))
                   for b in sorted(lst, key=lambda b: (grp(b), b["box"][1], b["box"][0]))]
json.dump(items, open(f"{D}/check_items.json", "w"), indent=1, ensure_ascii=False)
assert len(items) == 20

CSS = checkpage.BASE_CSS + """
.q{margin:8px 0}.q b{display:block}.btns button{margin:2px 4px 2px 0;padding:3px 10px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
.btns button.on.Yes{background:var(--ok);color:#fff}.btns button.on.No{background:var(--bad);color:#fff}.btns button.on.Unsure{background:var(--uns);color:#fff}
.entry{font-size:1.05rem;padding:6px 8px;border-left:3px solid var(--accent);background:var(--bg)}.agent{color:var(--muted);font-size:.85rem}
.crop{margin:6px 0;padding:6px;border:1px solid var(--line);border-radius:6px}.crop img{max-width:100%;height:auto;display:block;margin:4px 0;background:rgba(127,127,127,.12)}.crop .t{font:.85rem ui-monospace,Menlo,monospace;white-space:pre-wrap;color:var(--muted)}.crop .h{font-size:.85rem}
details.pg{margin:6px 0;border:1px solid var(--line);border-radius:6px;padding:4px 8px}details.pg summary{cursor:pointer;font-size:.9rem}
.pg ol{margin:6px 0 4px;padding-left:2.6em;font:.88rem/1.45 ui-monospace,Menlo,monospace}.pg li{white-space:pre-wrap}.pg li::marker{color:var(--muted);font-size:.75rem}
.pg li.m{color:var(--muted);font-style:italic}.pg li.m::before{content:"[margin] ";font-style:normal;font-size:.72rem}.pg li.h{background:color-mix(in srgb,var(--uns) 22%,transparent)}
.pg li.g{margin-top:.8em;border-top:1px dashed var(--line);padding-top:.3em}
textarea{width:100%;box-sizing:border-box;font:inherit}.top{position:sticky;top:0;background:var(--bg);padding:6px 0;border-bottom:1px solid var(--line)}
"""
JS = checkpage.HELPERS_JS + r"""
const store = new CheckStore("as_phase14_20c_links_v1");
const OPEN = new Set();  // fold-outs left open across re-renders
document.addEventListener("toggle", e => { const d = e.target; if (!d.dataset || !d.dataset.fold) return;
  d.open ? OPEN.add(d.dataset.fold) : OPEN.delete(d.dataset.fold); }, true);
const Q = [["q1","Is the number on the right-hand page of the first linked scan folio N (or does counting from nearby numbers give N)?"],
           ["q2","Is the entry's matter on that page or on the left-hand page of the second scan?"]];
function btns(id,q){ const cur = store.get(id)[q];
  return ["Yes","No","Unsure"].map(v => `<button type="button" class="${v}${cur===v?' on':''}" data-id="${id}" data-q="${q}" data-v="${v}">${v}</button>`).join(""); }
function cropHtml(c, label){
  if (!c) return `<div class="crop"><div class="h">${label}: no crop available (open the scan link)</div></div>`;
  return `<div class="crop"><div class="h">${label} — ${scanHtml(c.scan)} · <a href="${esc(c.full)}" target="_blank" rel="noopener">larger</a></div>
    <img loading="lazy" alt="crop of ${esc(c.scan)}" src="${esc(c.url)}" onerror="this.replaceWith(Object.assign(document.createElement('div'),{textContent:'image not available on the server; open the scan link'}))">
    <div class="t">${esc(c.text.join("\n"))}</div></div>`; }
function pageHtml(id, which, lines, scan, label){
  const k = id + ":" + which;
  if (!lines || !lines.length) return `<details class="pg" data-fold="${k}"><summary>${label} — no text lines in the layer</summary></details>`;
  const nh = lines.filter(l => l.h).length;
  return `<details class="pg" data-fold="${k}"${OPEN.has(k) ? " open" : ""}><summary>${label} (${lines.length} lines)</summary>
    <div class="ids">${scanHtml(scan)}${nh ? " &nbsp; highlighted: the lines of the crop above" : ""}</div><ol>${lines.map((l,i) => `<li class="${l.m?"m ":""}${l.h?"h ":""}${i && l.g !== lines[i-1].g ? "g" : ""}">${esc(l.t)}</li>`).join("")}</ol></details>`; }
function render(){
  document.getElementById("items").innerHTML = ITEMS.map((it,n) => `<div class="card" id="c${it.id}">
    <b>${n+1}/20</b> &nbsp; inventory ${esc(it.inv)} ${idLine([["read_id",it.id]])}
    <div class="entry">${esc(it.text)}</div>
    ${cropHtml(it.crop_entry, "Image of the entry (text below is what the layer extracted)")}
    <div>Entry on ${scanHtml(it.entry_scan)} (${it.entry_side}). All locators in the entry: ${esc(it.all_locators||"-")}.</div>
    <div>Locator checked: <b>${esc(it.locator)}</b></div>
    <div>Agent's target: folio ${esc(it.locator)} recto = right-hand page of ${scanHtml(it.recto_scan)}; its back = left-hand page of ${scanHtml(it.verso_scan)}</div>
    ${cropHtml(it.crop_number, "Top of the target right-hand page (where folio " + esc(it.locator) + " is written)")}
    ${it.agent_content === "absent" ? "" : cropHtml(it.crop_evidence, "Lines where the agent found the matter")}
    ${pageHtml(it.id, "r", it.page_recto, it.recto_scan, "Show full text: right-hand page of the first scan (folio " + esc(it.locator) + " recto)")}
    ${pageHtml(it.id, "v", it.page_verso, it.verso_scan, "Show full text: left-hand page of the second scan (folio " + esc(it.locator) + " verso)")}
    <div class="agent">Agent's answer: matter found on <b>${esc(it.agent_content)}</b>: ${esc(it.agent_evidence)}</div>
    <div class="q"><b>${Q[0][1]}</b><span class="btns">${btns(it.id,"q1")}</span></div>
    <div class="q"><b>${Q[1][1]}</b><span class="btns">${btns(it.id,"q2")}</span></div>
    <textarea rows="2" data-note="${it.id}" placeholder="note (optional)">${esc(store.get(it.id).note||"")}</textarea></div>`).join("");
  const done = ITEMS.filter(it => store.get(it.id).q1 && store.get(it.id).q2).length;
  document.getElementById("prog").textContent = done + " / " + ITEMS.length + " answered";
}
document.addEventListener("click", e => { const b = e.target.closest("button[data-q]"); if (!b) return;
  store.set(b.dataset.id, {[b.dataset.q]: b.dataset.v}); render(); });
document.addEventListener("input", e => { const t = e.target.closest("textarea[data-note]"); if (t) store.set(t.dataset.note, {note: t.value}); });
document.getElementById("exp").onclick = () => store.exportJSON("check_verdicts.json", "as_phase14_20c_links_v1",
  ITEMS.map(it => ({id: it.id, inv: it.inv, locator: it.locator, recto_scan: it.recto_scan, verso_scan: it.verso_scan, ...store.get(it.id)})));
render();
"""
html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Check 20 index links</title><style>{CSS}</style></head><body><main>
<h1>Check 20 index links (phase 14, step 20c)</h1>
<p><b>This page shows the agent's answer</b>: it is a confirmation, not a blind test. For each index entry of a States General register (1.01.02), the agent read the folio number the entry ends in, found that folio in the volume, and judged whether the entry's matter is there.</p>
<p>Folios are numbered on the right-hand page (recto) only; the back of folio N is the left-hand page of the <i>next</i> scan. In each item open the first scan (right-hand page: is the printed number N, or does counting from nearby printed numbers give N?) and the second scan (left-hand page). Answer Yes / No / Unsure on both questions; use the note for anything odd (for example the true folio).</p>
<p>Each item shows three image crops from the Nationaal Archief image server, each with the text the layer extracted for those lines below it: the entry, the top of the target right-hand page (the folio number), and the lines where the agent found the matter. They are for convenience; when a crop is cut off or wrong, open the scan link. Crops need an internet connection.</p>
<p>For question 2, two fold-outs under the crops show the full extracted text of the right-hand page of the first scan and the left-hand page of the second scan, in reading order. Margin notes are marked <i>[margin]</i> and placed at their height on the page; a dashed rule separates text columns; the lines of the evidence crop are highlighted. The text is the layer's handwriting recognition, so expect errors.</p>
<p>When done, press Export and save the file as <code>reports/data/phase-14-index-links/step-20c/check_verdicts.json</code> (AS root). Answers are kept in this browser until then.</p>
<div class="top"><span id="prog"></span> &nbsp; <button id="exp" type="button">Export check_verdicts.json</button></div>
<div id="items"></div></main>
<script>const ITEMS = {json.dumps(items, ensure_ascii=False)};</script><script>{JS}</script></body></html>"""
open(f"{D}/check_page.html", "w").write(html)

# checks: item count, links, JS syntax
assert html.count('"entry_scan"') == 20
for it in items:
    for k in ("entry_scan", "recto_scan", "verso_scan"):
        assert checkpage.scan_url(it[k]), it
src = JS.replace("document.addEventListener", "void 0 && document.addEventListener").replace("document.getElementById", "void 0 && document.getElementById")
p = os.path.join(tempfile.mkdtemp(), "p.js"); open(p, "w").write(checkpage.HELPERS_JS.replace("document.addEventListener", "void 0 && document.addEventListener") + "\n" + re.sub(r"^const store.*$", "", JS.split("const store",1)[1] and "const store"+JS.split("const store",1)[1], count=0, flags=re.M))
js = ("ObjC.import('Foundation'); var s=$.NSString.stringWithContentsOfFileEncodingError('%s',4,null).js;"
      "try{ new Function(s); 'syntax ok' } catch(e){ 'ERR '+e }" % p)
out = subprocess.run(["osascript", "-l", "JavaScript", "-e", js], capture_output=True, text=True).stdout.strip()
n_crops = {k: sum(1 for it in items if it.get(k)) for k in ("crop_entry", "crop_number", "crop_evidence")}
print("items", len(items), "| JS syntax:", out, "| crops:", n_crops)
# no method predictions on the page: only the agent's own answer by design
