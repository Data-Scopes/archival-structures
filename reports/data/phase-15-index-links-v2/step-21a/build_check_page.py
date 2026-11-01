"""Build the confirm page of 20 test link targets (step 21a).  Run from the AS root:
    .venv/bin/python reports/data/phase-15-index-links-v2/step-21a/build_check_page.py
Reads target_gold.tsv, targets.tsv, line_gold.tsv and the layer texts; writes check_items.json and check_page.html.
Pool: test targets with target_scan_id != none and folio_evidence != unclear, sorted by target_id;
random.Random(20261031).sample(pool, 20); items sorted by target_id.
Image crops (added 2026-10-30, UA-055) come from the NA IIIF server (network; cached in iiif_cache/)."""
import csv, json, os, random, re, subprocess, sys, tempfile
sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import checkpage
from _common import READS, side_lines, tab

D = "reports/data/phase-15-index-links-v2/step-21a"
KEY = "as_phase15_21a_links_v1"
EXPORT = f"{D}/check_verdicts.json"
rd = lambda n: list(csv.DictReader(open(f"{D}/{n}", newline=""), delimiter="\t"))
tgd = rd("target_gold.tsv")
pool = sorted((r for r in tgd if r["split"] == "test" and r["target_scan_id"] != "none" and r["folio_evidence"] != "unclear"),
              key=lambda r: r["target_id"])
pick = sorted(random.Random(20261031).sample(pool, 20), key=lambda r: r["target_id"])
reads = {r["read_id"]: r for r in csv.DictReader(open(READS, newline=""), delimiter="\t")}
gold = rd("line_gold.tsv")
SIDE = {"verso": "left page", "recto": "right page", "single": "whole scan"}

items = []
for r in pick:
    rd0 = reads[r["read_id"]]
    inv = r["inv"]
    ls = sorted((g for g in gold if g["read_id"] == r["read_id"]), key=lambda g: int(g["k"]))
    txt = {o[1]: o[2]["text"].strip() for o in side_lines(inv, rd0["scan_id"], rd0["page"])}
    ent_lines = [g for g in ls if g["label"] in ("S", "C") and g["entry"] == r["entry"]]
    locs = []
    for g in ls:
        for tok in filter(None, g["locators"].split(";")):
            if "@" in tok:
                n, e = tok.split("@")
                if e == r["entry"]:
                    locs.append(n)
            elif g["entry"] == r["entry"]:
                locs.append(tok)
    scans = [s["scan_id"] for s in tab("scans", inv)]
    i = scans.index(r["target_scan_id"])
    items.append(dict(id=r["target_id"], read_id=r["read_id"], inv=inv, entry=r["entry"],
                      text=" / ".join(txt[g["line_id"]] for g in ent_lines),
                      entry_scan=rd0["scan_id"], entry_side=SIDE[rd0["page"]], locator=r["target_locator"],
                      all_locators="; ".join(locs), recto_scan=scans[i], verso_scan=scans[i + 1],
                      next_scan=scans[i + 1], opening_scan=scans[i],
                      folio_evidence=r["folio_evidence"], agent_content=r["content"], agent_sides=r["content_sides"],
                      agent_evidence=r["evidence"], agent_note=r["note"],
                      entry_line_ids=[g["line_id"] for g in ent_lines]))

# --- image crops (added 2026-10-30 for UA-055, at the user's request; as in phase-14 step 20c): NA IIIF crops of
# (1) the entry's gold lines, (2) the top lines of the target recto (folio number), (3) the lines on the sides the
# agent named (content_sides: opening = verso of the target scan, front = its recto, back = verso of the next scan,
# next = recto of the next scan) that best match the agent's evidence. Boxes are PageXML pixels. Crops are display
# aids only: nothing here changes a gold file (hashes.txt).
import handprint
from difflib import SequenceMatcher
CACHE = f"{D}/iiif_cache"
boxes, sizes, sides = {}, {}, {}
for inv in {it["inv"] for it in items}:
    lay = {(x["scan_id"], x["line_id"]): x for x in tab("layout", inv)}
    for l in tab("lines", inv):
        a = lay.get((l["scan_id"], l["line_id"]))
        if not l["text"].strip() or not a:
            continue
        b = dict(line_id=l["line_id"], box=(int(float(l["x"])), int(float(l["y"])), int(float(l["w"])), int(float(l["h"]))),
                 text=l["text"].strip(), role=a["layout_role"], col=a["layout_column"])
        boxes[(l["scan_id"], l["line_id"])] = b
        sides.setdefault((l["scan_id"], a["page"]), []).append(b)
    for s in tab("scans", inv):
        if s["page_w"] and s["page_h"]:
            sizes[s["scan_id"]] = (int(s["page_w"]), int(s["page_h"]))
need = sorted({it[k] for it in items for k in ("entry_scan", "recto_scan", "verso_scan")})
bases = handprint.na_iiif_bases(need, CACHE)
img = {sid: handprint.image_size(bases[sid], CACHE, sid) for sid in need if sid in bases}

def mk(scan, lines):
    if not lines or scan not in bases or not img.get(scan) or scan not in sizes:
        return None
    c = handprint.crop(bases[scan], handprint.region_box([b["box"] for b in lines]), sizes[scan], img[scan], display_w=900)
    return dict(c, scan=scan, text=[b["text"] for b in sorted(lines, key=lambda b: (b["box"][1], b["box"][0])) if b["text"]]) if c else None

def toks(t):
    return {w for w in re.findall(r"[^\W\d_]{3,}", t.lower())}

SIDES = {"opening": ("opening_scan", "verso"), "front": ("recto_scan", "recto"),
         "back": ("verso_scan", "verso"), "next": ("next_scan", "recto")}
for it in items:
    ent = [boxes[(it["entry_scan"], lid)] for lid in it.pop("entry_line_ids") if (it["entry_scan"], lid) in boxes]
    it["crop_entry"] = mk(it["entry_scan"], ent)
    top = sorted(sides.get((it["recto_scan"], "recto"), []), key=lambda b: b["box"][1])[:3]
    if top and it["recto_scan"] in sizes:  # widen to the page's right edge and 300 px above: the number sits in the corner
        x0 = min(b["box"][0] for b in top); y0 = max(0, min(b["box"][1] for b in top) - 300)
        y1 = max(b["box"][1] + b["box"][3] for b in top)
        top = top + [dict(box=(x0, y0, sizes[it["recto_scan"]][0] - x0, y1 - y0), text="")]
    it["crop_number"] = mk(it["recto_scan"], top)
    # evidence: one crop per evidence fragment ("..." or ";" separated), best 2-line window on the named sides
    best_all, hl = [], set()
    if it["agent_content"] != "absent":
        cand = [SIDES[s] for s in it["agent_sides"].split(";") if s in SIDES] or [SIDES["front"], SIDES["back"]]
        frags = [f for f in (re.sub(r"\([^)]*\)|\bmarg\b", " ", x).strip(" .,:") for x in re.split(r"\.\.\.|;", it["agent_evidence"])) if len(toks(f)) >= 1]
        for f in frags:
            best = None
            for sk, side in cand:
                scan = it[sk]
                lst = sorted(sides.get((scan, side), []), key=lambda b: (b["box"][1], b["box"][0]))
                for k, b in enumerate(lst):
                    win = lst[k:k + 2]
                    sc = len(toks(f) & toks(" ".join(x["text"] for x in win))) + SequenceMatcher(None, f.lower(), b["text"].lower()).ratio()
                    if best is None or sc > best[0]:
                        best = (sc, scan, side, lst[max(0, k - 1):k + 2])
            if best and best[0] >= 1.5 and not any(set(id(b) for b in best[3]) & set(id(b) for b in o[3]) for o in best_all):
                best_all.append(best)
        crops = []
        for sc, scan, side, win in sorted(best_all, key=lambda o: (o[1], o[2] != "verso", o[3][0]["box"][1])):
            c = mk(scan, win)
            if c:
                crops.append(dict(c, side={"recto": "right-hand page", "verso": "left-hand page"}[side]))
                hl |= {(scan, b["line_id"]) for b in win}
        it["crop_evidence"] = crops[:3]
    else:
        it["crop_evidence"] = []
    # full text of both target pages, folded out: main lines by column then height; margin lines interleaved by
    # height with the nearest main column, flagged m=1; h=1 marks the lines of the evidence crops
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
    del it["next_scan"], it["opening_scan"]
json.dump(items, open(f"{D}/check_items.json", "w"), indent=1, ensure_ascii=False)
assert len(items) == 20 and len({it["id"] for it in items}) == 20

CSS = checkpage.BASE_CSS + """
.q{margin:8px 0}.q b{display:block}.btns button{margin:2px 4px 2px 0;padding:3px 10px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
.btns button.on.Yes{background:var(--ok);color:#fff}.btns button.on.No{background:var(--bad);color:#fff}.btns button.on.Unsure{background:var(--uns);color:#fff}
.entry{font-size:1.05rem;padding:6px 8px;border-left:3px solid var(--accent);background:var(--bg)}.agent{color:var(--muted);font-size:.85rem;margin:6px 0}
details.pg{margin:6px 0;border:1px solid var(--line);border-radius:6px;padding:4px 8px}details.pg summary{cursor:pointer;font-size:.9rem}
.pg ol{margin:6px 0 4px;padding-left:2.6em;font:.88rem/1.45 ui-monospace,Menlo,monospace}.pg li{white-space:pre-wrap}.pg li::marker{color:var(--muted);font-size:.75rem}
.pg li.m{color:var(--muted);font-style:italic}.pg li.m::before{content:"[margin] ";font-style:normal;font-size:.72rem}.pg li.h{background:color-mix(in srgb,var(--uns) 22%,transparent)}
.pg li.g{margin-top:.8em;border-top:1px dashed var(--line);padding-top:.3em}
.crop{margin:6px 0;padding:6px;border:1px solid var(--line);border-radius:6px}.crop img{max-width:100%;height:auto;display:block;margin:4px 0;background:rgba(127,127,127,.12)}.crop .t{font:.85rem ui-monospace,Menlo,monospace;white-space:pre-wrap;color:var(--muted)}.crop .h{font-size:.85rem}
textarea{width:100%;box-sizing:border-box;font:inherit}.top{position:sticky;top:0;background:var(--bg);padding:6px 0;border-bottom:1px solid var(--line)}
"""
JS = r"""
const store = new CheckStore("%KEY%");
const OPEN = new Set();
document.addEventListener("toggle", e => { const d = e.target; if (!d.dataset || !d.dataset.fold) return;
  d.open ? OPEN.add(d.dataset.fold) : OPEN.delete(d.dataset.fold); }, true);
const Q = [["q1","Is the number on the right-hand page of the first linked scan folio N (or does counting from nearby numbers give N)?"],
           ["q2","Is the entry's matter on that page or on the left-hand page of the second scan?"],
           ["q3","Do the entry's words belong together (one entry, not two columns mixed)?"]];
function btns(id,q){ const cur = store.get(id)[q];
  return ["Yes","No","Unsure"].map(v => `<button type="button" class="${v}${cur===v?' on':''}" data-id="${id}" data-q="${q}" data-v="${v}">${v}</button>`).join(""); }
function cropHtml(c, label){
  if (!c) return `<div class="crop"><div class="h">${label}: no crop available (open the scan link)</div></div>`;
  return `<div class="crop"><div class="h">${label} — ${scanHtml(c.scan)} · <a href="${esc(c.full)}" target="_blank" rel="noopener">larger</a></div>
    <img loading="lazy" alt="crop of ${esc(c.scan)}" src="${esc(c.url)}" onerror="this.replaceWith(Object.assign(document.createElement('div'),{textContent:'image not available on the server; open the scan link'}))">
    <div class="t">${esc(c.text.join("\n"))}</div></div>`; }
function evHtml(it){
  if (it.agent_content === "absent") return "";
  if (!it.crop_evidence.length) return cropHtml(null, "Lines where the agent found the matter");
  return it.crop_evidence.map((c,j) => cropHtml(c, "Lines where the agent found the matter" + (it.crop_evidence.length > 1 ? ` (${j+1}/${it.crop_evidence.length})` : "") + ", " + c.side)).join(""); }
function pageHtml(id, which, lines, scan, label){
  const k = id + ":" + which;
  if (!lines || !lines.length) return `<details class="pg" data-fold="${k}"><summary>${label}: no text lines in the layer</summary></details>`;
  const nh = lines.filter(l => l.h).length;
  return `<details class="pg" data-fold="${k}"${OPEN.has(k) ? " open" : ""}><summary>${label} (${lines.length} lines)</summary>
    <div class="ids">${scanHtml(scan)}${nh ? " &nbsp; highlighted: the lines of the evidence crops" : ""}</div><ol>${lines.map((l,i) => `<li class="${l.m?"m ":""}${l.h?"h ":""}${i && l.g !== lines[i-1].g ? "g" : ""}">${esc(l.t)}</li>`).join("")}</ol></details>`; }
function render(){
  document.getElementById("items").innerHTML = ITEMS.map((it,n) => `<div class="card" id="c${it.id}">
    <b>${n+1}/20</b> &nbsp; inventory ${esc(it.inv)} ${idLine([["target_id",it.id],["read_id",it.read_id],["entry",it.entry]])}
    <div class="entry">${esc(it.text)}</div>
    ${cropHtml(it.crop_entry, "Image of the entry's lines (text below is what the layer extracted)")}
    <div>Entry on ${scanHtml(it.entry_scan)} (${it.entry_side}). All locators in the entry: ${esc(it.all_locators||"-")}.</div>
    <div>Locator checked: <b>${esc(it.locator)}</b></div>
    <div>Agent's target: folio ${esc(it.locator)} recto = right-hand page of ${scanHtml(it.recto_scan)}; its back = left-hand page of ${scanHtml(it.verso_scan)}</div>
    ${cropHtml(it.crop_number, "Top of the target right-hand page (where folio " + esc(it.locator) + " is written)")}
    ${evHtml(it)}
    ${pageHtml(it.id, "r", it.page_recto, it.recto_scan, "Show extracted text: right-hand page of the first scan")}
    ${pageHtml(it.id, "v", it.page_verso, it.verso_scan, "Show extracted text: left-hand page of the second scan")}
    <div class="agent">Agent's answer: number ${esc(it.folio_evidence)}; matter ${esc(it.agent_content)} (${esc(it.agent_sides||"-")}): ${esc(it.agent_evidence)}${it.agent_note ? " [" + esc(it.agent_note) + "]" : ""}</div>
    ${Q.map(q => `<div class="q"><b>${q[1]}</b><span class="btns">${btns(it.id,q[0])}</span></div>`).join("")}
    <textarea rows="2" data-note="${it.id}" placeholder="note (optional)">${esc(store.get(it.id).note||"")}</textarea></div>`).join("");
  const done = ITEMS.filter(it => store.get(it.id).q1 && store.get(it.id).q2 && store.get(it.id).q3).length;
  document.getElementById("prog").textContent = done + " / " + ITEMS.length + " answered";
}
document.addEventListener("click", e => { const b = e.target.closest("button[data-q]"); if (!b) return;
  store.set(b.dataset.id, {[b.dataset.q]: b.dataset.v}); render(); });
document.addEventListener("input", e => { const t = e.target.closest("textarea[data-note]"); if (t) store.set(t.dataset.note, {note: t.value}); });
document.getElementById("exp").onclick = () => store.exportJSON("check_verdicts.json", "%KEY%",
  ITEMS.map(it => ({id: it.id, read_id: it.read_id, inv: it.inv, locator: it.locator, recto_scan: it.recto_scan, verso_scan: it.verso_scan, ...store.get(it.id)})));
render();
""".replace("%KEY%", KEY)
html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Check 20 index links (v2 gold)</title><style>{CSS}</style></head><body><main>
<h1>Check 20 index-link targets (phase 15, step 21a)</h1>
<p><b>This page shows the agent's answers</b>: it is a confirmation, not a blind test. For each index entry of a States General register (1.01.02), the agent read the folio number the entry ends in (the first locator), found that folio in the volume, and judged whether the entry's matter is there. The 20 items are drawn at random (seed 20261031) from the 63 test targets with a scan and a printed or counted folio number.</p>
<p>Folios are numbered on the right-hand page (recto) only; the back of folio N is the left-hand page of the <i>next</i> scan. In each item open the first scan (right-hand page: is the printed number N, or does counting from nearby printed numbers give N?) and the second scan (left-hand page). Answer Yes / No / Unsure on all three questions; use the note for anything odd (for example the true folio, or that the entry text above mixes two entries). The entry text is the HTR of the entry's lines joined in reading order, lines separated by " / "; when a line was merged with the other column, that column's words may show up in the text.</p>
<p>Each item shows image crops from the Nationaal Archief image server, each with the text the layer extracted for those lines below it: the entry's lines, the top of the target right-hand page (the folio number), and the lines where the agent found the matter (matched automatically from the agent's evidence, on the sides the agent named; up to three crops). They are for convenience; when a crop is cut off or wrong, open the scan link. Crops need an internet connection.</p>
<p>The fold-outs show the extracted text of the two target pages in reading order. Margin notes are marked <i>[margin]</i> and placed at their height on the page; a dashed rule separates text columns; the lines of the evidence crops are highlighted. It is handwriting recognition, so expect errors; the scan link is the authority.</p>
<p>When done, press Export and save the file as <code>{EXPORT}</code> (AS root). Answers are kept in this browser until then.</p>
<div class="top"><span id="prog"></span> &nbsp; <button id="exp" type="button">Export check_verdicts.json</button></div>
<div id="items"></div></main>
<script>const ITEMS = {json.dumps(items, ensure_ascii=False)};</script><script>{checkpage.HELPERS_JS}</script><script>{JS}</script></body></html>"""
open(f"{D}/check_page.html", "w").write(html)

# checks: item count, links on every item, no method predictions, JS syntax
assert html.count('"entry_scan"') == 20
for it in items:
    for k in ("entry_scan", "recto_scan", "verso_scan"):
        assert checkpage.scan_url(it[k]), it
assert "prediction" not in html.lower()
p = os.path.join(tempfile.mkdtemp(), "p.js")
open(p, "w").write(checkpage.HELPERS_JS + "\n" + JS)
js = ("ObjC.import('Foundation'); var s=$.NSString.stringWithContentsOfFileEncodingError('%s',4,null).js;"
      "try{ new Function(s); 'syntax ok' } catch(e){ 'ERR '+e }" % p)
out = subprocess.run(["osascript", "-l", "JavaScript", "-e", js], capture_output=True, text=True).stdout.strip()
n_crops = {k: sum(1 for it in items if it.get(k)) for k in ("crop_entry", "crop_number", "crop_evidence")}
print("items", len(items), "| pool", len(pool), "| JS syntax:", out, "| ids", items[0]["id"], "...", items[-1]["id"], "| crops:", n_crops)
print("scan links on all items:", all(it[k] for it in items for k in ("entry_scan", "recto_scan", "verso_scan")),
      "| splits", {reads[it["read_id"]]["split"] for it in items}, "| strata", sorted({reads[it["read_id"]]["stratum"] for it in items}))
assert out == "syntax ok"
