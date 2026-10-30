"""Build the confirm page of 20 test link targets (step 21a).  Run from the AS root:
    .venv/bin/python reports/data/phase-15-index-links-v2/step-21a/build_check_page.py
Reads target_gold.tsv, targets.tsv, line_gold.tsv and the layer texts; writes check_items.json and check_page.html.
Pool: test targets with target_scan_id != none and folio_evidence != unclear, sorted by target_id;
random.Random(20261031).sample(pool, 20); items sorted by target_id.  No image crops (no network needed)."""
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
    def page(scan, side):
        lay = {x["line_id"]: x for x in tab("layout", inv) if x["scan_id"] == scan}
        out = []
        for l in tab("lines", inv):
            if l["scan_id"] == scan and l["line_id"] in lay and lay[l["line_id"]]["page"] == side and l["text"].strip():
                out.append((lay[l["line_id"]]["layout_column"], float(l["y"]), float(l["x"]), l["text"].strip()))
        return [t[3] for t in sorted(out)]
    items.append(dict(id=r["target_id"], read_id=r["read_id"], inv=inv, entry=r["entry"],
                      text=" / ".join(txt[g["line_id"]] for g in ent_lines),
                      entry_scan=rd0["scan_id"], entry_side=SIDE[rd0["page"]], locator=r["target_locator"],
                      all_locators="; ".join(locs), recto_scan=scans[i], verso_scan=scans[i + 1],
                      folio_evidence=r["folio_evidence"], agent_content=r["content"], agent_sides=r["content_sides"],
                      agent_evidence=r["evidence"], agent_note=r["note"],
                      page_recto=page(scans[i], "recto"), page_verso=page(scans[i + 1], "verso")))
json.dump(items, open(f"{D}/check_items.json", "w"), indent=1, ensure_ascii=False)
assert len(items) == 20 and len({it["id"] for it in items}) == 20

CSS = checkpage.BASE_CSS + """
.q{margin:8px 0}.q b{display:block}.btns button{margin:2px 4px 2px 0;padding:3px 10px;border:1px solid var(--line);background:transparent;color:var(--fg);border-radius:4px;cursor:pointer}
.btns button.on.Yes{background:var(--ok);color:#fff}.btns button.on.No{background:var(--bad);color:#fff}.btns button.on.Unsure{background:var(--uns);color:#fff}
.entry{font-size:1.05rem;padding:6px 8px;border-left:3px solid var(--accent);background:var(--bg)}.agent{color:var(--muted);font-size:.85rem;margin:6px 0}
details.pg{margin:6px 0;border:1px solid var(--line);border-radius:6px;padding:4px 8px}details.pg summary{cursor:pointer;font-size:.9rem}
.pg ol{margin:6px 0 4px;padding-left:2.6em;font:.88rem/1.45 ui-monospace,Menlo,monospace}.pg li{white-space:pre-wrap}
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
function pageHtml(id, which, lines, scan, label){
  const k = id + ":" + which;
  if (!lines || !lines.length) return `<details class="pg" data-fold="${k}"><summary>${label}: no text lines in the layer</summary></details>`;
  return `<details class="pg" data-fold="${k}"${OPEN.has(k) ? " open" : ""}><summary>${label} (${lines.length} lines)</summary>
    <div class="ids">${scanHtml(scan)}</div><ol>${lines.map(t => `<li>${esc(t)}</li>`).join("")}</ol></details>`; }
function render(){
  document.getElementById("items").innerHTML = ITEMS.map((it,n) => `<div class="card" id="c${it.id}">
    <b>${n+1}/20</b> &nbsp; inventory ${esc(it.inv)} ${idLine([["target_id",it.id],["read_id",it.read_id],["entry",it.entry]])}
    <div class="entry">${esc(it.text)}</div>
    <div>Entry on ${scanHtml(it.entry_scan)} (${it.entry_side}). All locators in the entry: ${esc(it.all_locators||"-")}.</div>
    <div>Locator checked: <b>${esc(it.locator)}</b></div>
    <div>Agent's target: folio ${esc(it.locator)} recto = right-hand page of ${scanHtml(it.recto_scan)}; its back = left-hand page of ${scanHtml(it.verso_scan)}</div>
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
<p>The fold-outs show the extracted text of the two target pages (by column, then height). It is handwriting recognition, so expect errors; the scan link is the authority.</p>
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
print("items", len(items), "| pool", len(pool), "| JS syntax:", out, "| ids", items[0]["id"], "...", items[-1]["id"])
print("scan links on all items:", all(it[k] for it in items for k in ("entry_scan", "recto_scan", "verso_scan")),
      "| splits", {reads[it["read_id"]]["split"] for it in items}, "| strata", sorted({reads[it["read_id"]]["stratum"] for it in items}))
assert out == "syntax ok"
