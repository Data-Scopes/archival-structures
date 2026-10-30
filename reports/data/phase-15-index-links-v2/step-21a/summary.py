"""Counts for the step-21a report, from side_verdicts.tsv, line_gold.tsv, embedded_entries.tsv,
target_gold.tsv and targets.tsv (strata from planning/reads_sides.tsv, the one planning file 21a may open).
Usage (AS root): .venv/bin/python -I reports/data/phase-15-index-links-v2/step-21a/summary.py
"""
import csv
import os
import re
import sys
from collections import Counter, defaultdict

D = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, D)
from _common import READS, side_lines

rd = lambda p, d=D: list(csv.DictReader(open(f"{d}/{p}" if not p.startswith("/") else p, newline=""), delimiter="\t"))
verd, gold, emb = rd("side_verdicts.tsv"), rd("line_gold.tsv"), rd("embedded_entries.tsv")
tgt, tg = rd("target_gold.tsv"), rd("targets.tsv")
reads = {r["read_id"]: r for r in list(csv.DictReader(open(READS, newline=""), delimiter="\t"))}
grp = lambda rid: (reads[rid]["split"], reads[rid]["stratum"])
GROUPS = [("dev", "high"), ("dev", "low"), ("dev", "all"), ("test", "high"), ("test", "low"), ("test", "all"), ("all", "all")]
def member(g, rid):
    s, st = grp(rid)
    return (g[0] in ("all", s)) and (g[1] in ("all", st))
print_header = lambda: print(f"{'':<52}" + "".join(f"{g[0][:4]}-{g[1][:4]}".rjust(14) for g in GROUPS))
def row(label, fn, fmt="{}"):
    print(f"{label:<52}" + "".join(fmt.format(fn(g)).rjust(14) for g in GROUPS))

print_header()
print("== side verdicts ==")
print(Counter((v["split"], v["side_verdict"]) for v in verd))
print("reserves used:", [v["read_id"] for v in verd if v["used_for"]] or "none")
index_ids = sorted({v["read_id"] for v in verd if v["side_verdict"] == "index"}, key=lambda x: int(x[1:]))
print(f"judged index sides: {len(index_ids)}  sides per group:", {f"{g[0]}-{g[1]}": sum(member(g, r) for r in index_ids) for g in GROUPS})

# ---- per-side structures
by = defaultdict(list)
for r in gold:
    by[r["read_id"]].append(r)
for rid in by:
    by[rid].sort(key=lambda r: int(r["k"]))
ent_lines = defaultdict(list)       # (rid, entry) -> [k]
ent_loc = defaultdict(list)         # (rid, entry) -> [locator]
at_tokens = Counter()
for rid, ls in by.items():
    for r in ls:
        if r["label"] in "SC" and r["entry"]:
            ent_lines[(rid, int(r["entry"]))].append(int(r["k"]))
        for tok in filter(None, r["locators"].split(";")):
            if "@" in tok:
                n, e = tok.split("@")
                ent_loc[(rid, int(e))].append(n); at_tokens[rid] += 1
            elif r["entry"]:
                ent_loc[(rid, int(r["entry"]))].append(tok)
print("\n== size (index sides, line_gold.tsv) ==")
row("sides", lambda g: sum(member(g, r) for r in index_ids))
row("layout lines", lambda g: sum(len(by[r]) for r in index_ids if member(g, r)))
row("entries (S lines)", lambda g: sum(1 for (r, e) in ent_lines if member(g, r)))
row("H lines", lambda g: sum(1 for r in gold if r["label"] == "H" and member(g, r["read_id"])))
row("F lines", lambda g: sum(1 for r in gold if r["label"] == "F" and member(g, r["read_id"])))
row("X lines", lambda g: sum(1 for r in gold if r["label"] == "X" and member(g, r["read_id"])))
row("locators (line_gold, @ counted once)", lambda g: sum(len(v) for (r, e), v in ent_loc.items() if member(g, r)))
row("entries with 0 locators", lambda g: sum(1 for k in ent_lines if member(g, k[0]) and len(ent_loc[k]) == 0))
row("entries with 1 locator", lambda g: sum(1 for k in ent_lines if member(g, k[0]) and len(ent_loc[k]) == 1))
row("entries with >=2 locators", lambda g: sum(1 for k in ent_lines if member(g, k[0]) and len(ent_loc[k]) >= 2))
row("mid-line entry ends (@ tokens)", lambda g: sum(v for r, v in at_tokens.items() if member(g, r)))
row("two-column sides (any R line)", lambda g: sum(1 for r in index_ids if member(g, r) and any(x["col"] == "R" for x in by[r])))
row("lines with 'merged columns'", lambda g: sum(1 for r in gold if member(g, r["read_id"]) and "merged columns" in r["note"]))
row("embedded entries (embedded_entries.tsv)", lambda g: sum(1 for e in emb if member(g, e["read_id"])))
row("embedded locators", lambda g: sum(len([t for t in e["locators"].split(";") if t]) for e in emb if member(g, e["read_id"])))
def emb_share(g):
    e = sum(1 for x in emb if member(g, x["read_id"])); n = sum(1 for k in ent_lines if member(g, k[0]))
    return f"{e / (e + n):.3f}" if e + n else "-"
row("embedded share of all entries", emb_share)
def nl(g):
    ents = [k for k in ent_lines if member(g, k[0])]
    return sum(1 for k in ents if ent_lines[k] != list(range(min(ent_lines[k]), max(ent_lines[k]) + 1)))
def nl_inter(g):
    # not consecutive even after skipping H/F/X lines (i.e. another entry's line sits between its lines)
    n = 0
    for k in ent_lines:
        if not member(g, k[0]) or len(ent_lines[k]) < 2:
            continue
        ks = sorted(ent_lines[k]); lab = {int(r["k"]): r for r in by[k[0]]}
        if any(lab[j]["label"] in "SC" and lab[j]["entry"] != str(k[1]) for j in range(ks[0], ks[-1] + 1) if j not in ks):
            n += 1
    return n
row("entries with a gap in k (strict)", nl)
row("  of which another entry sits in the gap", nl_inter)
row("entries with >=2 lines", lambda g: sum(1 for k in ent_lines if member(g, k[0]) and len(ent_lines[k]) >= 2))

# ---- locator styles from the text
print("\n== locator styles (character after each gold locator in the line text) ==")
sty = {g: Counter() for g in GROUPS}
nvide = Counter()
for rid in index_ids:
    r0 = reads[rid]
    txt = {o[1]: o[2]["text"].strip() for o in side_lines(r0["inv"], r0["scan_id"], r0["page"])}
    for r in by[rid]:
        t = txt.get(r["line_id"], ""); pos = 0
        for tok in filter(None, r["locators"].split(";")):
            n = tok.split("@")[0]
            m = re.compile(r"(?<!\d)" + n + r"(?!\d)").search(t, pos)
            if not m:
                m = re.compile(n).search(t, pos)
            if not m:
                kind = "not found in text"
            else:
                pos = m.end(); nxt = t[m.end():m.end() + 1]
                kind = {":": "n:", ".": "n.", ",": "n,", "": "n (line end)"}.get(nxt, "n + other" if nxt not in " " else "n + other")
                if nxt == " ":
                    after = t[m.end():].lstrip()[:1]
                    kind = {"": "n (line end)", ",": "n,", ":": "n:", ".": "n."}.get(after, "n + other")
            for g in GROUPS:
                if member(g, rid):
                    sty[g][kind] += 1
        if "vide" in r["note"].lower() and r["locators"]:
            for g in GROUPS:
                if member(g, rid):
                    sty[g]["(lines with 'vide' note and locator)"] += 1
for k in sorted({k for c in sty.values() for k in c}):
    row(k, lambda g, k=k: sty[g][k])

# ---- non-locator numbers by kind (from the note column: a lower bound, not exhaustive)
print("\n== non-locator numbers recorded in line notes (lines mentioning the kind; lower bound) ==")
KINDS = {"amount": r"amount", "count": r"\bcount", "ordinal": r"ordinal", "date": r"\bdate", "year": r"\byear",
         "garbled": r"garbled", "glued": r"glued"}
for kn, rx in KINDS.items():
    row(kn, lambda g, rx=rx: sum(1 for r in gold if member(g, r["read_id"]) and re.search(rx, r["note"], re.I)))

# ---- targets
print("\n== targets (target_gold.tsv) ==")
tg_by = {t["target_id"]: t for t in tg}
ok = lambda t: t["target_scan_id"] != "none" and t["folio_evidence"] != "unclear"
for t in tgt:
    t["_ok"] = ok(t)
row("targets", lambda g: sum(1 for t in tgt if member(g, t["read_id"])))
row("usable (scan != none, evidence != unclear)", lambda g: sum(1 for t in tgt if member(g, t["read_id"]) and t["_ok"]))
for fe in ("printed", "neighbours", "unclear"):
    row(f"folio_evidence {fe}", lambda g, fe=fe: sum(1 for t in tgt if member(g, t["read_id"]) and t["folio_evidence"] == fe))
row("target_scan_id none", lambda g: sum(1 for t in tgt if member(g, t["read_id"]) and t["target_scan_id"] == "none"))
def mean_open(g):
    v = [int(t["scans_opened"]) for t in tgt if member(g, t["read_id"]) and t["scans_opened"].isdigit()]
    return f"{sum(v) / len(v):.2f}" if v else "-"
row("mean scans_opened", mean_open)
for c in ("leaf", "opening only", "next only", "absent", "unclear"):
    row(f"content {c}", lambda g, c=c: sum(1 for t in tgt if member(g, t["read_id"]) and t["content"] == c))
print("content_sides (exact combination; usable targets with a scan):")
combos = sorted({t["content_sides"] for t in tgt if t["target_scan_id"] != "none"}, key=lambda x: (x.count(";"), x))
for c in combos:
    row(f"  {c or '(none)'}", lambda g, c=c: sum(1 for t in tgt if member(g, t["read_id"]) and t["target_scan_id"] != "none" and t["content_sides"] == c))
print("content_sides (any mention):")
for s in ("opening", "front", "back", "next"):
    row(f"  mentions {s}", lambda g, s=s: sum(1 for t in tgt if member(g, t["read_id"]) and t["target_scan_id"] != "none" and s in t["content_sides"].split(";")))
def share(g, pred):
    u = [t for t in tgt if member(g, t["read_id"]) and t["_ok"]]
    return f"{sum(1 for t in u if pred(t))}/{len(u)}={sum(1 for t in u if pred(t)) / len(u):.3f}" if u else "-"
print("index accuracy, over usable targets (matter found at the named folio):")
row("  leaf (front or back of folio N)", lambda g: share(g, lambda t: t["content"] == "leaf"))
row("  matter at front", lambda g: share(g, lambda t: "front" in t["content_sides"].split(";")))
row("  matter at back", lambda g: share(g, lambda t: "back" in t["content_sides"].split(";")))
row("  matter on next folio recto", lambda g: share(g, lambda t: "next" in t["content_sides"].split(";")))
row("  matter only on back/next (not on front)", lambda g: share(g, lambda t: t["content_sides"] and "front" not in t["content_sides"].split(";")))
row("  absent", lambda g: share(g, lambda t: t["content"] == "absent"))
print("\nlocator-position check: targets.tsv rows", len(tg), "= target_gold rows", len(tgt))
