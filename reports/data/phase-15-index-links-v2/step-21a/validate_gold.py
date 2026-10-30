"""Acceptance-1 check of line_gold.tsv / side_verdicts.tsv. Usage: validate_gold.py [READ_ID ...]"""
import csv
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import read_row, side_lines

D = os.path.dirname(os.path.abspath(__file__))
rd = lambda n: list(csv.DictReader(open(f"{D}/{n}", newline=""), delimiter="\t"))
verd, gold = rd("side_verdicts.tsv"), rd("line_gold.tsv")
only = set(sys.argv[1:])
LAB = {"S", "C", "H", "F", "X"}
bad = []
by = defaultdict(list)
for r in gold:
    by[r["read_id"]].append(r)
idx = {v["read_id"] for v in verd if v["side_verdict"] == "index"}
for rid in sorted(idx | set(by)):
    if only and rid not in only:
        continue
    if rid not in idx:
        bad.append(f"{rid}: gold rows but verdict is not index"); continue
    ls = by.get(rid, [])
    if not ls:
        bad.append(f"{rid}: index side with no gold rows"); continue
    ro = read_row(rid)
    n = len(side_lines(ro["inv"], ro["scan_id"], ro["page"]))
    ks = [int(r["k"]) for r in ls]
    if ks != list(range(1, n + 1)):
        bad.append(f"{rid}: k not 1..{n} ({len(ks)} rows)")
    s_cnt, ents = Counter(), set()
    for r in ls:
        if r["label"] not in LAB:
            bad.append(f"{rid} k={r['k']}: bad label {r['label']!r}")
        if r["col"] not in {"L", "R", "S"}:
            bad.append(f"{rid} k={r['k']}: bad col {r['col']!r}")
        if r["label"] in {"S", "C"}:
            if not r["entry"]:
                bad.append(f"{rid} k={r['k']}: {r['label']} without entry")
            else:
                ents.add(r["entry"])
                if r["label"] == "S":
                    s_cnt[r["entry"]] += 1
        elif r["entry"]:
            bad.append(f"{rid} k={r['k']}: {r['label']} has entry")
    for e in ents:
        if s_cnt[e] != 1:
            bad.append(f"{rid}: entry {e} has {s_cnt[e]} S lines")
    if ents and {int(e) for e in ents} != set(range(1, len(ents) + 1)):
        bad.append(f"{rid}: entry ids not 1..{len(ents)}")
    for r in ls:
        for tok in filter(None, r["locators"].split(";")):
            if "@" in tok:
                num, e = tok.split("@")
                if e not in ents or not num.isdigit():
                    bad.append(f"{rid} k={r['k']}: bad @ token {tok}")
            elif not tok.isdigit():
                bad.append(f"{rid} k={r['k']}: non-numeric locator {tok!r}")
    nloc = sum(len([t for t in r["locators"].split(";") if t]) for r in ls)
    nl = Counter(r["label"] for r in ls)
    print(f"{rid}: lines={n} entries={len(ents)} H={nl['H']} F={nl['F']} X={nl['X']} locators={nloc} R-lines={sum(r['col']=='R' for r in ls)}")
emb = rd("embedded_entries.tsv") if os.path.exists(f"{D}/embedded_entries.tsv") else []
gk = {(r["read_id"], r["k"]): r for r in gold}
for e in emb:
    if only and e["read_id"] not in only:
        continue
    g = gk.get((e["read_id"], e["k"]))
    if not g:
        bad.append(f"embedded {e['read_id']} k={e['k']}: no line_gold row")
        continue
    if g["line_id"] != e["line_id"]:
        bad.append(f"embedded {e['read_id']} k={e['k']}: line_id mismatch")
    if "merged columns" not in g["note"]:
        bad.append(f"embedded {e['read_id']} k={e['k']}: line note lacks 'merged columns'")
    toks = [t for t in e["locators"].split(";") if t]
    if not toks or not all(t.isdigit() for t in toks):
        bad.append(f"embedded {e['read_id']} k={e['k']}: locators not integers {e['locators']!r}")
print(f"embedded entries: {len(emb)}, locators: {sum(len([t for t in e['locators'].split(';') if t]) for e in emb)}")

# --- target_gold.tsv against targets.tsv (added in session 7) ---
tg = rd("targets.tsv") if os.path.exists(f"{D}/targets.tsv") else []
gg = rd("target_gold.tsv") if os.path.exists(f"{D}/target_gold.tsv") else []
if not only:
    ids = [r["target_id"] for r in gg]
    if ids != [r["target_id"] for r in tg]:
        bad.append(f"target_gold ids differ from targets.tsv ({len(ids)} vs {len(tg)}) or are not in order")
    if len(set(ids)) != len(ids):
        bad.append("target_gold has duplicate target_ids")
    if ids != [f"T{i:03d}" for i in range(1, len(ids) + 1)]:
        bad.append("target_gold ids are not T001..Tn")
    tgd = {r["target_id"]: r for r in tg}
    for r in gg:
        t = tgd.get(r["target_id"])
        if not t:
            continue
        for c in ("read_id", "split", "inv", "entry", "target_locator"):
            if r[c] != t[c]:
                bad.append(f"{r['target_id']}: {c} {r[c]!r} != targets.tsv {t[c]!r}")
        if r["folio_evidence"] not in {"printed", "neighbours", "unclear"}:
            bad.append(f"{r['target_id']}: bad folio_evidence {r['folio_evidence']!r}")
        if r["content"] not in {"leaf", "opening only", "next only", "absent", "unclear", ""}:
            bad.append(f"{r['target_id']}: bad content {r['content']!r}")
        for s_ in filter(None, r["content_sides"].split(";")):
            if s_ not in {"opening", "front", "back", "next"}:
                bad.append(f"{r['target_id']}: bad content_side {s_!r}")
        if r["target_scan_id"] == "none" and "beyond volume" not in r["note"]:
            bad.append(f"{r['target_id']}: scan none without 'beyond volume'")
        if len(r["evidence"]) > 80:
            bad.append(f"{r['target_id']}: evidence > 80 chars")
    print(f"target_gold: {len(gg)} rows vs targets.tsv {len(tg)} rows")
for v in verd:
    if v["side_verdict"] not in {"index", "dated index", "not index", "unreadable input"}:
        bad.append(f"bad verdict {v}")
print("OK" if not bad else "\n".join(bad))
sys.exit(1 if bad else 0)
