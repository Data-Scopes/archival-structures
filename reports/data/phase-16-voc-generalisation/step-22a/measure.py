"""Step 22a measurements on the VOC layer. Run from the AS root:
.venv/bin/python reports/data/phase-16-voc-generalisation/step-22a/measure.py
Writes modes/sides/types/laypa_pn/breaks/index_flags .tsv and summary.json next to this file."""
import collections
import csv
import gzip
import json
import re
import statistics
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
LAYER = ROOT / "data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5"
SGREF = ROOT / "data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5"
PLAN = ROOT / "reports/data/phase-16-voc-generalisation/planning"
GLOB = set("1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split())
BREAKS = "¬-=„:"


def invs():
    return (OUT / "inventories.txt").read_text().split()


def write(name, header, rows):
    with open(OUT / name, "w") as f:
        f.write("\t".join(header) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")


def quart(v):
    v = sorted(v)
    if not v:
        return {}
    q = statistics.quantiles(v, n=4) if len(v) > 1 else [v[0]] * 3
    return {"n": len(v), "min": v[0], "q1": q[0], "median": q[1], "q3": q[2], "max": v[-1]}


def main():
    inv_list = invs()
    gm = {r["inv"]: r["layout"] for r in csv.DictReader(open(PLAN / "gm_split.tsv"), delimiter="\t")}
    summ = {}
    # modes
    rows, boxes = [], {"GM": [], "GLOBALISE": []}
    modes = collections.Counter()
    for i in inv_list:
        m = json.load(open(LAYER / "mode" / f"{i}.json"))
        grp = "GLOBALISE" if i in GLOB else "GM"
        rows.append((i, grp, m["mode"], m.get("value"), m.get("n_chars"), m.get("scan_minority_share")))
        modes[(grp, m["mode"])] += 1
        if m.get("value") is not None:
            boxes[grp].append(m["value"])
    write("modes.tsv", ["inv", "group", "mode", "value", "n_chars", "scan_minority_share"], rows)
    summ["modes"] = {f"{g}|{m}": n for (g, m), n in modes.items()}
    summ["box_height"] = {g: quart(v) for g, v in boxes.items()}
    # sides
    rows, agree, dis = [], collections.Counter(), []
    for i in inv_list:
        c = collections.Counter()
        p = LAYER / "layout/pages" / f"{i}.tsv.gz"
        if p.exists():
            for r in read_tsv_gz(p):
                c[r["page"]] += 1
        else:
            for r in read_tsv_gz(LAYER / "layout/lines" / f"{i}.tsv.gz"):
                c[r["page"]] += 1
        tot = sum(c.values()) or 1
        lay = gm.get(i, "")
        ok = ""
        if lay:
            sh = c["single"] / tot
            dh = (c["verso"] + c["recto"]) / tot
            ok = int((lay == "Enkel" and sh >= .9) or (lay == "Dubbel" and dh >= .9))
            agree[(lay, ok)] += 1
            if not ok:
                dis.append(i)
        rows.append((i, c["single"], c["verso"], c["recto"], lay, ok))
    write("sides.tsv", ["inv", "single", "verso", "recto", "gm_layout", "agrees"], rows)
    summ["sides_agree"] = {f"{k[0]}|{k[1]}": v for k, v in agree.items()}
    summ["sides_disagree"] = dis
    # types (+ SG reference) and page numbers
    pool = collections.Counter()
    rows = []
    for i in inv_list:
        c = collections.Counter()
        for r in read_tsv_gz(LAYER / "types/segments" / f"{i}.tsv.gz"):
            c[f"{r['line_type']}|{r['type_method']}"] += 1
        pool.update(c)
        for k, v in c.items():
            rows.append((i, "voc", k, v))
    ref = collections.Counter()
    st = json.load(open(SGREF / "types/stats.json"))
    nhand = 0
    for i, s in st.items():
        mp = SGREF / "mode" / f"{i}.json"
        if mp.exists() and json.load(open(mp)).get("mode") == "hand":
            nhand += 1
            ref.update(s.get("methods", {}))
    for k, v in pool.items():
        rows.append(("POOLED", "voc", k, v))
    for k, v in ref.items():
        rows.append(("POOLED", "sg_hand_ref", k, v))
    write("types.tsv", ["inv", "source", "line_type|type_method", "n_segments"], rows)
    lt = lambda c: collections.Counter(k.split("|")[0] for k in c for _ in range(c[k]))
    summ["types_voc_share"] = {k: round(v / sum(pool.values()), 4) for k, v in lt(pool).items()}
    summ["types_sg_share"] = {k: round(v / sum(ref.values()), 4) for k, v in lt(ref).items()}
    summ["pn_methods_voc"] = {k: v for k, v in pool.items() if k.startswith("page_number")}
    summ["pn_methods_sg"] = {k: v for k, v in ref.items() if k.startswith("page_number")}
    summ["n_sg_hand_ref"] = nhand
    # laypa page-number agreement
    rows, TP = [], [0, 0, 0]
    for i in inv_list:
        lp = set()
        for r in read_tsv_gz(LAYER / "regions_meta" / f"{i}.tsv.gz"):
            if r["region_tag"] == "page-number":
                lp.add(r["region_id"])
        lreg = {}
        for r in read_tsv_gz(LAYER / "lines" / f"{i}.lines.tsv.gz"):
            lreg[r["line_id"]] = r["region_id"]
        pn = {r["line_id"] for r in read_tsv_gz(LAYER / "types/segments" / f"{i}.tsv.gz")
              if r["line_type"] == "page_number"}
        inl = {l for l, rg in lreg.items() if rg in lp}
        tp, fp, fn = len(pn & inl), len(pn - inl), len(inl - pn)
        TP[0] += tp; TP[1] += fp; TP[2] += fn
        rows.append((i, len(lp), tp, fp, fn))
    write("laypa_pn.tsv", ["inv", "n_laypa_pn_regions", "tp", "fp", "fn"], rows)
    P = TP[0] / max(1, TP[0] + TP[1]); R = TP[0] / max(1, TP[0] + TP[2])
    summ["laypa_pn"] = {"tp": TP[0], "fp": TP[1], "fn": TP[2], "P": round(P, 4), "R": round(R, 4)}
    # breaks
    rows, tot, ntot, unj, nunits = [], collections.Counter(), 0, 0, 0
    for i in inv_list:
        c, n = collections.Counter(), 0
        for r in read_tsv_gz(LAYER / "lines" / f"{i}.lines.tsv.gz"):
            t = r["text"].rstrip()
            if not t:
                continue
            n += 1
            if t[-1] in BREAKS:
                c[t[-1]] += 1
        u = 0
        for r in read_tsv_gz(LAYER / "join/units" / f"{i}.tsv.gz"):
            u += len(re.findall(r"¬(?=\s|$)", r["text"]))   # a ¬ still followed by a space or the unit end
        tot.update(c); ntot += n; unj += u
        rows.append((i, n, *[c[ch] for ch in BREAKS], u, round(1000 * u / max(1, n), 2)))
    write("breaks.tsv", ["inv", "n_lines_text"] + [f"end_{ch}" for ch in BREAKS] + ["unjoined_¬_in_join", "per_1000_lines"], rows)
    summ["breaks"] = {"n_lines": ntot, **{ch: tot[ch] for ch in BREAKS}, "unjoined_not": unj,
                      "unjoined_per_1000": round(1000 * unj / max(1, ntot), 3)}
    # index flags
    gold = {}
    for r in csv.DictReader(open(PLAN / "toc_gold.tsv"), delimiter="\t"):
        gold[r["scan_id"]] = r["toc"] == "1"
    rows, T = [], [0, 0, 0, 0]
    for i in sorted(GLOB, key=int):
        flag = {r["scan_id"] for r in read_tsv_gz(LAYER / "layout/pages" / f"{i}.tsv.gz") if r["index_side"] == "1"}
        g = {s for s, v in gold.items() if v and s.split("_")[2] == i}
        allg = {s for s in gold if s.split("_")[2] == i}
        tp, fp, fn = len(flag & g), len(flag - g), len(g - flag)
        T[0] += tp; T[1] += fp; T[2] += fn; T[3] += len(g)
        rows.append((i, len(allg), len(g), len(flag), tp, fp, fn))
    write("index_flags.tsv", ["inv", "n_gold_scans", "n_toc", "n_flagged_scans", "tp", "fp", "fn"], rows)
    summ["index_flags"] = {"tp": T[0], "fp": T[1], "fn": T[2], "toc_total": T[3]}
    json.dump(summ, open(OUT / "summary.json", "w"), indent=1, ensure_ascii=False)
    print(json.dumps(summ, indent=1, ensure_ascii=False))


main()
