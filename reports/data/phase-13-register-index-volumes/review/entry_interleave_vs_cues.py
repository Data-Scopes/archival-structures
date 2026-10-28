"""Reviewer diagnostic: E5 errors in column-mixed vs clean regions of the 19c clones, and E5 on a geometric
column re-split of the regions (2-means on line start x per side; no labels used)."""
import sys, json, statistics
from collections import defaultdict, Counter
from pathlib import Path
sys.path.insert(0, "reports/data/phase-13-register-index-volumes/step-19d")
import entry_eval as ee
from archival_structures.text_layer.units import UnitParams, build_units_page

split = sys.argv[1]
out = Path(sys.argv[2])
sides, gold = ee.load_sides(split)
cache = ee.Cache()
E5 = ee.variants()["E5"]
from archival_structures.text_layer.presets import SG_INDEX_DATE_START as DP

def two_means(xs):
    xs = sorted(xs)
    best = None
    for i in range(1, len(xs)):
        a, b = xs[:i], xs[i:]
        ma, mb = sum(a)/len(a), sum(b)/len(b)
        sse = sum((x-ma)**2 for x in a) + sum((x-mb)**2 for x in b)
        if best is None or sse < best[0]:
            best = (sse, ma, mb, xs[i])
    return best

def side_regions(side):
    regions_by, segs_by = cache.get(ee.CLONE[side["layer"]], side["inv"])
    sid, page = side["scan_id"], side["page"]
    regs = []
    for r in regions_by[(sid, page)]:
        reg = {"layout_region_id": r["layout_region_id"], "layout_role": r["layout_role"],
               "layout_column": int(r["layout_column"]) if r["layout_column"] else 0}
        regs.append((reg, segs_by.get((sid, page, r["layout_region_id"]), [])))
    return regs

def predict(regs, side, cfg=None):
    units, _ = build_units_page(side["scan_id"], side["page"], regs, UnitParams(index_entry=cfg or E5), False,
                                taken=set(), claimable=None, index_side=True)
    units.sort(key=lambda u: u["_key"])
    return ee.starts_from_units([{"unit_type": u["unit_type"], "unit_method": u["unit_method"],
                                  "layout_column": u["layout_column"], "first_line_id": u["segs"][0]["line_id"],
                                  "lines": {s["line_id"] for s in u["segs"]}} for u in units])

rows = []
VAR = {}
resplit_preds = []
side_info = []
for side in sides:
    regs = side_regions(side)
    # line geometry (first seg per line; x = min seg x, right = max seg x+w)
    geo = {}
    for reg, segs in regs:
        for s in segs:
            g = geo.setdefault(s["line_id"], {"x": s["x"], "r": s["x"]+s["w"], "y": s["y"], "h": s["h"],
                                              "reg": reg["layout_region_id"], "types": [], "text": ""})
            g["x"] = min(g["x"], s["x"]); g["r"] = max(g["r"], s["x"]+s["w"])
            g["types"].append(s["line_type"]); g["text"] += s.get("text", "")
    lines8 = [g for g in geo.values() if len(g["text"].strip()) >= 8]
    xs = [g["x"] for g in lines8]
    sx0 = min(g["x"] for g in geo.values()); sx1 = max(g["r"] for g in geo.values())
    W = sx1 - sx0
    mh = statistics.median(g["h"] for g in geo.values())
    _, ma, mb, cutx = two_means(xs)
    two = (mb - ma) >= 0.25 * W and min(sum(x < cutx for x in xs), sum(x >= cutx for x in xs)) >= 5
    for g in geo.values():
        g["col"] = ("R" if g["x"] >= cutx else "L") if two else "L"
        # a line that reaches well into the right column although it starts left: span
        g["span"] = two and g["col"] == "L" and g["r"] > cutx + 0.5*(mb - ma) + 0*mh
    # mixed region = >= 2 main lines in each column
    reg_cols = defaultdict(Counter)
    for g in geo.values():
        if "main_text" in g["types"]:
            reg_cols[g["reg"]][g["col"]] += 1
    mixed = {rid for rid, c in reg_cols.items() if c["L"] >= 2 and c["R"] >= 2}
    mixed1 = {rid for rid, c in reg_cols.items() if c["L"] >= 1 and c["R"] >= 1}
    # previous main segment in region order, per line
    prevm = {}
    import re as _re
    for reg, segs in regs:
        mains = [s for s in segs if s["line_type"] == "main_text"]
        for i, m in enumerate(mains):
            if m["line_id"] in prevm: continue
            prevm[m["line_id"]] = (mains[i-1] if i else None, i, mains)
    starts, covered = predict(regs, side)
    # re-split: mixed regions -> L piece + R piece, each sorted by y (row order); others unchanged
    new = []
    for reg, segs in regs:
        if reg["layout_region_id"] in mixed:
            for c, off in (("L", 0.0), ("R", 0.5)):
                ps = sorted([s for s in segs if geo[s["line_id"]]["col"] == c],
                            key=lambda s: (geo[s["line_id"]]["y"], s["layout_line_idx"], s["seg_idx"]))
                if ps:
                    new.append(({**reg, "layout_region_id": reg["layout_region_id"] + c,
                                 "layout_column": reg["layout_column"] + off}, ps))
        else:
            new.append((reg, segs))
    rstarts, rcovered = predict(new, side)
    reo = []
    for reg, segs in regs:
        if reg["layout_region_id"] in mixed:
            key = lambda s: (geo[s["line_id"]]["col"] == "R", geo[s["line_id"]]["y"], s["layout_line_idx"], s["seg_idx"])
            reo.append((reg, sorted(segs, key=key)))
        else:
            reo.append((reg, segs))
    for vk in ("E3", "E4"):
        VAR.setdefault(vk + "reo", {"orig": [], "resplit": []})
        VAR[vk + "reo"]["resplit"].append((side, *predict(reo, side, ee.variants()[vk])))
    for vk, vc in ee.variants().items():
        VAR.setdefault(vk, {"orig": [], "resplit": []})
        VAR[vk]["orig"].append((side, *predict(regs, side, vc)))
        VAR[vk]["resplit"].append((side, *predict(new, side, vc)))
    resplit_preds.append((side, rstarts, rcovered))
    nm = sum(1 for lid in gold[side["read_id"]] if lid in geo and geo[lid]["reg"] in mixed)
    side_info.append({"read_id": side["read_id"], "group": side["group"], "inv": side["inv"], "scan": side["scan_id"][-4:],
                      "page": side["page"], "two_cols": two, "n_regions": len(regs), "mixed_regions": len(mixed),
                      "gold_lines": len(gold[side["read_id"]]), "gold_in_mixed": nm})
    for lid, lab in gold[side["read_id"]].items():
        g = geo.get(lid)
        rows.append({"read_id": side["read_id"], "group": side["group"], "line_id": lid, "label": lab,
                     "mixed": bool(g and g["reg"] in mixed), "mixed1": bool(g and g["reg"] in mixed1), "col": g["col"] if g else "",
                     "pos": prevm[lid][1] if lid in prevm else "",
                     "prev_text": (prevm[lid][0]["text"][-30:] if lid in prevm and prevm[lid][0] else ""),
                     "outdent_h": (round((prevm[lid][0]["x"] - g["x"]) / g["h"], 2) if lid in prevm and prevm[lid][0] else ""),
                     "prev_ends_num": bool(lid in prevm and prevm[lid][0] and _re.search(r"\d+\s*[.,:;]?\s*$", prevm[lid][0]["text"].strip())),
                     "date_start": bool(g and _re.match(DP, g["text"].strip(), _re.I)),
                     "E5": starts.get(lid, "-" if lid in covered else "nounit"),
                     "resplit": rstarts.get(lid, "-" if lid in rcovered else "nounit"),
                     "E4rs": VAR["E4"]["resplit"][-1][1].get(lid, "-" if lid in VAR["E4"]["resplit"][-1][2] else "nounit"),
                     "E4o": VAR["E4"]["orig"][-1][1].get(lid, "-" if lid in VAR["E4"]["orig"][-1][2] else "nounit"),
                     "E3o": VAR["E3"]["orig"][-1][1].get(lid, "-" if lid in VAR["E3"]["orig"][-1][2] else "nounit"),
                     "E3reo": VAR["E3reo"]["resplit"][-1][1].get(lid, "-" if lid in VAR["E3reo"]["resplit"][-1][2] else "nounit"),
                     "E4reo": VAR["E4reo"]["resplit"][-1][1].get(lid, "-" if lid in VAR["E4reo"]["resplit"][-1][2] else "nounit"),
                     "E3rs": VAR["E3"]["resplit"][-1][1].get(lid, "-" if lid in VAR["E3"]["resplit"][-1][2] else "nounit"),
                     "types": ",".join(sorted(set(g["types"]))) if g else "", "x": g["x"] if g else "",
                     "text": (g["text"][:60] if g else "")})

def prf(rs, key):
    tp = sum(1 for r in rs if r["label"] == "S" and r[key] not in ("-", "nounit"))
    fn = sum(1 for r in rs if r["label"] == "S" and r[key] in ("-", "nounit"))
    fnn = sum(1 for r in rs if r["label"] == "S" and r[key] == "nounit")
    fp = sum(1 for r in rs if r["label"] == "C" and r[key] not in ("-", "nounit"))
    p = tp/(tp+fp) if tp+fp else 0; rc = tp/(tp+fn) if tp+fn else 0
    f = 2*p*rc/(p+rc) if p+rc else 0
    return f"S {tp+fn:4d} C {sum(1 for r in rs if r['label']=='C'):4d} tp {tp:4d} fp {fp:3d} fn {fn:3d} (nounit {fnn:2d}) P {p:.4f} R {rc:.4f} F1 {f:.4f}"

print("== sides ==")
for s in side_info: print(s)
for name, sel in [("all", lambda r: True), ("mixed", lambda r: r["mixed"]), ("clean", lambda r: not r["mixed"])]:
    rs = [r for r in rows if sel(r)]
    print(f"E5      {name:6s}", prf(rs, "E5"))
    print(f"resplit {name:6s}", prf(rs, "resplit"))
rs = [r for r in rows if r["E5"] != "nounit" or r["label"] != "S"]
print("E5 excl. nounit S:", prf(rs, "E5"))
print("resplit excl. nounit S:", prf([r for r in rows if r["resplit"] != "nounit" or r["label"] != "S"], "resplit"))
for g in sorted({r["group"] for r in rows}):
    rs = [r for r in rows if r["group"] == g]
    print(g, "E5", prf(rs, "E5")); print(g, "RS", prf(rs, "resplit"))
import csv
with open(out, "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t"); w.writeheader(); w.writerows(rows)

print("== post-hoc variants (diagnostic only) ==")
for vk in VAR:
    for kind in ("orig", "resplit"):
        if not VAR[vk][kind]: continue
        m = ee.score(VAR[vk][kind], gold)["micro"]
        print(f"{vk} {kind:8s} P {m['P']:.4f} R {m['R']:.4f} F1 {m['F1']:.4f} tp {m['tp']} fp {m['fp']} fn {m['fn']} nounit {m['fn_nounit']}")
