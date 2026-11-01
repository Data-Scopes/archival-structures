"""Step 22b: the folio map on the VOC Generale Missiven gold. Single process. Run from the AS root:

    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/eval_gm.py --stage dev      # dev_grid.tsv, select.json
    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/eval_gm.py --stage test     # ONCE: test_done.json, gold_eval.tsv
    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/eval_gm.py --stage archive  # volumes/loo/restarts/rectos
    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/eval_gm.py --stage context  # text dumps for reading misses

Reads (read only): data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5 (a link to T7_Data),
planning/gm_anchors.tsv (frozen gold), planning/gm_split.tsv, step-22a/sides.tsv. Writes only into this directory.
"""
import argparse
import csv
import dataclasses
import datetime
import json
import math
import random
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, ".")
from archival_structures.analysis.folio_map import FOLIO_MAP_V2, FolioMapParams, build_folio_map  # noqa: E402
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs  # noqa: E402
from archival_structures.analysis.page_numbering import (  # noqa: E402
    interpolate_folio_to_scan, main_chain, numbering_restarts)
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

LAYER = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
PLAN = Path("reports/data/phase-16-voc-generalisation/planning")
SIDES = Path("reports/data/phase-16-voc-generalisation/step-22a/sides.tsv")
OUT = Path(__file__).parent
SEED = 20261031
V0, V2 = FolioMapParams(), FOLIO_MAP_V2
MS = {"V0": V0, "V2": V2}
TAGS = {"T": (), "TL": ("page-number",)}
CELLS = [(m, s, n) for m in ("V0", "V2") for s in ("off", "on") for n in ("T", "TL")]
UNCHANGED = [("V0", "off", "T"), ("V2", "off", "T")]
DECREASING = ["1269", "1279", "1341", "1363", "1408", "1614", "1696", "2394", "2550", "2808", "3987"]


def cname(c):
    return "-".join(c)


def params(c):
    p = MS[c[0]]
    return dataclasses.replace(p, single_foliation=True) if c[1] == "on" else p


def wilson(k, n, z=1.96):
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def loo(chain):
    ok = tot = 0
    for i in range(1, len(chain) - 1):
        s, _, _ = interpolate_folio_to_scan(chain[i, 1], np.delete(chain, i, axis=0))
        tot += 1
        ok += math.floor(s + 0.5) == int(chain[i, 0])
    return ok, tot


def read_tsv(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def side_layouts():
    out = {}
    for r in read_tsv(SIDES):
        s, v, rc = int(r["single"]), int(r["verso"]), int(r["recto"])
        out[r["inv"]] = "openings" if (v + rc) >= 0.5 * (s + v + rc) else "single"
    return out


class Vols:
    """Loads each (inv, N variant) once."""

    def __init__(self):
        self.cache = {}

    def get(self, inv, n):
        key = (inv, n)
        if key not in self.cache:
            try:
                v = read_volume_inputs(LAYER, inv, number_tags=TAGS[n])
            except FileNotFoundError:
                self.cache[key] = None
                return None
            run = front_index_run(v.flags)
            excl = {s for i, s in enumerate(v.sides) if (run and i <= run[1]) or v.flags[i]}
            self.cache[key] = (v, excl)
        return self.cache[key]


def gold_rows(split):
    sp = {r["inv"]: r["split"] for r in read_tsv(PLAN / "gm_split.tsv")}
    return [r for r in read_tsv(PLAN / "gm_anchors.tsv") if sp[r["inv"]] == split]


def evaluate(rows, cells, vols, slay):
    """-> (detail rows, counts of unreadable anchors)."""
    det, unread = [], 0
    maps = {}
    for r in rows:
        inv = r["inv"]
        gold = f"NL-HaNA_1.04.02_{inv}_{int(r['scan']):04d}"
        loaded0 = vols.get(inv, "T")
        if loaded0 is not None and gold not in set(loaded0[0].scan_ids):
            unread += 1
            continue
        for c in cells:
            base = dict(inv=inv, split=r["split"], layout=r["layout"], side_layout=slay.get(inv, "?"), kind=r["kind"],
                        folio=int(r["folio"]), gold_scan=gold, cell=cname(c))
            ld = vols.get(inv, c[2])
            if ld is None:
                det.append({**base, "pred_scan": "", "method": "no_pages", "confidence": "", "leaf": 0, "strict": 0})
                continue
            v, excl = ld
            if (inv, c) not in maps:
                maps[(inv, c)] = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=params(c))
            t = maps[(inv, c)].locate(base["folio"])
            if t is None:
                det.append({**base, "pred_scan": "", "method": "none", "confidence": "", "leaf": 0, "strict": 0})
                continue
            pos = v.scan_ids.index(t.front[0])
            leaf = {v.scan_ids[pos]} | ({v.scan_ids[pos + 1]} if pos + 1 < len(v.scan_ids) else set())
            det.append({**base, "pred_scan": t.front[0], "method": t.method, "confidence": t.confidence,
                        "leaf": int(gold in leaf), "strict": int(gold == t.front[0])})
    return det, unread


def share(rows, key=None, val=None):
    sel = [r for r in rows if key is None or r[key] == val]
    n = len(sel)
    return sum(r["leaf"] for r in sel), sum(r["strict"] for r in sel), n


def fmt(k, n):
    return f"{k / n:.4f}" if n else "nan"


def write_tsv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        w.writerows(rows)


def stage_dev():
    t0 = time.time()
    slay, vols = side_layouts(), Vols()
    rows = gold_rows("dev")
    det, unread = evaluate(rows, CELLS, vols, slay)
    print(f"dev anchors {len(rows)}, unreadable {unread}, readable {len(rows) - unread}")
    glay = sorted({r["layout"] for r in rows})
    header = (["cell", "M", "S", "N", "n", "leaf", "leaf_share", "strict", "strict_share"]
              + [f"{k}_{lab}_{x}" for k in ("gm", "side") for lab in (glay if k == "gm" else ["openings", "single"])
                 for x in ("n", "leaf_share", "strict_share")])
    out, res = [], {}
    for c in CELLS:
        d = [r for r in det if r["cell"] == cname(c)]
        k, s, n = share(d)
        row = [cname(c), *c, n, k, fmt(k, n), s, fmt(s, n)]
        for key, labs in (("layout", glay), ("side_layout", ["openings", "single"])):
            for lab in labs:
                kk, ss, nn = share(d, key, lab)
                row += [nn, fmt(kk, nn), fmt(ss, nn)]
        out.append(row)
        res[c] = (k, s, n)
    write_tsv(OUT / "dev_grid.tsv", header, out)
    best = max(k / n for k, s, n in res.values())
    cand = [c for c in CELLS if res[c][0] / res[c][2] >= best - 0.002]
    base = ("V2", "off", "T")
    cand.sort(key=lambda c: (int(c[1] != base[1]) + int(c[2] != base[2]), c[0] != "V2"))
    sel = cand[0]
    json.dump(dict(cell=list(sel), name=cname(sel), dev_leaf=res[sel][0], dev_strict=res[sel][1], dev_n=res[sel][2],
                   dev_leaf_share=res[sel][0] / res[sel][2], best_dev_leaf_share=best, tie_candidates=[cname(c) for c in cand],
                   n_unreadable_dev=unread, seconds=round(time.time() - t0, 1),
                   time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")),
              open(OUT / "select.json", "w"), indent=1)
    write_tsv(OUT / "dev_eval_detail.tsv", list(det[0]), [list(r.values()) for r in det])
    for r in out:
        print("\t".join(map(str, r[:9])))
    print("selected", cname(sel), "candidates", [cname(c) for c in cand])


def stage_test():
    sel = json.load(open(OUT / "select.json"))
    if (OUT / "test_done.json").exists():
        sys.exit("test_done.json exists: test runs once")
    sc = tuple(sel["cell"])
    t0 = time.time()
    json.dump(dict(status="started", selected=cname(sc),
                   time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")),
              open(OUT / "test_done.json", "w"), indent=1)
    slay, vols = side_layouts(), Vols()
    rows = gold_rows("test")
    cells = list(dict.fromkeys(UNCHANGED + [sc]))
    det, unread = evaluate(rows, cells, vols, slay)
    write_tsv(OUT / "gold_eval.tsv", list(det[0]), [list(r.values()) for r in det])
    summ = {}
    for c in cells:
        d = [r for r in det if r["cell"] == cname(c)]
        k, s, n = share(d)
        lo, hi = wilson(k, n)
        e = dict(n=n, leaf=k, leaf_share=k / n, wilson_lo=lo, wilson_hi=hi, strict=s, strict_share=s / n,
                 bar_f_pass=k / n >= 0.95)
        for key in ("layout", "side_layout", "method"):
            e[key] = {}
            for lab in sorted({r[key] for r in d}):
                kk, ss, nn = share(d, key, lab)
                l2, h2 = wilson(kk, nn)
                e[key][lab] = dict(n=nn, leaf=kk, strict=ss, leaf_share=kk / nn, wilson=[l2, h2])
        summ[cname(c)] = e
    verdict = ("unchanged" if summ["V2-off-T"]["bar_f_pass"] else
               "reparameterised" if summ[cname(sc)]["bar_f_pass"] else "fails on VOC")
    json.dump(dict(status="done", selected=cname(sc), n_test_anchors=len(rows), n_unreadable=unread, verdict=verdict,
                   cells=summ, seconds=round(time.time() - t0, 1),
                   time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")),
              open(OUT / "test_done.json", "w"), indent=1)
    print(json.dumps({k: {a: b for a, b in v.items() if a in ("n", "leaf", "leaf_share", "wilson_lo", "wilson_hi", "strict")}
                      for k, v in summ.items()}, indent=1), verdict)


def scan_pos_of(v):
    return {s: i + 1 for i, s in enumerate(v.scan_ids)}


def chain_scan_positions(m, v):
    """chain positions as scan positions (foliation chains already are)."""
    if m.mode == "foliation":
        return m.chain[:, 0]
    sp = scan_pos_of(v)
    return np.array([sp[m.sides[int(p) - 1][0]] for p in m.chain[:, 0]], float)


def stage_archive():
    sel = tuple(json.load(open(OUT / "select.json"))["cell"])
    slay, vols = side_layouts(), Vols()
    split = {r["inv"]: r for r in read_tsv(PLAN / "gm_split.tsv")}
    invs = sorted(split, key=lambda i: (int("".join(ch for ch in i if ch.isdigit())), i))
    vrows, loorows, restarts = [], [], []
    anchors_by_inv = {}
    for inv in invs:
        base = vols.get(inv, "T")
        if base is None:
            vrows.append([inv, split[inv]["split"], split[inv]["layout"], slay.get(inv, "?"), "print"] + [""] * 8)
            continue
        v, excl = base
        mV2 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=V2)
        ld = vols.get(inv, sel[2])
        mS = build_folio_map(ld[0].scan_ids, ld[0].sides, ld[0].numbers, exclude=ld[1], params=params(sel))
        o, t = loo(mS.chain) if mS.mode != "none" else (0, 0)
        looi = [inv, split[inv]["split"], mS.mode, o, t]
        looi.append(len(mS.chain))
        loorows.append(looi)
        sc = chain_scan_positions(mS, ld[0]) if mS.mode != "none" else np.array([])
        slope = float(np.median(np.diff(sc) / np.diff(mS.chain[:, 1]))) if len(sc) > 2 else float("nan")
        # restarts: anchors on all numbered, non-excluded sides, scan positions
        sp, anc = scan_pos_of(v), []
        import re
        pat = re.compile(V2.number_pattern)
        for side in v.sides:
            if side in excl:
                continue
            for text in v.numbers.get(side, ()):
                m_ = pat.match(text.strip())
                if m_:
                    anc.append((sp[side[0]], int(m_.group(1))))
                    break
        anc = np.array(sorted(set(anc)), float).reshape(-1, 2)
        anchors_by_inv[inv] = anc
        n_runs, big = 0, []
        if len(anc) >= 3:
            _, keep = main_chain(anc)
            runs = numbering_restarts(anc, keep)
            big = [r for r in runs if len(r) >= 10]
            for r in big:
                span_s, span_n = int(r[-1, 0] - r[0, 0]), int(r[-1, 1] - r[0, 1])
                slope_r = span_s / span_n if span_n else float("nan")
                dens = len(r) / (span_s + 1)
                # post-hoc descriptive filter (not in the brief): a real restart is a compact run, not a scatter of
                # misreads across the volume
                coherent = int(0.5 <= slope_r <= 3 and dens >= 0.2)
                restarts.append([inv, split[inv]["layout"], slay.get(inv, "?"), len(r), int(r[0, 0]), int(r[-1, 0]),
                                 int(r[0, 1]), int(r[-1, 1]), len(anc), len(mV2.chain), span_s, span_n,
                                 f"{slope_r:.3f}", f"{dens:.3f}", coherent])
        vrows.append([inv, split[inv]["split"], split[inv]["layout"], slay.get(inv, "?"), len(v.scan_ids), len(v.sides),
                      mV2.n_anchors, f"{mV2.recto_share:.3f}" if mV2.n_anchors else "", mV2.mode, mS.mode,
                      len(mS.chain), f"{slope:.3f}", len(big)])
    write_tsv(OUT / "volumes.tsv", ["inv", "split", "gm_layout", "side_layout", "n_scans", "n_sides", "n_anchors",
                                    "recto_share_V2offT", "mode_V2offT", "mode_selected", "chain_len", "chain_slope_scans_per_number",
                                    "n_restart_runs_ge10"], vrows)
    write_tsv(OUT / "loo.tsv", ["inv", "split", "mode", "loo_hits", "loo_n", "chain_len"], loorows)
    write_tsv(OUT / "restarts.tsv", ["inv", "gm_layout", "side_layout", "run_len", "scan_first", "scan_last", "num_first",
                                     "num_last", "n_anchors", "chain_len_V2offT", "span_s", "span_n", "slope", "density", "coherent"], restarts)
    # pooled LOO, dev volumes (the brief) and all
    for lab, sel_ in (("dev", [r for r in loorows if r[1] == "dev"]), ("all", loorows)):
        o, t = sum(r[3] for r in sel_), sum(r[4] for r in sel_)
        print(f"LOO {lab}: {o}/{t} = {o / t:.4f} wilson {wilson(o, t)} over {sum(1 for r in sel_ if r[4])} volumes with chain")
    # rectos
    op = [r for r in vrows if r[3] == "openings" and r[7] != ""]
    shares = np.array([float(r[7]) for r in op])
    print(f"openings volumes with anchors: {len(op)}; recto_share quantiles 5/25/50/75/95:",
          np.round(np.percentile(shares, [5, 25, 50, 75, 95]), 3), f">=0.9: {(shares >= 0.9).sum()}",
          f"foliation mode: {sum(r[8] == 'foliation' for r in op)}", Counter(r[8] for r in op))
    k = n = 0
    per = []
    for inv in invs:
        if slay.get(inv) != "single" or vols.get(inv, "T") is None:
            continue
        v, excl = vols.get(inv, "T")
        m = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=V2)
        if m.mode == "none":
            continue
        sc = chain_scan_positions(m, v)
        d_n, d_s = np.diff(m.chain[:, 1]), np.diff(sc)
        one = d_n == 1
        kk, nn = int((d_s[one] == 2).sum()), int(one.sum())
        k += kk
        n += nn
        per.append((inv, kk, nn))
    print(f"single volumes: consecutive chain points one number apart that are exactly 2 scans apart: {k}/{n} = {k / n:.4f}; "
          f"volumes {len(per)}; volumes with share>=0.5: {sum(1 for _, a, b in per if b and a / b >= 0.5)}")
    # modes of the selected cell by side layout
    print(Counter((r[3], r[9]) for r in vrows))
    print(Counter((r[3], r[8]) for r in vrows))
    # decreasing parts
    gold = read_tsv(PLAN / "gm_anchors.tsv")
    lines = []
    for inv in DECREASING:
        g = sorted([(int(r["scan"]), int(r["folio"]), r["gm_id"], r["kind"]) for r in gold if r["inv"] == inv])
        dec = [(a, b) for a, b in zip(g, g[1:]) if b[1] < a[1]]
        lines.append(f"== {inv} side_layout={slay.get(inv)} gold sorted by scan: {g}")
        ld = vols.get(inv, "TL")
        if ld is None:
            continue
        v, excl = ld
        for a, b in dec:
            lines.append(f"-- decrease {a} -> {b}")
            for sid, page in v.sides:
                sn = int(sid.rsplit("_", 1)[1])
                if a[0] - 3 <= sn <= b[0] + 3 and (sn <= a[0] + 6 or sn >= b[0] - 6):
                    lines.append(f"   scan {sn} {page}: {v.numbers.get((sid, page), [])}")
        an = anchors_by_inv.get(inv)
        if an is not None and len(an):
            lines.append("   main chain / restart anchors (scan,num) head: " + str(an[:0]))
    open(OUT / "decreasing_context.txt", "w").write("\n".join(lines))


def stage_context():
    """Text dumps for reading misses: 40 dev misses (seed) and all test misses (<=30) of the selected cell."""
    sel = json.load(open(OUT / "select.json"))
    cname_ = sel["name"]
    vols = Vols()
    sc = tuple(sel["cell"])

    def dump(path, rows):
        L = []
        for i, r in enumerate(rows, 1):
            inv = r["inv"]
            ld = vols.get(inv, "TL")
            v, excl = ld
            gs = int(r["gold_scan"].rsplit("_", 1)[1])
            L.append(f"#{i} inv {inv} {r['layout']} {r['side_layout']} folio {r['folio']} gold {gs} pred "
                     f"{r['pred_scan'].rsplit('_', 1)[1] if r['pred_scan'] else '-'} method {r['method']} (n_scans {len(v.scan_ids)})")
            lo = min(gs, int(r["pred_scan"].rsplit("_", 1)[1]) if r["pred_scan"] else gs)
            m = build_folio_map(*[v.scan_ids, v.sides, v.numbers], exclude=excl, params=params(sc))
            L.append(f"   map mode {m.mode} chain_len {len(m.chain)} first/last {m.chain[0].tolist() if len(m.chain) else '-'} "
                     f"{m.chain[-1].tolist() if len(m.chain) else '-'}")
            for sid, page in v.sides:
                sn = int(sid.rsplit("_", 1)[1])
                if gs - 3 <= sn <= gs + 3 or (r["pred_scan"] and abs(sn - int(r["pred_scan"].rsplit("_", 1)[1])) <= 1):
                    L.append(f"   {sn}{'*' if sn == gs else ''} {page}{' EXCL' if (sid, page) in excl else ''}: {v.numbers.get((sid, page), [])}")
        open(path, "w").write("\n".join(L))

    dev = [r for r in read_tsv(OUT / "dev_eval_detail.tsv") if r["cell"] == cname_ and r["leaf"] == "0"]
    random.Random(SEED).shuffle(dev)
    dev = dev[:40]
    write_tsv(OUT / "dev_misses_sample.tsv", list(dev[0]), [list(r.values()) for r in dev])
    dump(OUT / "dev_misses_context.txt", dev)
    if (OUT / "gold_eval.tsv").exists():
        te = [r for r in read_tsv(OUT / "gold_eval.tsv") if r["cell"] == cname_ and r["leaf"] == "0"]
        random.Random(SEED).shuffle(te)  # 313 misses: a seeded sample of 30, not the first 30 in file order
        te = te[:30]
        write_tsv(OUT / "test_misses_list.tsv", list(te[0]), [list(r.values()) for r in te])
        dump(OUT / "test_misses_context.txt", te)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["dev", "test", "archive", "context"])
    a = ap.parse_args()
    {"dev": stage_dev, "test": stage_test, "archive": stage_archive, "context": stage_context}[a.stage]()
