"""Step 23d (phase 17): one-column VOC TOC detector (bar D). Stages, writes only into this directory:
  dev    96-cell grid + PH16 on the 22 GLOBALISE volumes (all dev), asserts the planning cell, select.json,
         dev_grid.tsv, front_runs.tsv (dev rows)
  test   ONCE, on the fresh layer + 23a TOC labels (needs step-23a/gold/agent_gold.done): scan_eval.tsv, test_done.json,
         front_runs.tsv (adds fresh rows)
  cases  text dumps for error analysis -> cases_dump.txt (test cases only after test_done.json exists)
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23d/eval_toc17.py STAGE
"""
import argparse, datetime, itertools, json, math, random, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.analysis.index_pages import (TOC_SIDES_DEFAULTS, side_features, side_rule, fill_runs,
                                                      toc_side_features, toc_side_rule, toc_sides)
from archival_structures.analysis.index_links import front_index_run
from archival_structures.text_layer import layout
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import SG_INDEX_SIDES_V5, _TYPE_MONTHS

HERE = Path("reports/data/phase-17-voc-rules-v1/step-23d")
DEV_ROOT = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
TEST_ROOT = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
DEV_GOLD = Path("reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv")
A23 = Path("reports/data/phase-17-voc-rules-v1/step-23a/gold")
DEV_INVS = "1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split()
# phase-16 cell machinery (PH16 = 22c's selected cell)
VOC_TOC_CUE = (r"\W{0,2}((den\s+)?\d{1,2}\s*[.,:'\"]*\s*" + _TYPE_MONTHS +
               r"|(fo\.?|fol\.?|f\.|pag\.?)?\s*\d{1,4}\s*(verso\s*)?(a|tot|en|—|–|-)\s*\d{1,4})")
SG16 = {"text_min_chars": 8, "shape_only_min": None, "date_pattern": None, "gap": 8}
PH16 = dict(text_min_chars=8, shape_only_min=0.25, date_pattern=VOC_TOC_CUE, gap=2)
GRID_AXES = dict(window=[45, 60], toc_min=[0.15, 0.2, 0.3, 0.4], amount_max=[None, 0.3], gap=[0, 2, 4], title=[False, True])
PLAN_CELL = dict(window=60, toc_min=0.2, amount_max=None, gap=4, title=True)


def read_plain(p):
    with open(p, encoding="utf-8") as f:
        h = f.readline().rstrip("\n").split("\t")
        for l in f:
            yield dict(zip(h, l.rstrip("\n").split("\t")))


class Vol:
    def __init__(self, root, inv):
        self.inv = inv
        self.scans = [r["scan_id"] for r in read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz")]
        self.scan_order = {s: i for i, s in enumerate(self.scans)}
        rows = [{k: r[k] for k in ("scan_id", "line_id", "x", "w", "text")}
                for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")]
        lm = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz")}
        sides = defaultdict(list)
        for l in rows:
            sides[(l["scan_id"], lm[(l["scan_id"], l["line_id"])])].append(l)
        self.keys = sorted(sides, key=lambda k: (self.scan_order[k[0]], layout.PAGE_ORDER[k[1]], k[1]))
        self.sides = [sides[k] for k in self.keys]
        self.texts = [[l["text"] for l in s] for s in self.sides]
        self.sidx = [self.scan_order[k[0]] for k in self.keys]
        self._f = {}

    def feats(self, cfg=None):
        key = json.dumps({k: cfg[k] for k in cfg if k.endswith("pattern") or k == "text_min_chars"} if cfg else {}, sort_keys=True)
        if key not in self._f:
            self._f[key] = [toc_side_features(t, cfg) for t in self.texts]
        return self._f[key]

    def flags(self, cfg):
        c = {**TOC_SIDES_DEFAULTS, **cfg}
        return fill_runs([toc_side_rule(f, i, c) for f, i in zip(self.feats(c), self.sidx)], c["gap"], 1)

    def ph16_flags(self):
        c = {**SG_INDEX_SIDES_V5, **SG16, **PH16}
        return fill_runs([side_rule(side_features(s, c), c) for s in self.sides], c["gap"], c["min_run"])

    def pred(self, flags):
        return {k[0] for k, f in zip(self.keys, flags) if f}


def prf(tp, fp, fn):
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n; d = 1 + z * z / n; c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(c - h, 4), round(c + h, 4))


def mcnemar(b, c):
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n)


def write_tsv(path, header, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(header) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")
    path.chmod(0o664)


def dump_json(path, obj):
    path.write_text(json.dumps(obj, indent=1)); path.chmod(0o664)


def load_dev_gold():
    g = defaultdict(dict)
    for r in read_plain(DEV_GOLD):
        g[r["inv"]][r["scan_id"]] = max(g[r["inv"]].get(r["scan_id"], 0), int(r["toc"]))
    return g


def score_pred(preds, gold):
    tp = fp = fn = 0
    for inv, gd in gold.items():
        for s, y in gd.items():
            q = s in preds[inv]
            tp += bool(y and q); fp += bool((not y) and q); fn += bool(y and not q)
    return tp, fp, fn


def cell_str(c):
    return ",".join(f"{k}={c[k]}" for k in GRID_AXES)


def n_diff(c):
    return sum(c[k] != TOC_SIDES_DEFAULTS[k] for k in GRID_AXES)


def voc_run(window):
    return dict(min_flagged=1, max_first=window, gap=20, min_after_frac=0.5)


def run_str(r):
    return f"{r[0]}-{r[1]}/{r[2]}" if r else "None"


def stage_dev():
    assert not (HERE / "test_done.json").exists(), "test already ran; dev must precede it"
    gold = load_dev_gold()
    vols = {i: Vol(DEV_ROOT, i) for i in DEV_INVS}
    gold = {i: gold[i] for i in DEV_INVS}

    def run_cell(c):
        return score_pred({i: vols[i].pred(vols[i].flags(c)) for i in DEV_INVS}, gold)
    got = run_cell(PLAN_CELL)
    assert got == (106, 48, 80), f"planning cell does not reproduce: {got}"
    print("planning cell reproduces (106, 48, 80)")
    grid = [dict(zip(GRID_AXES, v)) for v in itertools.product(*GRID_AXES.values())]
    rows = []
    for c in grid:
        tp, fp, fn = run_cell(c); p, r, f = prf(tp, fp, fn)
        rows.append([cell_str(c), *[c[k] for k in GRID_AXES], tp, fp, fn, round(p, 4), round(r, 4), round(f, 4)])
    tp, fp, fn = score_pred({i: vols[i].pred(vols[i].ph16_flags()) for i in DEV_INVS}, gold); p, r, f = prf(tp, fp, fn)
    rows.append(["PH16", "", "", "", "", "", tp, fp, fn, round(p, 4), round(r, 4), round(f, 4)])
    write_tsv(HERE / "dev_grid.tsv", ["cell", *GRID_AXES, "TP", "FP", "FN", "P", "R", "F1"], rows)
    best = max(r[-1] for r in rows[:-1])
    cand = [(r, c) for r, c in zip(rows, grid) if r[-1] >= best - 0.005]
    cand.sort(key=lambda rc: (n_diff(rc[1]), rc[1]["window"]))
    r, c = cand[0]
    # ledger guard: sides in window passing the rule without the guard but with amt >= 0.3 (dev, SEL)
    blocked = guard_blocked(vols.values(), c)
    sel = {"cell": c, "dev_TP": r[-6], "dev_FP": r[-5], "dev_FN": r[-4], "dev_P": r[-3], "dev_R": r[-2], "dev_F1": r[-1],
           "best_dev_F1": best, "n_candidates_within_0.005": len(cand), "n_diff_from_defaults": n_diff(c),
           "dev_guard_blocked_sides": blocked,
           "written_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
           "VOC_RUN": voc_run(c["window"])}
    dump_json(HERE / "select.json", sel)
    print(json.dumps(sel, indent=1))
    fr = []
    for i in DEV_INVS:
        v = vols[i]; fl = v.flags(c); n = len(fl)
        fr.append(["dev", i, n, sum(fl), run_str(front_index_run(fl)), run_str(front_index_run(fl, **voc_run(c["window"])))])
    write_tsv(HERE / "front_runs.tsv", ["split", "inv", "n_sides", "n_flagged", "sg_defaults", "voc_run"], fr)
    print("front runs dev: SG", sum(r[4] != "None" for r in fr), "VOC_RUN", sum(r[5] != "None" for r in fr), "of", len(fr))


def guard_blocked(vols, c):
    """Sides inside the window that the amount guard (amount_max=0.3) blocks although the rule fires without it."""
    c0 = {**c, "amount_max": None}; n = 0
    for v in vols:
        for f, i in zip(v.feats(c0), v.sidx):
            if toc_side_rule(f, i, c0) and f["amt"] >= 0.3:
                n += 1
    return n


def load_sel():
    s = json.loads((HERE / "select.json").read_text())
    return s, s["cell"]


def read_test_gold():
    g = {}  # (inv, scan_id) -> row
    for r in read_plain(A23 / "toc_labels.tsv"):
        g[(r["inv"], r["scan_id"])] = r
    return g


def stage_test():
    assert (HERE / "select.json").exists() and not (HERE / "test_done.json").exists(), "test runs once"
    assert (A23 / "agent_gold.done").exists(), "agent_gold.done missing"
    sel, c = load_sel()
    tg = read_test_gold()
    invs = sorted({k[0] for k in tg})
    vols = {i: Vol(TEST_ROOT, i) for i in invs}
    rows, per = [], {}
    cnt = {"SEL": [0, 0, 0], "PH16": [0, 0, 0]}
    pervol = {"SEL": defaultdict(lambda: [0, 0, 0]), "PH16": defaultdict(lambda: [0, 0, 0])}
    byloc = defaultdict(lambda: [0, 0, 0])
    bytype = defaultdict(lambda: [0, 0, 0])  # list_type -> n, SEL hit, PH16 hit
    b = cc = 0
    for i in invs:
        v = vols[i]
        ps, pp = v.pred(v.flags(c)), v.pred(v.ph16_flags())
        for (inv, sid), r in tg.items():
            if inv != i or r["label"] not in ("toc", "no"):
                continue
            y = r["label"] == "toc"; qs, qp = sid in ps, sid in pp
            rows.append([inv, sid, r["scan_num"], r["label"], r["list_type"], int(qs), int(qp)])
            for nm, q in (("SEL", qs), ("PH16", qp)):
                k = cnt[nm]; pv = pervol[nm][inv]
                for t in (k, pv):
                    t[0] += bool(y and q); t[1] += bool((not y) and q); t[2] += bool(y and not q)
            if y:
                bt = bytype[r["list_type"]]; bt[0] += 1; bt[1] += qs; bt[2] += qp
                bl = byloc[r["note"].split(";")[0]]; bl[0] += 1; bl[1] += qs; bl[2] += qp
            sc, pc = (qs == y), (qp == y)
            b += sc and not pc; cc += pc and not sc
    write_tsv(HERE / "scan_eval.tsv", ["inv", "scan_id", "scan_num", "label", "list_type", "pred_SEL", "pred_PH16"], rows)
    out = {"run_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"), "select_written_at_utc": sel["written_at_utc"],
           "sel_cell": c, "n_labelled_scans": len(rows), "n_volumes": len(invs)}
    for nm in ("SEL", "PH16"):
        tp, fp, fn = cnt[nm]; p, r, f = prf(tp, fp, fn)
        out[nm] = {"TP": tp, "FP": fp, "FN": fn, "P": round(p, 4), "R": round(r, 4), "F1": round(f, 4),
                   "P_wilson95": wilson(tp, tp + fp), "R_wilson95": wilson(tp, tp + fn),
                   "per_volume": {i: dict(zip(("TP", "FP", "FN"), v)) for i, v in sorted(pervol[nm].items())}}
    out["bar_D_pass_SEL"] = bool(out["SEL"]["P"] >= 0.95 and out["SEL"]["R"] >= 0.90)
    out["recall_by_list_type"] = {t: {"n": v[0], "SEL_hit": v[1], "PH16_hit": v[2]} for t, v in sorted(bytype.items())}
    out["recall_by_loc"] = {t: {"n": v[0], "SEL_hit": v[1], "PH16_hit": v[2]} for t, v in sorted(byloc.items())}
    out["mcnemar_SEL_vs_PH16"] = {"b_SEL_right_PH16_wrong": b, "c_PH16_right_SEL_wrong": cc, "p_exact_two_sided": round(mcnemar(b, cc), 6)}
    out["guard_blocked_sides_fresh"] = guard_blocked(vols.values(), c)
    fr = []
    for i in invs:
        v = vols[i]; fl = v.flags(c); n = len(fl)
        fr.append(["fresh", i, n, sum(fl), run_str(front_index_run(fl)), run_str(front_index_run(fl, **voc_run(c["window"])))])
    old = [l.rstrip("\n").split("\t") for l in open(HERE / "front_runs.tsv")]
    write_tsv(HERE / "front_runs.tsv", old[0], old[1:] + fr)
    out["front_runs_fresh"] = {"n": len(fr), "sg_defaults": sum(r[4] != "None" for r in fr), "voc_run": sum(r[5] != "None" for r in fr)}
    dump_json(HERE / "test_done.json", out)
    print(json.dumps({k: v for k, v in out.items() if k not in ("SEL", "PH16")}, indent=1))
    for nm in ("SEL", "PH16"):
        print(nm, {k: v for k, v in out[nm].items() if k != "per_volume"})


def stage_cases():
    sel, c = load_sel()
    rng = random.Random(20261101)
    out = []

    def dump(label, vols, preds, golds, kind, limit):
        items = [(i, s) for i in sorted(vols) for s, y in golds[i].items() if (kind == "FN" and y and s not in preds[i]) or (kind == "FP" and not y and s in preds[i])]
        tot = len(items)
        if len(items) > limit:
            items = sorted(rng.sample(items, limit))
        out.append(f"##### {label} {kind} total={tot} shown={len(items)}")
        for i, s in items:
            v = vols[i]; fe = v.feats(c); idx = [k for k, key in enumerate(v.keys) if key[0] == s]
            out.append(f"=== {i} {s} scan_index={v.scan_order[s]} {kind} " + " | ".join(
                f"{v.keys[k][1]} n={fe[k]['n']} fol={fe[k]['fol']:.2f} date={fe[k]['date']:.2f} amt={fe[k]['amt']:.2f} title={int(fe[k]['title'])}" for k in idx))
            for k in idx[:2]:
                for l in v.texts[k][:5]:
                    out.append("    " + l[:110])
            if not idx:
                out.append("    (no lines)")
    gold = load_dev_gold()
    vols = {i: Vol(DEV_ROOT, i) for i in DEV_INVS}
    preds = {i: vols[i].pred(vols[i].flags(c)) for i in DEV_INVS}
    for kind in ("FN", "FP"):
        dump("dev", vols, preds, {i: gold[i] for i in DEV_INVS}, kind, 15)
    if (HERE / "test_done.json").exists():
        tg = read_test_gold()
        invs = sorted({k[0] for k in tg})
        tv = {i: Vol(TEST_ROOT, i) for i in invs}
        tp = {i: tv[i].pred(tv[i].flags(c)) for i in invs}
        tgd = {i: {s: int(r["label"] == "toc") for (j, s), r in tg.items() if j == i and r["label"] in ("toc", "no")} for i in invs}
        for kind in ("FN", "FP"):
            dump("test", tv, tp, tgd, kind, 30)
    (HERE / "cases_dump.txt").write_text("\n".join(out)); (HERE / "cases_dump.txt").chmod(0o664)
    print("wrote", HERE / "cases_dump.txt", len(out), "lines")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("stage", choices=["dev", "test", "cases"])
    {"dev": stage_dev, "test": stage_test, "cases": stage_cases}[ap.parse_args().stage]()
