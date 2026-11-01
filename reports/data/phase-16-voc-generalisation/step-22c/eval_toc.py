"""Step 22c: index-side detector on VOC tables of contents (bar D on GLOBALISE gold).

Stages (run in order; the order protects the dev/test protocol):
  replica   _write_pages_table replica vs layout/pages of the layer     -> replica_check.tsv
  dev       32-cell grid on the 11 dev volumes, selection              -> dev_grid.tsv, select.json
  test      selected + unchanged cell on test volumes (once), 2775 apart, front runs
                                                                         -> test_done.json, scan_eval.tsv, front_runs.tsv
  cases     dump cases for error analysis to --out (text)
Run from the AS root: .venv/bin/python reports/data/phase-16-voc-generalisation/step-22c/eval_toc.py STAGE
"""
import argparse, itertools, json, math, random, sys, tempfile, datetime
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from archival_structures.analysis.index_pages import side_features, side_rule, fill_runs
from archival_structures.analysis.index_links import front_index_run
from archival_structures.text_layer import layout
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import SG_INDEX_SIDES_V5, SG_INDEX_DATE_START, _TYPE_MONTHS

VOC_TOC_CUE = (r"\W{0,2}((den\s+)?\d{1,2}\s*[.,:'\"]*\s*" + _TYPE_MONTHS +
               r"|(fo\.?|fol\.?|f\.|pag\.?)?\s*\d{1,4}\s*(verso\s*)?(a|tot|en|—|–|-)\s*\d{1,4})")
ROOT = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
HERE = Path(__file__).resolve().parent
GOLD = Path("reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv")
DEV = "1267 1274 1388 1547 1557 2548 2555 3142 7923 8121 8697".split()
TEST = "1120 1529 1539 2448 3891 8023 8237 8276 8284 8834".split()
SPECIAL = "2775"
ALL = DEV + TEST + [SPECIAL]
SG = {"text_min_chars": 8, "shape_only_min": None, "date_pattern": SG_INDEX_DATE_START, "gap": 8}
GRID = [dict(text_min_chars=a, shape_only_min=b, date_pattern=c, gap=d) for a, b, c, d in
        itertools.product([8, 3], [None, 0.15, 0.25, 0.35], [SG_INDEX_DATE_START, VOC_TOC_CUE], [8, 2])]


def cell_name(c):
    return f"tmc{c['text_min_chars']}_som{c['shape_only_min']}_{'cue' if c['date_pattern'] == VOC_TOC_CUE else 'sg'}_gap{c['gap']}"


def n_changed(c):
    return sum(c[k] != SG[k] for k in SG)


def load_gold():
    g = defaultdict(list)
    for r in read_tsv_gz_plain(GOLD):
        g[r["inv"]].append((r["scan_id"], int(r["toc"]), r["split"]))
    return g


def read_tsv_gz_plain(p):
    with open(p, encoding="utf-8") as f:
        h = f.readline().rstrip("\n").split("\t")
        for l in f:
            yield dict(zip(h, l.rstrip("\n").split("\t")))


class Vol:
    def __init__(self, inv):
        self.inv = inv
        self.scans = [r["scan_id"] for r in read_tsv_gz(ROOT / "scans" / f"{inv}.scans.tsv.gz")]
        self.scan_order = {s: i for i, s in enumerate(self.scans)}
        self.line_rows = [{k: r[k] for k in ("scan_id", "line_id", "x", "w", "text")}
                          for r in read_tsv_gz(ROOT / "lines" / f"{inv}.lines.tsv.gz")]
        self.line_map = {(r["scan_id"], r["line_id"]): {"page": r["page"]}
                         for r in read_tsv_gz(ROOT / "layout" / "lines" / f"{inv}.tsv.gz")}
        self.has_pages = (ROOT / "layout" / "pages" / f"{inv}.tsv.gz").exists()
        sides = defaultdict(list)
        for l in self.line_rows:
            sides[(l["scan_id"], self.line_map[(l["scan_id"], l["line_id"])]["page"])].append(l)
        self.keys = sorted(sides, key=lambda k: (self.scan_order[k[0]], layout.PAGE_ORDER[k[1]], k[1]))
        self.sides = [sides[k] for k in self.keys]
        self._feat = {}

    def feats(self, cfg):
        k = (cfg["text_min_chars"], cfg["date_pattern"])
        if k not in self._feat:
            self._feat[k] = [side_features(s, cfg) for s in self.sides]
        return self._feat[k]

    def flags(self, cfg):
        c = {**SG_INDEX_SIDES_V5, **SG, **cfg}
        if not self.has_pages:
            return [False] * len(self.keys)
        rules = [side_rule(f, c) for f in self.feats(c)]
        return fill_runs(rules, c["gap"], c["min_run"])

    def scan_pred(self, cfg):
        fl = self.flags(cfg)
        p = set(k[0] for k, f in zip(self.keys, fl) if f)
        return p


def prf(tp, fp, fn):
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    f = 2 * p * r / (p + r) if p + r else 0.0
    return p, r, f


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(c - h, 4), round(c + h, 4))


def score(vols, gold, invs, cfg):
    tot = [0, 0, 0]
    per = {}
    for inv in invs:
        pred = vols[inv].scan_pred(cfg)
        tp = fp = fn = 0
        for sid, toc, _ in gold[inv]:
            q = sid in pred
            tp += toc and q
            fp += (not toc) and q
            fn += toc and not q
        per[inv] = (tp, fp, fn)
        tot = [a + b for a, b in zip(tot, (tp, fp, fn))]
    return tot, per


def write_tsv(path, header, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(header) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")
    path.chmod(0o664)


def stage_replica():
    rows = []
    for inv in ALL:
        v = Vol(inv)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "p.tsv.gz"
            layout._write_pages_table(out, v.line_rows, v.line_map, SG_INDEX_SIDES_V5, v.scan_order)
            mine = [(r["scan_id"], r["page"], r["rule"], r["index_side"]) for r in read_tsv_gz(out)]
        ref = [(r["scan_id"], r["page"], r["rule"], r["index_side"]) for r in read_tsv_gz(ROOT / "layout" / "pages" / f"{inv}.tsv.gz")]
        # also check the in-script flag path
        mine2 = [(k[0], k[1], f) for k, f in zip(v.keys, v.flags({}))]
        ok2 = all(a[3] == str(int(b[2])) for a, b in zip(ref, mine2)) and len(ref) == len(mine2)
        rows.append([inv, len(ref), len(mine), sum(a != b for a, b in zip(ref, mine)) + abs(len(ref) - len(mine)),
                     int(mine == ref), int(ok2), sum(r[3] == "1" for r in ref)])
    write_tsv(HERE / "replica_check.tsv", ["inv", "n_sides_layer", "n_sides_replica", "n_diff", "equal", "inscript_flags_equal", "n_flagged"], rows)
    bad = [r[0] for r in rows if not (r[4] and r[5])]
    print("replica: all equal" if not bad else f"MISMATCH {bad}")
    return not bad


def stage_dev():
    gold = load_gold()
    vols = {i: Vol(i) for i in DEV}
    rows = []
    for c in GRID:
        (tp, fp, fn), _ = score(vols, gold, DEV, c)
        p, r, f = prf(tp, fp, fn)
        rows.append([cell_name(c), c["text_min_chars"], c["shape_only_min"], "VOC_TOC_CUE" if c["date_pattern"] == VOC_TOC_CUE else "SG",
                     c["gap"], tp, fp, fn, round(p, 4), round(r, 4), round(f, 4)])
    write_tsv(HERE / "dev_grid.tsv", ["cell", "text_min_chars", "shape_only_min", "date_pattern", "gap", "TP", "FP", "FN", "P", "R", "F1"], rows)
    best = max(r[10] for r in rows)
    cand = [(r, c) for r, c in zip(rows, GRID) if r[10] >= best - 0.005]
    key = lambda rc: (n_changed(rc[1]), -1 if rc[1]["shape_only_min"] is None else rc[1]["shape_only_min"], 0 if rc[1]["gap"] == 8 else 1)
    cand.sort(key=key)
    r, c = cand[0]
    sel = {"selected": cell_name(c), "cell": {**c, "date_pattern": "VOC_TOC_CUE" if c["date_pattern"] == VOC_TOC_CUE else "SG_INDEX_DATE_START"},
           "dev_F1": r[10], "dev_P": r[8], "dev_R": r[9], "best_dev_F1": best,
           "candidates_within_0.005": [cell_name(cc) for _, cc in cand], "n_changed_from_SG": n_changed(c),
           "written_at": datetime.datetime.now().isoformat(timespec="seconds"), "test_numbers_seen": False}
    (HERE / "select.json").write_text(json.dumps(sel, indent=1))
    (HERE / "select.json").chmod(0o664)
    print(json.dumps(sel, indent=1))


def load_sel():
    s = json.loads((HERE / "select.json").read_text())
    c = s["cell"]
    c["date_pattern"] = VOC_TOC_CUE if c["date_pattern"] == "VOC_TOC_CUE" else SG_INDEX_DATE_START
    return c


def fmt(tp, fp, fn):
    p, r, f = prf(tp, fp, fn)
    return {"TP": tp, "FP": fp, "FN": fn, "P": round(p, 4), "R": round(r, 4), "F1": round(f, 4),
            "P_num_den": f"{tp}/{tp + fp}", "R_num_den": f"{tp}/{tp + fn}",
            "P_wilson95": wilson(tp, tp + fp), "R_wilson95": wilson(tp, tp + fn)}


def stage_test():
    assert (HERE / "select.json").exists() and not (HERE / "test_done.json").exists(), "test runs once"
    gold = load_gold()
    sel = load_sel()
    vols = {i: Vol(i) for i in ALL}
    out = {"run_at": datetime.datetime.now().isoformat(timespec="seconds"), "selected": cell_name(sel), "unchanged": cell_name(SG)}
    scan_rows = []
    for inv in TEST + [SPECIAL]:
        pu, ps = vols[inv].scan_pred(SG), vols[inv].scan_pred(sel)
        for sid, toc, split in gold[inv]:
            scan_rows.append([inv, sid, split, toc, int(sid in pu), int(sid in ps)])
    write_tsv(HERE / "scan_eval.tsv", ["inv", "scan_id", "split", "toc", "pred_unchanged", "pred_selected"], scan_rows)
    for name, cfg in (("unchanged", SG), ("selected", sel)):
        tot, per = score(vols, gold, TEST, cfg)
        out[name] = {"pooled_test10": fmt(*tot), "per_volume": {i: dict(zip(("TP", "FP", "FN"), v)) for i, v in per.items()}}
        t2, _ = score(vols, gold, [SPECIAL], cfg)
        out[name]["inv_2775"] = fmt(*t2)
        out[name]["bar_D_pass"] = bool(out[name]["pooled_test10"]["P"] >= 0.95 and out[name]["pooled_test10"]["R"] >= 0.90)
    out["verdict"] = ("unchanged" if out["unchanged"]["bar_D_pass"] else
                      "reparameterised" if out["selected"]["bar_D_pass"] else "fails on VOC")
    # front runs
    fr = []
    reasons = {}
    for inv in ALL:
        v = vols[inv]
        gset = {s for s, t, _ in gold[inv] if t}
        variants = {"unchanged": v.flags(SG), "selected": v.flags(sel), "oracle": [k[0] in gset for k in v.keys]}
        for name, fl in variants.items():
            r = front_index_run(fl)
            fr.append([inv, name, r[0] if r else "None", r[1] if r else "None", r[2] if r else "None"])
            if name == "oracle" and r is None:
                reasons[inv] = oracle_reason(fl)
    write_tsv(HERE / "front_runs.tsv", ["inv", "variant", "first", "last", "end"], fr)
    out["oracle_none_reasons"] = reasons
    (HERE / "test_done.json").write_text(json.dumps(out, indent=1))
    (HERE / "test_done.json").chmod(0o664)
    print(json.dumps({k: v for k, v in out.items() if k != "oracle_none_reasons"}, indent=1)[:6000])
    print(reasons)


def oracle_reason(flags, min_flagged=10, max_first=10, gap=20, min_after=300, max_share=0.6):
    n = len(flags)
    if n == 0:
        return "empty"
    if sum(flags) / n > max_share:
        return "max_share"
    end, run = None, 0
    for i, f in enumerate(flags):
        run = 0 if f else run + 1
        if run >= gap:
            end = i - gap + 1
            break
    if end is None:
        return "no_20_gap"
    idx = [i for i in range(end) if flags[i]]
    why = []
    if len(idx) < min_flagged:
        why.append(f"min_flagged({len(idx)})")
    if idx and idx[0] >= max_first:
        why.append(f"max_first(first={idx[0]})")
    if n - end < min_after:
        why.append(f"min_after({n - end})")
    return "+".join(why) or "?"


def stage_cases(outp):
    gold = load_gold()
    sel = load_sel()
    vols = {i: Vol(i) for i in ALL if i != SPECIAL}
    rng = random.Random(20261031)
    out = []

    def collect(invs, cfg_name, cfg, kind, limit):
        items = []
        for inv in invs:
            v = vols[inv]
            pred = v.scan_pred(cfg)
            for sid, toc, _ in gold[inv]:
                q = sid in pred
                if kind == "FN" and toc and not q or kind == "FP" and q and not toc:
                    items.append((inv, sid))
        if len(items) > limit:
            items = sorted(rng.sample(items, limit))
        return items, 

    def dump(split, cname, cfg, kind, limit, invs):
        (items,) = collect(invs, cname, cfg, kind, limit)
        out.append(f"##### {split} {cname} {kind} n_shown={len(items)}")
        for inv, sid in items:
            v = vols[inv]
            fe = v.feats(cfg)
            idx = [i for i, k in enumerate(v.keys) if k[0] == sid]
            fl = v.flags(cfg)
            out.append(f"=== {inv} {sid} {kind} " + " | ".join(
                f"{v.keys[i][1]} nl={fe[i]['n_lines']} t={fe[i]['n_text8']} cs={fe[i]['col_share']} en={fe[i]['end_num']:.2f} ds={fe[i]['date_start']:.2f} fl={int(fl[i])}"
                for i in idx))
            for i in idx[:1]:
                for l in v.sides[i][:5]:
                    out.append("    " + l["text"][:110])
            if not idx:
                out.append("    (no lines/side)")
    dump("dev", cell_name(sel), sel, "FN", 30, DEV)
    dump("dev", cell_name(sel), sel, "FP", 30, DEV)
    dump("dev", cell_name(SG), SG, "FN", 15, DEV)
    if (HERE / "test_done.json").exists():
        dump("test", cell_name(sel), sel, "FN", 30, TEST)
        dump("test", cell_name(sel), sel, "FP", 30, TEST)
    Path(outp).write_text("\n".join(out))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["replica", "dev", "test", "cases"])
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    if a.stage == "replica":
        sys.exit(0 if stage_replica() else 1)
    if a.stage == "dev":
        stage_dev()
    if a.stage == "test":
        stage_test()
    if a.stage == "cases":
        stage_cases(a.out)
