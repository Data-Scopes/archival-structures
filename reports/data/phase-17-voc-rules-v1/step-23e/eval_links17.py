"""Step 23e (phase 17): locator L9 / L9g and the monotone filter on VOC tables of contents; links with the 23c map.
Single process. Run from the AS root (reads the layers; writes only into this directory):

    PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23e/eval_links17.py l8check
    ... dev      # asserts the six SEL22B prototype rows, writes dev_grid.tsv, select.json, dev_links.tsv
    ... test     # ONCE: test_done.json, test_links.tsv, test_lines.tsv, end_to_end.tsv, silver.tsv
    ... errors   # after test: errors_dump.txt (reads test_links.tsv / test_lines.tsv only)

Reads: data/text_layer_voc_22a (dev, a link to T7_Data), data/text_layer_voc_23a (test), planning/toc_gold.tsv (dev gold),
step-23c/select.json, step-23d/select.json, step-23a/gold/*.tsv (test only), the OBP silver csv under ground_truth/.
Starts and targets are scoring only; no rule reads them.
"""
import argparse
import csv
import datetime
import importlib.util
import json
import math
import random
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.analysis.folio_map import FolioMapParams, build_folio_map  # noqa: E402
from archival_structures.analysis import index_links as IL  # noqa: E402
from archival_structures.analysis.index_links import (  # noqa: E402
    VOC_LOCATOR_PARAMS, extract_locators, front_index_run, monotone_mask, read_volume_inputs)
from archival_structures.datasets.globalise_index import load_globalise_index  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

DEV_ROOT = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
TEST_ROOT = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
DEV_GOLD = Path("reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv")
A23 = Path("reports/data/phase-17-voc-rules-v1/step-23a/gold")
P17 = Path("reports/data/phase-17-voc-rules-v1")
SILVER = Path("ground_truth/NL-HaNA_1.04.02/globalise_digitized_indexes/source/"
              "GLOBALISE - Digitized Indexes of the Dutch East India Company OBP (1602-1799).csv")
HERE = Path(__file__).resolve().parent
DEV = ("1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 "
       "8834").split()
TAGS = ("page-number",)
GAP = 20
SEED = 20261101
SEL22B = FolioMapParams()
PROTO = {("L8", False): (1009, 1851, 790, 1526), ("L9", False): (1239, 2045, 971, 1526),
         ("L9", True): (898, 1112, 866, 1526), ("L9g", False): (1199, 1870, 969, 1526),
         ("L9g", True): (901, 1104, 867, 1526)}


def sel23c_params():
    d = json.loads((P17 / "step-23c" / "select.json").read_text())["cell"]
    return FolioMapParams(**d)


def wilson(k, n, z=1.96):
    if n == 0:
        return [None, None]
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return [round(c - h, 4), round(c + h, 4)]


def write_tsv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    path.chmod(0o664)


def dump_json(path, obj):
    path.write_text(json.dumps(obj, indent=1, default=str))
    path.chmod(0o664)


def read_plain(p):
    with open(p, encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)


def load_dev_gold():
    g = defaultdict(dict)
    for r in read_plain(DEV_GOLD):
        old = g[r["inv"]].get(r["scan_id"], (0, 0))
        g[r["inv"]][r["scan_id"]] = (max(old[0], int(r["toc"])), max(old[1], int(r["start"])))
    return g


class Vol:
    """One volume: TOC sides (given), the front/later split at a gap > GAP sides, exclude = front sides + all TOC sides."""

    def __init__(self, root, inv, toc_scans=None, gold_starts=frozenset(), toc_keys=None, excl=None):
        self.root, self.inv = root, inv
        self.gold_starts = set(gold_starts)
        self.base = read_volume_inputs(root, inv)
        self.scan_ids = self.base.scan_ids
        self.pos = {s: i for i, s in enumerate(self.scan_ids)}
        sides = self.base.sides
        if toc_keys is not None:
            self.toc_idx = [i for i, sd in enumerate(sides) if sd in toc_keys]
        else:
            self.toc_idx = [i for i, sd in enumerate(sides) if sd[0] in toc_scans]
        front_last, prev = None, None
        for i in self.toc_idx:
            if prev is not None and i - prev > GAP:
                break
            front_last = prev = i
        self.part = {sides[i]: ("front" if front_last is not None and i <= front_last else "later") for i in self.toc_idx}
        self.front_last_idx = front_last
        self.front_last_scan = sides[front_last][0] if front_last is not None else None
        self.excl = excl if excl is not None else (
            {s for i, s in enumerate(sides) if front_last is not None and i <= front_last} | set(self.part))
        page_of = {(r["scan_id"], r["line_id"]): r["page"]
                   for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz")}
        self.lines = []   # (scan_id, page, line_id, text) in file order, TOC sides only
        for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
            pg = page_of.get((r["scan_id"], r["line_id"]))
            if pg is not None and (r["scan_id"], pg) in self.part and r["text"].strip():
                self.lines.append((r["scan_id"], pg, r["line_id"], r["text"]))
        self._maps, self._locs = {}, {}

    def fmap(self, mparams):
        if mparams not in self._maps:
            v = read_volume_inputs(self.root, self.inv, number_tags=TAGS)
            self._maps[mparams] = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=self.excl, params=mparams)
        return self._maps[mparams]

    def rng(self, mparams):
        fm = self.fmap(mparams)
        return (int(fm.chain[0, 1]), int(fm.chain[-1, 1])) if len(fm.chain) else None

    def locs(self, variant, mono, mparams):
        """-> (kept, dropped): lists of dicts (scan_id, page, line_id, text, number) in reading order."""
        rng = self.rng(mparams)
        key = (variant, rng)
        if key not in self._locs:
            p = VOC_LOCATOR_PARAMS[variant]
            allr = []
            for sid, pg, lid, t in self.lines:
                for loc in extract_locators(t, p, folio_range=rng):
                    allr.append(dict(scan_id=sid, page=pg, line_id=lid, text=t, number=loc.number))
            self._locs[key] = allr
        allr = self._locs[key]
        if not mono:
            return allr, []
        mask = monotone_mask([r["number"] for r in allr])
        return [r for r, k in zip(allr, mask) if k], [r for r, k in zip(allr, mask) if not k]

    def leaf(self, mparams, number):
        t = self.fmap(mparams).locate(number)
        if t is None:
            return None, ()
        p = self.pos[t.front[0]]
        return t, tuple(self.scan_ids[p:p + 2])

    def links(self, cell):
        variant, mono, mp = cell
        rows = []
        for r in self.locs(variant, mono, mp)[0]:
            t, leaf = self.leaf(mp, r["number"])
            row = dict(r, inv=self.inv, front_scan_id=t.front[0] if t else "", method=t.method if t else "none",
                       leaf_hit=int(any(s in self.gold_starts for s in leaf)), toc_part=self.part[(r["scan_id"], r["page"])],
                       _leaf=leaf)
            rows.append(row)
        return rows

    def coverage(self, rows):
        after = {s for s in self.gold_starts
                 if self.front_last_idx is not None and self.pos[s] > self.pos[self.front_last_scan]}
        cov = {s for r in rows for s in r.get("_leaf", ()) if s in after}
        return len(cov), len(after)


def dev_vols():
    gold = load_dev_gold()
    return {i: Vol(DEV_ROOT, i, toc_scans={s for s, (t, st) in gold[i].items() if t},
                   gold_starts={s for s, (t, st) in gold[i].items() if st}) for i in DEV}


def score(vols, cell):
    n = hit = cov = tot = 0
    for v in vols.values():
        rows = v.links(cell)
        c, t = v.coverage(rows)
        n += len(rows)
        hit += sum(r["leaf_hit"] for r in rows)
        cov += c
        tot += t
    E = hit / n if n else 0.0
    C = cov / tot if tot else 0.0
    return dict(n=n, hit=hit, E=E, cov=cov, tot=tot, C=C, F1=(2 * E * C / (E + C)) if E + C else 0.0)


# ---------------------------------------------------------------------------------------------------- l8check
def stage_l8check():
    src = subprocess.run(["git", "show", "HEAD:archival_structures/analysis/index_links.py"], capture_output=True,
                         text=True, check=True).stdout
    scratch = Path("/private/tmp/claude-504/-Users-Shared-claude-projects-data-scopes-archival-structures/"
                   "bed0a4c2-b97f-4c21-9e13-b6d98d1d1cd8/scratchpad")
    scratch.mkdir(parents=True, exist_ok=True)
    pth = scratch / "index_links_head.py"
    pth.write_text(src)
    spec = importlib.util.spec_from_file_location("archival_structures.analysis.index_links_head", pth)
    head = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = head
    spec.loader.exec_module(head)
    gold = load_dev_gold()
    n = diff = 0
    ex = []
    for inv in DEV:
        v = Vol(DEV_ROOT, inv, toc_scans={s for s, (t, st) in gold[inv].items() if t})
        for sid, pg, lid, t in v.lines:
            a = [(l.number, l.char_start, l.char_end) for l in extract_locators(t, VOC_LOCATOR_PARAMS["L8"])]
            b = [(l.number, l.char_start, l.char_end) for l in head.extract_locators(t, head.VOC_LOCATOR_PARAMS["L8"])]
            # also with a folio range
            a2 = [(l.number, l.char_start, l.char_end) for l in
                  extract_locators(t, VOC_LOCATOR_PARAMS["L8"], folio_range=(1, 900))]
            b2 = [(l.number, l.char_start, l.char_end) for l in
                  head.extract_locators(t, head.VOC_LOCATOR_PARAMS["L8"], folio_range=(1, 900))]
            n += 1
            if a != b or a2 != b2:
                diff += 1
                ex.append((inv, sid, t[:80], a, b))
    out = {"volumes": len(DEV), "lines": n, "differences": diff, "examples": ex[:10],
           "head_module": "git show HEAD:archival_structures/analysis/index_links.py"}
    dump_json(HERE / "l8check.json", out)
    print(json.dumps(out, indent=1))
    if diff:
        sys.exit("l8check: differences, the step stops")


# ---------------------------------------------------------------------------------------------------- dev
def grid_cells(m23):
    """C3 first (tie order after the best), then the rest of the grid."""
    maps = [("SEL22B", SEL22B), ("SEL23C", m23)] if m23 != SEL22B else [("SEL22B", SEL22B)]
    cells = []
    for mname, mp in maps:
        for variant in ("L8", "L9", "L9g"):
            for mono in (False, True):
                cells.append((variant, mono, mname, mp))
    return cells


def diffs(c):
    return (c[0] != "L8") + (c[1] is not False) + (c[2] != "SEL22B")


def stage_dev():
    m23 = sel23c_params()
    cells = grid_cells(m23)
    vols = dev_vols()
    res = {}
    for variant, mono, mname, mp in cells:
        res[(variant, mono, mname)] = score(vols, (variant, mono, mp))
    # prototype assertion: the six SEL22B rows
    chk = []
    bad = []
    for (variant, mono), (h, n, cv, tot) in PROTO.items():
        r = res[(variant, mono, "SEL22B")]
        ok = (r["hit"], r["n"], r["cov"], r["tot"]) == (h, n, cv, tot)
        chk.append([variant, mono, f"{r['hit']}/{r['n']}", f"{r['cov']}/{r['tot']}", f"{h}/{n}", f"{cv}/{tot}", ok])
        if not ok:
            bad.append(chk[-1])
    write_tsv(HERE / "c3_reproduction.tsv", ["variant", "mono", "got_E", "got_cov", "proto_E", "proto_cov", "match"], chk)
    print("C3 reproduction")
    for r in chk:
        print(r)
    if bad:
        sys.exit("prototype rows do not reproduce; the step stops: " + str(bad))
    header = ["variant", "mono", "map", "n_links", "hits", "E", "cov", "cov_total", "coverage", "F1", "diff_from_C3"]
    rows = [[v, "on" if m else "off", mn, r["n"], r["hit"], round(r["E"], 4), r["cov"], r["tot"], round(r["C"], 4),
             round(r["F1"], 4), diffs((v, m, mn))] for (v, m, mn), r in res.items()]
    write_tsv(HERE / "dev_grid.tsv", header, rows)
    best = max(r["F1"] for r in res.values())
    cand = [k for k in res if res[k]["F1"] >= best - 0.005]
    sel = min(cand, key=lambda k: (diffs(k), list(res).index(k)))
    out = {"variant": sel[0], "mono": sel[1], "map": sel[2],
           "map_params": dict(m23.__dict__) if sel[2] == "SEL23C" else dict(SEL22B.__dict__),
           "sel23c_equals_sel22b": m23 == SEL22B, "dev_F1": round(res[sel]["F1"], 4), "best_dev_F1": round(best, 4),
           "dev_E": round(res[sel]["E"], 4), "dev_hits": res[sel]["hit"], "dev_n": res[sel]["n"],
           "dev_coverage": round(res[sel]["C"], 4), "candidates_within_0.005": ["|".join(map(str, k)) for k in cand],
           "n_cells": len(res), "time_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
    dump_json(HERE / "select.json", out)
    print(json.dumps(out, indent=1))
    for r in rows:
        print(r)


# ---------------------------------------------------------------------------------------------------- test
def sel_cell():
    s = json.loads((HERE / "select.json").read_text())
    m23 = sel23c_params()
    mp = m23 if s["map"] == "SEL23C" else SEL22B
    return (s["variant"], s["mono"], mp), s


def c3_cell():
    return ("L8", False, SEL22B)


def cname(c):
    return f"{c[0]}|{'mono' if c[1] else 'nomono'}|{'SEL23C' if c[2] != SEL22B else 'SEL22B'}"


def in_leaf(v, mp, number):
    return v.leaf(mp, number)[1]


def stage_test():
    assert (HERE / "select.json").exists() and not (HERE / "test_done.json").exists(), "test runs once"
    assert (A23 / "agent_gold.done").exists(), "agent_gold.done missing"
    sel_c, sel_json = sel_cell()
    cells = [("SEL", sel_c), ("C3", c3_cell())]
    labels = {(r["inv"], r["scan_id"]): r for r in read_plain(A23 / "toc_labels.tsv")}
    lines = [r for r in read_plain(A23 / "link_lines.tsv")]
    targets = {}
    for r in read_plain(A23 / "link_targets.tsv"):
        targets[(r["inv"], r["line_id"], int(r["folio"]))] = r
    invs = sorted({r["inv"] for r in lines})
    vols = {}
    for i in invs:
        toc = {s for (inv, s), r in labels.items() if inv == i and r["label"] == "toc"}
        vols[i] = Vol(TEST_ROOT, i, toc_scans=toc)
    sampled = {}
    for r in lines:
        gf = [int(x) for x in r["gold_folios"].split(";") if x.strip()]
        htr = re.search(r"htr=(\d+)", r["note"])
        loc = labels.get((r["inv"], r["scan_id"]), {}).get("note", "").split(";")[0]
        sampled[(r["inv"], r["line_id"])] = dict(r, gold=gf, htr=int(htr.group(1)) if htr else None,
                                                  loc=loc if loc.startswith("loc=") else "loc=?")
    out = {"run_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
           "select_time_utc": sel_json["time_utc"], "select": {k: sel_json[k] for k in ("variant", "mono", "map")},
           "n_volumes": len(invs), "n_lines": len(lines), "n_lines_ok": sum(r["status"] == "ok" for r in lines),
           "n_lines_unreadable": sum(r["status"] != "ok" for r in lines), "cells": {}}
    link_rows, line_rows = [], []
    for nm, cell in cells:
        variant, mono, mp = cell
        tp = np_ = ng = 0
        tp_h = ng_h = 0
        strat = defaultdict(lambda: [0, 0, 0])   # stratum -> tp, pred, gold
        hit = pred_n = excl = excl_likely = hit_sure = pred_sure = 0
        miss_cat = Counter()
        emap_k = emap_n = emap_sure_k = emap_sure_n = 0
        off_toc = 0
        n_line_pred = 0
        for inv in invs:
            v = vols[inv]
            kept, dropped = v.locs(variant, mono, mp)
            pbl, dbl = defaultdict(list), defaultdict(list)
            for r in kept:
                pbl[r["line_id"]].append(r["number"])
            for r in dropped:
                dbl[r["line_id"]].append(r["number"])
            tocs = {(s, p) for s, p, _l, _t in v.lines}
            for (i2, lid), s in sampled.items():
                if i2 != inv or s["status"] != "ok":
                    continue
                if (s["scan_id"], s["page"]) not in v.part:
                    off_toc += 1
                P, G = Counter(pbl.get(lid, [])), Counter(s["gold"])
                tpl = sum((P & G).values())
                Gh = Counter([s["htr"]] if s["htr"] is not None else s["gold"])
                tp_h += sum((P & Gh).values())
                ng_h += sum(Gh.values())
                tp += tpl
                np_ += sum(P.values())
                ng += sum(G.values())
                for key in ("all", s["loc"], "gold" if G else "nogold"):
                    st = strat[key]
                    st[0] += tpl
                    st[1] += sum(P.values())
                    st[2] += sum(G.values())
                line_rows.append([nm, inv, s["scan_id"], lid, s["loc"], s["text"], ";".join(map(str, s["gold"])),
                                  ";".join(map(str, pbl.get(lid, []))), tpl, ";".join(map(str, dbl.get(lid, []))),
                                  s["htr"] or ""])
                # E over predicted (line, N)
                for n_ in pbl.get(lid, []):
                    t, leaf = v.leaf(mp, n_)
                    tg = targets.get((inv, lid, n_)) if n_ in s["gold"] else None
                    if tg is None:
                        outcome = "miss"
                        cat = "not_in_gold"
                    elif tg["status"] == "not_found":
                        outcome = "excluded"
                        cat = ""
                    else:
                        ok_ = tg["target_scan"] in leaf
                        outcome = "hit" if ok_ else "miss"
                        cat = "" if ok_ else ("unlocated" if t is None else "wrong_leaf")
                    conf = tg["confidence"] if tg else ""
                    if outcome == "excluded":
                        excl += 1
                    else:
                        pred_n += 1
                        hit += outcome == "hit"
                        if conf == "likely":
                            excl_likely += 1
                        else:
                            pred_sure += 1
                            hit_sure += outcome == "hit"
                    link_rows.append([nm, inv, s["scan_id"], lid, n_, t.front[0] if t else "", ",".join(leaf),
                                      t.method if t else "none", int(n_ in s["gold"]),
                                      tg["status"] if tg else "", conf, tg["target_scan"] if tg else "", outcome, cat,
                                      s["text"]])
                # E_map over gold (line, N) with found target
                for n_ in sorted(set(s["gold"])):
                    tg = targets.get((inv, lid, n_))
                    if tg is None or tg["status"] != "found":
                        continue
                    leaf = v.leaf(mp, n_)[1]
                    okm = tg["target_scan"] in leaf
                    emap_n += 1
                    emap_k += okm
                    if tg["confidence"] == "sure":
                        emap_sure_n += 1
                        emap_sure_k += okm
        P_, R_ = (tp / np_ if np_ else 0.0), (tp / ng if ng else 0.0)
        E = hit / pred_n if pred_n else 0.0
        out["cells"][nm] = dict(
            cell=cname(cell), TP=tp, n_pred=np_, n_gold=ng, P=round(P_, 4), R=round(R_, 4),
            P_wilson95=wilson(tp, np_), R_wilson95=wilson(tp, ng),
            htr_substituted=dict(TP=tp_h, n_gold=ng_h, P=round(tp_h / np_, 4) if np_ else 0.0,
                                 R=round(tp_h / ng_h, 4) if ng_h else 0.0),
            E_hits=hit, E_denominator=pred_n, E=round(E, 4), E_wilson95=wilson(hit, pred_n), n_excluded_not_found=excl,
            E_sure_only=dict(hits=hit_sure, n=pred_sure, E=round(hit_sure / pred_sure, 4) if pred_sure else 0.0,
                             wilson95=wilson(hit_sure, pred_sure), dropped_likely=excl_likely),
            E_map=dict(hits=emap_k, n=emap_n, share=round(emap_k / emap_n, 4) if emap_n else 0.0,
                       wilson95=wilson(emap_k, emap_n), sure_hits=emap_sure_k, sure_n=emap_sure_n),
            strata={k: dict(TP=v_[0], n_pred=v_[1], n_gold=v_[2], P=round(v_[0] / v_[1], 4) if v_[1] else None,
                            R=round(v_[0] / v_[2], 4) if v_[2] else None) for k, v_ in sorted(strat.items())},
            lines_not_on_oracle_toc_side=off_toc)
        if nm == "SEL":
            out["bar_L_pass"] = bool(P_ >= 0.95 and R_ >= 0.90)
            out["bar_E_pass"] = bool(E >= 0.90)
    # sampled lines per loc stratum
    cnt = Counter(s["loc"] for s in sampled.values() if s["status"] == "ok")
    out["sampled_ok_lines_by_loc"] = dict(cnt)
    out["sampled_ok_lines_with_gold_by_loc"] = dict(Counter(s["loc"] for s in sampled.values()
                                                            if s["status"] == "ok" and s["gold"]))
    write_tsv(HERE / "test_links.tsv", ["cell", "inv", "scan_id", "line_id", "number", "front_scan_id", "leaf", "method",
                                        "in_gold", "target_status", "confidence", "target_scan", "outcome", "category",
                                        "text"], link_rows)
    write_tsv(HERE / "test_lines.tsv", ["cell", "inv", "scan_id", "line_id", "loc", "text", "gold", "pred", "tp",
                                        "mono_dropped", "htr"], line_rows)
    # ----- end to end (reported only)
    sys.path.insert(0, str(P17 / "step-23d"))
    import eval_toc17 as T
    t23 = json.loads((P17 / "step-23d" / "select.json").read_text())
    cfg, voc_run = t23["cell"], t23["VOC_RUN"]
    e2e, e2e_sum = [], dict(lines=0, lines_with_gold=0, hit=0, n=0, excl=0, tp=0, np=0, ng=0)
    variant, mono, mp = sel_c
    for inv in invs:
        tv = T.Vol(TEST_ROOT, inv)
        fl = tv.flags(cfg)
        run = front_index_run(fl, **voc_run)
        ok_lines = [s for (i2, _l), s in sampled.items() if i2 == inv and s["status"] == "ok"]
        if run is None:
            e2e.append([inv, "none", 0, len(ok_lines), 0, 0, 0, 0, 0])
            e2e_sum["ng"] += sum(len(s["gold"]) for s in ok_lines)
            continue
        first, last, end = run
        keys = tv.keys
        pred_keys = {keys[i] for i in range(len(keys)) if i <= last and fl[i]}
        excl_keys = {keys[i] for i in range(len(keys)) if i <= last or fl[i]}
        ev = Vol(TEST_ROOT, inv, toc_keys=pred_keys, excl=excl_keys)
        kept, _d = ev.locs(variant, mono, mp)
        pbl = defaultdict(list)
        for r in kept:
            pbl[r["line_id"]].append(r["number"])
        on = h = n = ex = tpp = npp = ngg = 0
        for s in ok_lines:
            if (s["scan_id"], s["page"]) not in ev.part:
                ngg += len(s["gold"])
                continue
            on += 1
            P, G = Counter(pbl.get(s["line_id"], [])), Counter(s["gold"])
            tpp += sum((P & G).values())
            npp += sum(P.values())
            ngg += sum(G.values())
            for n_ in pbl.get(s["line_id"], []):
                tg = targets.get((inv, s["line_id"], n_)) if n_ in s["gold"] else None
                if tg is not None and tg["status"] == "not_found":
                    ex += 1
                    continue
                n += 1
                if tg is not None and tg["target_scan"] in ev.leaf(mp, n_)[1]:
                    h += 1
        e2e.append([inv, f"{first}-{last}/{end}", len(pred_keys), len(ok_lines), on, h, n, ex, tpp])
        e2e_sum["lines"] += on
        e2e_sum["lines_with_gold"] += sum(1 for s in ok_lines if s["gold"] and (s["scan_id"], s["page"]) in ev.part)
        e2e_sum["hit"] += h
        e2e_sum["n"] += n
        e2e_sum["excl"] += ex
        e2e_sum["tp"] += tpp
        e2e_sum["np"] += npp
        e2e_sum["ng"] += ngg
    write_tsv(HERE / "end_to_end.tsv", ["inv", "predicted_run", "predicted_toc_sides", "sampled_ok_lines",
                                        "sampled_on_predicted_sides", "hits", "predicted_links", "excluded", "tp"], e2e)
    es = e2e_sum
    out["end_to_end"] = dict(
        es, volumes_with_run=sum(r[1] != "none" for r in e2e), n_volumes=len(e2e),
        E=round(es["hit"] / es["n"], 4) if es["n"] else None, E_wilson95=wilson(es["hit"], es["n"]),
        P=round(es["tp"] / es["np"], 4) if es["np"] else None, R=round(es["tp"] / es["ng"], 4) if es["ng"] else None)
    # ----- silver (reported only)
    df = load_globalise_index(SILVER, inventories=invs)
    srows = []
    for nm, cell in cells:
        for inv in invs:
            nums = {r["number"] for r in vols[inv].locs(*cell)[0]}
            ref = {int(x) for x in df[df["inventory_number"] == inv]["folio_start"].dropna()}
            tpv = len(nums & ref)
            srows.append([inv, nm, len(nums), len(ref), tpv, round(tpv / len(nums), 4) if nums else "",
                          round(tpv / len(ref), 4) if ref else ""])
    write_tsv(HERE / "silver.tsv", ["inv", "cell", "n_numbers", "n_obp", "tp", "precision", "recall"], srows)
    sil = {}
    for nm, _c in cells:
        rr = [r for r in srows if r[1] == nm and r[3] > 0]
        a, b, c_ = sum(r[2] for r in rr), sum(r[3] for r in rr), sum(r[4] for r in rr)
        sil[nm] = dict(volumes_in_obp=len(rr), n_numbers=a, n_obp=b, tp=c_, precision=round(c_ / a, 4) if a else None,
                       recall=round(c_ / b, 4) if b else None)
    out["silver"] = sil
    dump_json(HERE / "test_done.json", out)
    print(json.dumps(out, indent=1))


# ---------------------------------------------------------------------------------------------------- errors
def stage_errors():
    assert (HERE / "test_done.json").exists()
    rng = random.Random(SEED)
    L = []
    links = [r for r in read_plain(HERE / "test_links.tsv") if r["cell"] == "SEL"]
    lines = [r for r in read_plain(HERE / "test_lines.tsv") if r["cell"] == "SEL"]
    miss = [r for r in links if r["outcome"] == "miss"]
    pick = sorted(rng.sample(miss, 30), key=lambda r: (r["inv"], r["scan_id"], r["line_id"])) if len(miss) > 30 else miss
    L.append(f"## SEL E misses: {len(pick)} of {len(miss)}")
    for r in pick:
        L.append(f"{r['inv']}\t{r['scan_id'][-4:]}\tN={r['number']}\tgold={r['in_gold']}\ttarget={r['target_scan'][-4:]}"
                 f"\tleaf={','.join(s[-4:] for s in r['leaf'].split(',') if s)}\t{r['method']}\t{r['category']}\t| {r['text'][:150]}")
    fn = []
    for r in lines:
        G, P = Counter(int(x) for x in r["gold"].split(";") if x), Counter(int(x) for x in r["pred"].split(";") if x)
        for k in (G - P):
            fn.append((r, k))
    pick = sorted(rng.sample(fn, 20), key=lambda x: (x[0]["inv"], x[0]["line_id"])) if len(fn) > 20 else fn
    L.append(f"## L false negatives (gold folio not predicted): {len(pick)} of {len(fn)}")
    for r, k in pick:
        L.append(f"{r['inv']}\t{r['scan_id'][-4:]}\tgold={k}\tpred={r['pred']}\tdropped_by_mono={r['mono_dropped']}\t| {r['text'][:150]}")
    fp = []
    for r in lines:
        G, P = Counter(int(x) for x in r["gold"].split(";") if x), Counter(int(x) for x in r["pred"].split(";") if x)
        if P - G:
            fp.append(r)
    L.append(f"## L false positives (lines with a predicted number not in gold): {len(fp)}")
    for r in fp[:30]:
        L.append(f"{r['inv']}\t{r['scan_id'][-4:]}\tgold={r['gold']}\tpred={r['pred']}\t| {r['text'][:150]}")
    hits = [r for r in links if r["outcome"] == "hit"]
    pick = sorted(rng.sample(hits, 10), key=lambda r: (r["inv"], r["line_id"])) if len(hits) > 10 else hits
    L.append(f"## SEL E hits: {len(pick)} of {len(hits)}")
    for r in pick:
        L.append(f"{r['inv']}\t{r['scan_id'][-4:]}\tN={r['number']}\ttarget={r['target_scan'][-4:]}\t{r['method']}\t{r['confidence']}\t| {r['text'][:150]}")
    md = []
    for r in lines:
        G = set(int(x) for x in r["gold"].split(";") if x)
        D = set(int(x) for x in r["mono_dropped"].split(";") if x)
        if G & D:
            md.append(r)
    L.append(f"## lines where mono dropped a gold folio: {len(md)}")
    for r in md:
        L.append(f"{r['inv']}\t{r['scan_id'][-4:]}\tgold={r['gold']}\tdropped={r['mono_dropped']}\tpred={r['pred']}\t| {r['text'][:150]}")
    (HERE / "errors_dump.txt").write_text("\n".join(L) + "\n")
    (HERE / "errors_dump.txt").chmod(0o664)
    print("\n".join(L))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["l8check", "dev", "test", "errors"])
    a = ap.parse_args()
    {"l8check": stage_l8check, "dev": stage_dev, "test": stage_test, "errors": stage_errors}[a.stage]()
