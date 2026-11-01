"""Step 22d: index links on VOC tables of contents (TOCs), bar E against GLOBALISE document starts. Single process.
Run from the AS root (read only on the layer; writes only into this directory):

    PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22d/eval_links.py --stage dev
    ... --stage test       # ONCE (refuses when test_done.json exists): test_done.json, links_eval.tsv, silver.tsv, end_to_end.tsv
    ... --stage errors     # after test: text dumps of dev/test non-hits and of dev lines without a locator

Reads: data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5 (a link to T7_Data), planning/toc_gold.tsv (frozen),
step-22b/select.json (the map cell), the OBP silver csv under ground_truth/. Gold sides: TOC scans from toc_gold.tsv.
"""
import argparse
import csv
import datetime
import json
import math
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.analysis.folio_map import FOLIO_MAP_V2, FolioMapParams, build_folio_map  # noqa: E402
from archival_structures.analysis.index_links import (  # noqa: E402
    SG_LOCATOR_PARAMS, VOC_LOCATOR_PARAMS, extract_locators, read_volume_inputs, volume_links)
from archival_structures.datasets.globalise_index import load_globalise_index  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

LAYER = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
GOLD = Path("reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv")
SEL22B = Path("reports/data/phase-16-voc-generalisation/step-22b/select.json")
SILVER = Path("ground_truth/NL-HaNA_1.04.02/globalise_digitized_indexes/source/"
              "GLOBALISE - Digitized Indexes of the Dutch East India Company OBP (1602-1799).csv")
HERE = Path(__file__).resolve().parent
DEV = "1267 1274 1388 1547 1557 2548 2555 3142 7923 8121 8697".split()
TEST = "1120 1529 1539 2448 3891 8023 8237 8276 8284 8834".split()   # 2775 apart
SPECIAL = "2775"
ALL = DEV + TEST + [SPECIAL]
V0, V2 = FolioMapParams(), FOLIO_MAP_V2
L2, L7, L8 = SG_LOCATOR_PARAMS["L2"], SG_LOCATOR_PARAMS["L7"], VOC_LOCATOR_PARAMS["L8"]
# SEL = 22b's frozen cell: M = V0 = FolioMapParams(), S = off, N = ("page-number",)
SEL_MAP, SEL_TAGS = V0, ("page-number",)
# name: (locator params, entry source, map params, number_tags); order = tie preference after the best
CELLS = {
    "U2": (L7, "units", V2, ()),
    "U1": (L2, "units", V0, ()),
    "C1": (L7, "units", SEL_MAP, SEL_TAGS),
    "C2": (L8, "lines", V2, ()),
    "C3": (L8, "lines", SEL_MAP, SEL_TAGS),
}
SEED = 20261031
GAP = 20


def wilson(k, n, z=1.96):
    if n == 0:
        return [None, None]
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return [round(c - h, 4), round(c + h, 4)]


def write_tsv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    path.chmod(0o664)


def load_gold():
    g = defaultdict(dict)
    with open(GOLD, newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            # a scan can have several GLOBALISE CSV rows (start row followed by a non-start row): take the max (22r)
            old = g[r["inv"]].get(r["scan_id"], (0, 0))
            g[r["inv"]][r["scan_id"]] = (max(old[0], int(r["toc"])), max(old[1], int(r["start"])))
    return g


class Vol:
    """One volume: layer tables, gold TOC sides (front run and later TOCs), gold starts, per-map-cell maps."""

    def __init__(self, inv, gold):
        self.inv = inv
        g = gold[inv]
        self.gold_starts = {s for s, (t, st) in g.items() if st}
        self.base = read_volume_inputs(LAYER, inv)
        self.scan_ids = self.base.scan_ids
        self.pos = {s: i for i, s in enumerate(self.scan_ids)}
        self.toc_idx = [i for i, sd in enumerate(self.base.sides) if g.get(sd[0], (0, 0))[0]]
        self.part = {}
        front_last = None
        prev = None
        for i in self.toc_idx:
            if prev is not None and i - prev > GAP:
                break
            front_last = i
            prev = i
        for i in self.toc_idx:
            self.part[self.base.sides[i]] = "front" if front_last is not None and (
                i <= front_last) else "later"
        self.front_last_scan = self.base.sides[front_last][0] if front_last is not None else None
        self.front_last_idx = front_last
        self.excl = ({s for i, s in enumerate(self.base.sides) if front_last is not None and i <= front_last}
                     | set(self.part))
        self.units = [u for u in read_tsv_gz(LAYER / "units" / "units" / f"{inv}.tsv.gz")
                      if (u["scan_id"], u["page"]) in self.part]
        self.utext = {(r["scan_id"], r["unit_id"]): r["text"]
                      for r in read_tsv_gz(LAYER / "join" / "units" / f"{inv}.tsv.gz")}
        page_of = {(r["scan_id"], r["line_id"]): r["page"]
                   for r in read_tsv_gz(LAYER / "layout" / "lines" / f"{inv}.tsv.gz")}
        self.lines = []   # (scan_id, page, line_id, text) in file order, TOC sides only
        for r in read_tsv_gz(LAYER / "lines" / f"{inv}.lines.tsv.gz"):
            pg = page_of.get((r["scan_id"], r["line_id"]))
            if pg is not None and (r["scan_id"], pg) in self.part and r["text"].strip():
                self.lines.append((r["scan_id"], pg, r["line_id"], r["text"]))
        self._maps = {}

    def fmap(self, mparams, tags):
        key = (id(mparams), tags)
        if key not in self._maps:
            v = read_volume_inputs(LAYER, self.inv, number_tags=tags) if tags else self.base
            self._maps[key] = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=self.excl, params=mparams)
        return self._maps[key]

    def entries(self, source):
        if source == "units":
            return [(u["scan_id"], u["page"], u["unit_id"], self.utext.get((u["scan_id"], u["unit_id"]), ""))
                    for u in self.units if u["unit_type"] == "main_text"]
        return [(s, p, lid, t) for s, p, lid, t in self.lines]

    def links(self, cell):
        params, source, mparams, tags = CELLS[cell]
        fmap = self.fmap(mparams, tags)
        rng = (int(fmap.chain[0, 1]), int(fmap.chain[-1, 1])) if len(fmap.chain) else None
        rows = []
        for sid, page, src_id, text in self.entries(source):
            for k, loc in enumerate(extract_locators(text, params, folio_range=rng)):
                t = fmap.locate(loc.number)
                row = dict(inv=self.inv, cell=cell, scan_id=sid, page=page, source_id=src_id, loc_idx=k,
                           number=loc.number, front_scan_id="", method="none", leaf_hit=0, chance_hit=0,
                           toc_part=self.part[(sid, page)])
                if t is not None:
                    p = self.pos[t.front[0]]
                    leaf = self.scan_ids[p:p + 2]
                    chance = self.scan_ids[p + 3:p + 5]
                    row.update(front_scan_id=t.front[0], method=t.method,
                               leaf_hit=int(any(s in self.gold_starts for s in leaf)),
                               chance_hit=int(any(s in self.gold_starts for s in chance)))
                    row["_leaf"] = leaf
                rows.append(row)
        return rows

    def coverage(self, rows):
        """(covered, total): gold starts after the front run's last scan that lie in some link's leaf."""
        after = {s for s in self.gold_starts
                 if self.front_last_idx is not None and self.pos[s] > self.pos[self.front_last_scan]}
        cov = {s for r in rows for s in r.get("_leaf", ()) if s in after}
        return len(cov), len(after)


def score(vols, invs, cell):
    """-> dict of pooled counts for the cell over invs."""
    n = hit = chance = loc = cov = tot = 0
    per = {}
    for inv in invs:
        rows = vols[inv].links(cell)
        c, t = vols[inv].coverage(rows)
        nn, hh = len(rows), sum(r["leaf_hit"] for r in rows)
        per[inv] = dict(n=nn, hit=hh, chance=sum(r["chance_hit"] for r in rows),
                        located=sum(r["method"] != "none" for r in rows), cov=c, cov_total=t)
        n += nn
        hit += hh
        chance += per[inv]["chance"]
        loc += per[inv]["located"]
        cov += c
        tot += t
    E = hit / n if n else 0.0
    C = cov / tot if tot else 0.0
    return dict(n=n, hit=hit, E=E, E_wilson=wilson(hit, n), chance=chance, chance_E=chance / n if n else 0.0,
                located=loc, located_share=loc / n if n else 0.0, cov=cov, cov_total=tot, coverage=C,
                F1=(2 * E * C / (E + C)) if E + C else 0.0, per_volume=per)


def stage_dev():
    gold = load_gold()
    vols = {i: Vol(i, gold) for i in DEV}
    res = {c: score(vols, DEV, c) for c in CELLS}
    write_tsv(HERE / "dev_grid.tsv", ["cell", "locators", "entries", "map", "number_tags", "n_links", "hits", "E", "located",
                                      "cov", "cov_total", "coverage", "chance_E", "F1"],
              [[c, CELLS[c][0].variant, CELLS[c][1], "V2" if CELLS[c][2] is V2 else "V0", "|".join(CELLS[c][3]) or "-",
                r["n"], r["hit"], round(r["E"], 4), round(r["located_share"], 4), r["cov"], r["cov_total"],
                round(r["coverage"], 4), round(r["chance_E"], 4), round(r["F1"], 4)] for c, r in res.items()])
    best = max(r["F1"] for r in res.values())
    cand = [c for c in CELLS if res[c]["F1"] >= best - 0.005]     # CELLS order = tie order: U2, U1, fewest changes
    sel = cand[0]
    out = {"selected": sel, "dev_F1": round(res[sel]["F1"], 4), "best_dev_F1": round(best, 4),
           "candidates_within_0.005": cand, "dev_E": round(res[sel]["E"], 4), "dev_coverage": round(res[sel]["coverage"], 4),
           "all_F1": {c: round(r["F1"], 4) for c, r in res.items()}, "sel_22b": "V0-off-TL",
           "written_at": datetime.datetime.now().isoformat(timespec="seconds"), "test_numbers_seen": False}
    (HERE / "select.json").write_text(json.dumps(out, indent=1))
    (HERE / "select.json").chmod(0o664)
    # dev links of every cell, for the error analysis
    rows = []
    for c in CELLS:
        for i in DEV:
            rows += vols[i].links(c)
    write_tsv(HERE / "dev_links.tsv", LINK_HEADER, [[r[h] for h in LINK_HEADER] for r in rows])
    print(json.dumps(out, indent=1))
    for c, r in res.items():
        print(c, {k: v for k, v in r.items() if k != "per_volume"})


LINK_HEADER = ["inv", "cell", "scan_id", "page", "source_id", "loc_idx", "number", "front_scan_id", "method",
               "leaf_hit", "chance_hit", "toc_part"]


def silver_rows(vols, invs, cells, split):
    df = load_globalise_index(SILVER, inventories=invs)
    out = []
    for c in cells:
        for inv in invs:
            nums = {r["number"] for r in vols[inv].links(c)}
            ref = {int(x) for x in df[df["inventory_number"] == inv]["folio_start"].dropna()}
            tp = len(nums & ref)
            out.append([inv, split, c, len(nums), len(ref), tp, round(tp / len(nums), 4) if nums else "",
                        round(tp / len(ref), 4) if ref else ""])
    return out


def stage_test():
    assert (HERE / "select.json").exists() and not (HERE / "test_done.json").exists(), "test runs once"
    gold = load_gold()
    sel = json.loads((HERE / "select.json").read_text())["selected"]
    cells = list(dict.fromkeys(["U1", "U2", sel]))
    vols = {i: Vol(i, gold) for i in TEST + [SPECIAL]}
    out = {"run_at": datetime.datetime.now().isoformat(timespec="seconds"), "selected": sel, "cells": {}}
    link_rows = []
    for c in cells:
        r = score(vols, TEST, c)
        s2775 = score(vols, [SPECIAL], c)
        for k in ("n", "hit"):
            pass
        r["bar_E_pass"] = bool(r["E"] >= 0.90)
        r["inv_2775"] = {k: v for k, v in s2775.items() if k != "per_volume"}
        out["cells"][c] = r
        for i in TEST + [SPECIAL]:
            for row in vols[i].links(c):
                link_rows.append([("test" if i != SPECIAL else "2775")] + [row[h] for h in LINK_HEADER])
    out["verdict"] = ("unchanged" if out["cells"]["U2"]["bar_E_pass"] else
                      "reparameterised" if any(out["cells"][c]["bar_E_pass"] for c in cells if c not in ("U1", "U2"))
                      else "fails on VOC")
    write_tsv(HERE / "links_eval.tsv", ["split"] + LINK_HEADER, link_rows)
    write_tsv(HERE / "silver.tsv", ["inv", "split", "cell", "n_numbers", "n_obp", "tp", "precision", "recall"],
              silver_rows(vols, TEST + [SPECIAL], cells, "test"))
    # end to end, unchanged, the 22 volumes (also dev), with the layer's own flags
    e2e = []
    for inv in ALL:
        inputs = read_volume_inputs(LAYER, inv)
        units = list(read_tsv_gz(LAYER / "units" / "units" / f"{inv}.tsv.gz"))
        texts = {(r["scan_id"], r["unit_id"]): r["text"]
                 for r in read_tsv_gz(LAYER / "join" / "units" / f"{inv}.tsv.gz")}
        res = volume_links(inputs, units, texts, L2)
        gs = {s for s, (t, st) in gold[inv].items() if st}
        if res is None:
            e2e.append([inv, "none", 0, 0, 0])
            continue
        hits = 0
        pos = {s: i for i, s in enumerate(inputs.scan_ids)}
        for lk in res["links"]:
            if lk["front_scan_id"]:
                p = pos[lk["front_scan_id"]]
                hits += int(any(s in gs for s in inputs.scan_ids[p:p + 2]))
        e2e.append([inv, "%d-%d-%d" % res["front"], len(res["links"]), sum(1 for lk in res["links"] if lk["front_scan_id"]), hits])
    write_tsv(HERE / "end_to_end.tsv", ["inv", "front", "links", "located", "leaf_hits"], e2e)
    out["end_to_end"] = {"volumes_with_front": sum(r[1] != "none" for r in e2e), "n": len(e2e),
                         "links": sum(r[2] for r in e2e), "hits": sum(r[4] for r in e2e)}
    (HERE / "test_done.json").write_text(json.dumps(out, indent=1, default=str))
    (HERE / "test_done.json").chmod(0o664)
    for c, r in out["cells"].items():
        print(c, {k: v for k, v in r.items() if k not in ("per_volume",)})
    print(out["verdict"], out["end_to_end"])


def stage_errors():
    """Dumps for reading: dev non-hits of the selected cell (30, seed), all test non-hits (<=30), dev lines with no
    L8 locator whose text holds an OBP folio_start of the volume (15). Only after the test stage."""
    assert (HERE / "test_done.json").exists()
    gold = load_gold()
    sel = json.loads((HERE / "select.json").read_text())["selected"]
    rng = random.Random(SEED)
    lines = []

    def near(vol, row):
        p = vol.pos.get(row["front_scan_id"])
        if p is None:
            return "unlocated"
        ds = sorted((abs(vol.pos[s] - p), s) for s in vol.gold_starts)
        return f"nearest gold start {ds[0][1][-4:]} at {vol.pos[ds[0][1]] - p:+d} leaves" if ds else "no gold starts"

    def dump(split, invs, n, tag):
        vols = {i: Vol(i, gold) for i in invs}
        txt = {}
        miss = []
        for i in invs:
            v = vols[i]
            ent = {(s, src): t for s, p, src, t in v.entries(CELLS[sel][1])}
            for r in v.links(sel):
                if not r["leaf_hit"]:
                    miss.append((v, r, ent[(r["scan_id"], r["source_id"])]))
        pick = sorted(rng.sample(miss, n), key=lambda m: (m[0].inv, m[1]["scan_id"])) if len(miss) > n else miss
        lines.append(f"## {tag}: {len(pick)} of {len(miss)} non-hits")
        for v, r, t in pick:
            lines.append(f"{v.inv}\t{r['scan_id'][-4:]}\tloc={r['number']}\tfront={r['front_scan_id'][-4:] or '-'}\t"
                         f"{r['method']}\t{near(v, r)}\t| {t[:160]}")
        return vols

    dvols = dump("dev", DEV, 30, f"dev non-hits ({sel})")
    dump("test", TEST, 30, f"test non-hits ({sel})")
    df = load_globalise_index(SILVER, inventories=DEV)
    cand = []
    for i in DEV:
        ref = {str(int(x)) for x in df[df["inventory_number"] == i]["folio_start"].dropna()}
        for s, p, lid, t in dvols[i].lines:
            if not extract_locators(t, L8):
                import re
                if any(m in ref for m in re.findall(r"(?<!\d)\d{1,4}(?!\d)", t)):
                    cand.append((i, s, p, t))
    pick = sorted(rng.sample(cand, 15), key=lambda c: (c[0], c[1])) if len(cand) > 15 else cand
    lines.append(f"## dev lines without an L8 locator whose text holds an OBP folio_start: {len(pick)} of {len(cand)}")
    for i, s, p, t in pick:
        lines.append(f"{i}\t{s[-4:]}\t{p}\t| {t[:160]}")
    Path(HERE / "errors_dump.txt").write_text("\n".join(lines) + "\n")
    (HERE / "errors_dump.txt").chmod(0o664)
    print("\n".join(lines))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["dev", "test", "errors"])
    a = ap.parse_args()
    {"dev": stage_dev, "test": stage_test, "errors": stage_errors}[a.stage]()
