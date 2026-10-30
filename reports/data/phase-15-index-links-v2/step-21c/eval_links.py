"""Step 21c: dev grid + selection (--part dev), then one test run (--part test) of locators and entry assembly.

Run from the AS root with .venv/bin/python (single process). Inputs (read only):
  data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5   (the v5 layer)
  reports/data/phase-15-index-links-v2/step-21a/{side_verdicts,line_gold,embedded_entries,targets,target_gold}.tsv
Outputs go to --out-dir (default: this script's directory).

--part dev   dev rows only (the split comes from side_verdicts.tsv; test rows of any gold file are dropped on read).
             Writes dev_grid.tsv (4 methods x 4 variants), select.json, dev_errors.tsv (selected combination), dev_extra.json.
--part test  refuses unless select.json exists; writes test_done.json FIRST and refuses if it exists. Scores the selection
             and the v1 baseline (A0 + L2) on the test sides: test_sides.tsv, test_errors.tsv, test_targets.tsv.
"""
import argparse
import csv
import datetime
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.analysis.folio_map import FOLIO_MAP_V2, build_folio_map
from archival_structures.analysis.index_links import (
    SG_LOCATOR_PARAMS, assemble_entries, entry_locators, extract_locators, front_index_run, read_side_lines,
    read_volume_inputs)
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import SG_HAND_COLUMN_ORDER_V5, SG_HAND_V2

HERE = Path(__file__).resolve().parent
LAYER = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
GOLD = Path("reports/data/phase-15-index-links-v2/step-21a")
METHODS = ("A0", "A1", "A2", "A3")
VARIANTS = ("L2", "L5", "L6", "L7")


def read_rows(path, keep):
    """Stream a TSV; keep only rows for which keep(row) is true (others are dropped without being stored)."""
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            if keep(r):
                out.append(r)
    return out


def load_gold(split):
    verd = {r["read_id"]: r for r in read_rows(GOLD / "side_verdicts.tsv", lambda r: r["split"] == split)}
    ids = {k for k, r in verd.items() if r["side_verdict"] == "index"}
    sides = {}
    for r in read_rows(GOLD / "line_gold.tsv", lambda r: r["read_id"] in ids):
        sides.setdefault(r["read_id"], []).append(r)
    emb = defaultdict(list)
    if (GOLD / "embedded_entries.tsv").exists():
        for r in read_rows(GOLD / "embedded_entries.tsv", lambda r: r["read_id"] in ids):
            emb[r["read_id"]].append(r)
    out = {}
    for rid in sorted(ids):
        rows = sorted(sides.get(rid, []), key=lambda r: int(r["k"]))
        v = verd[rid]
        first = {r["entry"]: r["line_id"] for r in rows if r["label"] == "S" and r["entry"]}
        glocs, glinks = Counter(), Counter()
        for r in rows:
            for tok in filter(None, (t.strip() for t in r["locators"].split(";"))):
                n, _, a = tok.partition("@")
                ent = a or r["entry"]
                glocs[(r["line_id"], int(n))] += 1
                glinks[(r["line_id"], int(n), first.get(ent))] += 1
        neutral = Counter()
        for r in emb.get(rid, []):
            for tok in filter(None, (t.strip() for t in r["locators"].split(";"))):
                n = int(tok.partition("@")[0])
                neutral[(r["line_id"], n)] += 1
        out[rid] = dict(inv=v["inv"], side=(v["scan_id"], v["page"]), rows=rows, locs=glocs + neutral, links=glinks,
                        neutral=neutral)
    return out


class Volume:
    def __init__(self, inv):
        self.inv = inv
        vi = read_volume_inputs(LAYER, inv)
        run = front_index_run(vi.flags)
        self.fmap, self.rng = None, None
        if run is not None:
            last = run[1]
            excl = {s for i, s in enumerate(vi.sides) if i <= last or vi.flags[i]}
            self.fmap = build_folio_map(vi.scan_ids, vi.sides, vi.numbers, exclude=excl, params=FOLIO_MAP_V2)
            if len(self.fmap.chain):
                self.rng = (int(self.fmap.chain[0, 1]), int(self.fmap.chain[-1, 1]))
        self.wbc = (SG_HAND_V2.wbc_pinned or {}).get(inv) or SG_HAND_V2.wbc_pinned["default"]
        self.text = {(r["scan_id"], r["line_id"]): r["text"]
                     for r in read_tsv_gz(LAYER / "lines" / f"{inv}.lines.tsv.gz")}
        self._v1 = None
        self._lines = {}
        self._asm = {}

    def v1_entries(self, side):
        """A0: [(text, [(char_start, char_end, line_id)], first_line_id)] for the side's main_text units."""
        if self._v1 is None:
            root = LAYER
            units = [u for u in read_tsv_gz(root / "units" / "units" / f"{self.inv}.tsv.gz")
                     if u["unit_type"] == "main_text"]
            txt = {(r["scan_id"], r["unit_id"]): r["text"]
                   for r in read_tsv_gz(root / "join" / "units" / f"{self.inv}.tsv.gz")}
            segs = defaultdict(list)
            for g in read_tsv_gz(root / "join" / "segments" / f"{self.inv}.tsv.gz"):
                segs[(g["scan_id"], g["unit_id"])].append(g)
            by_side = defaultdict(list)
            for u in sorted(units, key=lambda u: int(u["unit_order"])):
                key = (u["scan_id"], u["unit_id"])
                gs = sorted(segs.get(key, []), key=lambda g: (int(g["seg_order"]), int(g["seg_idx"] or 0)))
                by_side[(u["scan_id"], u["page"])].append(
                    (txt.get(key, ""), [(int(g["char_start"]), int(g["char_end"]), g["line_id"]) for g in gs],
                     gs[0]["line_id"] if gs else None))
            self._v1 = by_side
        return self._v1.get(side, [])

    def lines(self, side):
        if side not in self._lines:
            self._lines.update(read_side_lines(LAYER, self.inv, [side]))
        return self._lines[side]

    def assembled(self, side, method):
        key = (side, method)
        if key not in self._asm:
            self._asm[key] = assemble_entries(self.lines(side), method, wbc=self.wbc,
                                              column_cfg=SG_HAND_COLUMN_ORDER_V5)
        return self._asm[key]


def predict(vol, side, method, variant):
    """-> list of (line_id, number, first_line_id)."""
    params = SG_LOCATOR_PARAMS[variant]
    out = []
    if method == "A0":
        for text, segs, first in vol.v1_entries(side):
            for loc in extract_locators(text, params, folio_range=vol.rng):
                lid = next((g[2] for g in segs if g[0] <= loc.char_start < g[1]), None)
                if lid is None and segs:
                    lid = [g for g in segs if g[0] <= loc.char_start][-1][2] if segs[0][0] <= loc.char_start else segs[0][2]
                out.append((lid, loc.number, first))
        return out
    for e in vol.assembled(side, method):
        for loc, lid in entry_locators(e, params, folio_range=vol.rng):
            out.append((lid, loc.number, e.line_ids[0]))
    return out


def score_side(g, pred):
    """Counts for one side. Embedded (neutral) pairs count as locators, and are dropped from links and attribution."""
    plocs = Counter((l, n) for l, n, _ in pred)
    loc_tp = sum((plocs & g["locs"]).values())
    neutral = g["neutral"].copy()
    kept = []
    for l, n, f in pred:
        if neutral[(l, n)] > 0:
            neutral[(l, n)] -= 1
            continue
        kept.append((l, n, f))
    n_neutral = len(pred) - len(kept)
    plinks = Counter(kept)
    link_tp = sum((plinks & g["links"]).values())
    return dict(loc_tp=loc_tp, loc_fp=len(pred) - loc_tp, loc_fn=sum(g["locs"].values()) - loc_tp,
                link_tp=link_tp, link_fp=len(kept) - link_tp, link_fn=sum(g["links"].values()) - link_tp,
                n_neutral=n_neutral, n_embedded_gold=sum(g["neutral"].values()), loc_tp_nonneutral=loc_tp - n_neutral)


def ratios(c):
    d = lambda a, b: a / b if b else 0.0
    P, R = d(c["loc_tp"], c["loc_tp"] + c["loc_fp"]), d(c["loc_tp"], c["loc_tp"] + c["loc_fn"])
    lp, lr = d(c["link_tp"], c["link_tp"] + c["link_fp"]), d(c["link_tp"], c["link_tp"] + c["link_fn"])
    return dict(loc_P=P, loc_R=R, link_F1=d(2 * lp * lr, lp + lr), attribution=d(c["link_tp"], c["loc_tp_nonneutral"]))


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    den = 1 + z * z / n
    c = (p + z * z / (2 * n)) / den
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return (c - h, c + h)


def total(cs):
    t = Counter()
    for c in cs:
        t.update(c)
    return dict(t)


def write_tsv(path, header, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(header) + "\n")
        for r in rows:
            f.write("\t".join(str(r[h]) if not isinstance(r[h], float) else f"{r[h]:.4f}" for h in header) + "\n")
    path.chmod(0o664)


def errors_of(gold, vols, method, variant):
    """Rows for locator FP/FN and attribution errors of one combination."""
    rows = []
    for rid, g in gold.items():
        vol = vols[g["inv"]]
        pred = predict(vol, g["side"], method, variant)
        kz = {r["line_id"]: r["k"] for r in g["rows"]}
        plocs = Counter((l, n) for l, n, _ in pred)
        txt = lambda l: vol.text.get((g["side"][0], l), "")
        for (l, n), c in (g["locs"] - plocs).items():
            rows.append(dict(read_id=rid, kind="loc_FN", k=kz.get(l, ""), line_id=l, text=txt(l), gold=n, predicted=""))
        for (l, n), c in (plocs - g["locs"]).items():
            rows.append(dict(read_id=rid, kind="loc_FP", k=kz.get(l, ""), line_id=l, text=txt(l), gold="", predicted=n))
        neutral = g["neutral"].copy()
        kept = []
        for l, n, f in pred:
            if neutral[(l, n)] > 0:
                neutral[(l, n)] -= 1
            else:
                kept.append((l, n, f))
        pl = Counter(kept)
        gl = g["links"]
        for (l, n, f), c in (pl - gl).items():
            if (l, n) in g["locs"] and (l, n) not in g["neutral"]:
                gf = [x[2] for x in gl if x[0] == l and x[1] == n]
                rows.append(dict(read_id=rid, kind="attribution", k=kz.get(l, ""), line_id=l, text=txt(l),
                                 gold=f"{n}->{gf}", predicted=f"{n}->{f}"))
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--part", required=True, choices=["dev", "test"])
    ap.add_argument("--out-dir", default=str(HERE))
    a = ap.parse_args(argv)
    out = Path(a.out_dir)
    log = open(out / "eval_links.txt", "a", encoding="utf-8")

    def say(*s):
        print(*s)
        print(*s, file=log)
        log.flush()

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sel_path, done_path = out / "select.json", out / "test_done.json"

    if a.part == "dev":
        if done_path.exists():
            raise SystemExit("test_done.json exists: the dev part must not be re-run after the test")
        gold = load_gold("dev")
        say(f"== dev part {now}: {len(gold)} index sides")
        vols = {}
        for g in gold.values():
            vols.setdefault(g["inv"], Volume(g["inv"]))
        grid, extra = [], {"reorder": {}, "headings": {}, "n_entries": {}}
        for m in METHODS:
            if m in ("A2", "A3"):
                moved = sum(side_moved(vols[g["inv"]], g["side"]) for g in gold.values())
                extra["reorder"][m] = f"{moved}/{len(gold)}"
            if m != "A0":
                ents = [e for g in gold.values() for e in vols[g["inv"]].assembled(g["side"], m)]
                extra["headings"][m] = sum(e.kind == "heading" for e in ents)
                extra["n_entries"][m] = sum(e.kind == "entry" for e in ents)
            for v in VARIANTS:
                cs = [score_side(g, predict(vols[g["inv"]], g["side"], m, v)) for g in gold.values()]
                t = total(cs)
                grid.append(dict(method=m, variant=v, **{k: t.get(k, 0) for k in (
                    "loc_tp", "loc_fp", "loc_fn")}, **ratios(t), **{k: t.get(k, 0) for k in (
                    "link_tp", "link_fp", "link_fn", "n_neutral", "n_embedded_gold")}))
        hdr = ["method", "variant", "loc_tp", "loc_fp", "loc_fn", "loc_P", "loc_R", "link_tp", "link_fp", "link_fn",
               "link_F1", "attribution", "n_neutral", "n_embedded_gold"]
        write_tsv(out / "dev_grid.tsv", hdr, grid)
        best = min(grid, key=lambda r: (-round(r["link_F1"], 9), METHODS.index(r["method"]), VARIANTS.index(r["variant"])))
        if sel_path.exists():
            raise SystemExit("select.json exists already; not overwriting")
        sel_path.write_text(json.dumps(dict(method=best["method"], variant=best["variant"],
                                            dev_link_F1=round(best["link_F1"], 6), run_utc=now), indent=1) + "\n")
        sel_path.chmod(0o664)
        for r in grid:
            say("\t".join(f"{r[h]:.3f}" if isinstance(r[h], float) else str(r[h]) for h in hdr))
        say("selection:", best["method"], best["variant"], f"{best['link_F1']:.4f}")
        write_tsv(out / "dev_errors.tsv", ["read_id", "kind", "k", "line_id", "text", "gold", "predicted"],
                  errors_of(gold, vols, best["method"], best["variant"]))
        (out / "dev_extra.json").write_text(json.dumps(extra, indent=1) + "\n")
        (out / "dev_extra.json").chmod(0o664)
        say("extra:", json.dumps(extra))
        return 0

    # --- test part ---
    if not sel_path.exists():
        raise SystemExit("select.json missing: run --part dev first")
    if done_path.exists():
        raise SystemExit("test_done.json exists: the test part has run (once only)")
    done_path.write_text(json.dumps(dict(started_utc=now), indent=1) + "\n")
    done_path.chmod(0o664)
    sel = json.loads(sel_path.read_text())
    m, v = sel["method"], sel["variant"]
    gold = load_gold("test")
    say(f"== test part {now}: selection {m} {v}; {len(gold)} index sides")
    vols = {}
    for g in gold.values():
        vols.setdefault(g["inv"], Volume(g["inv"]))
    side_rows, cs_sel, cs_base = [], [], []
    for rid, g in gold.items():
        vol = vols[g["inv"]]
        cs = score_side(g, predict(vol, g["side"], m, v))
        cb = score_side(g, predict(vol, g["side"], "A0", "L2"))
        cs_sel.append(cs)
        cs_base.append(cb)
        side_rows.append(dict(read_id=rid, inv=g["inv"], **{f"sel_{k}": cs[k] for k in cs},
                              **{f"v1_{k}": cb[k] for k in cb}))
    write_tsv(out / "test_sides.tsv", list(side_rows[0]) if side_rows else ["read_id"], side_rows)
    write_tsv(out / "test_errors.tsv", ["read_id", "kind", "k", "line_id", "text", "gold", "predicted"],
              errors_of(gold, vols, m, v))
    # targets
    tg = {r["target_id"]: r for r in read_rows(GOLD / "target_gold.tsv", lambda r: True)} \
        if (GOLD / "target_gold.tsv").exists() else {}
    trows = []
    for t in read_rows(GOLD / "targets.tsv", lambda r: r["split"] == "test"):
        gr = tg.get(t["target_id"])
        if gr is None or gr["target_scan_id"] == "none" or gr["folio_evidence"] == "unclear" or t["read_id"] not in gold:
            continue
        g = gold[t["read_id"]]
        vol = vols[g["inv"]]
        n = int(t["target_locator"])
        found = entry_ok = map_ok = e2e = False
        for l, num, first in predict(vol, g["side"], m, v):
            if num != n:
                continue
            found = True
            ok_e = first == t["first_line_id"]
            tgt = vol.fmap.locate(num) if vol.fmap is not None else None
            ok_m = tgt is not None and tgt.front[0] == gr["target_scan_id"]
            entry_ok |= ok_e
            map_ok |= ok_m
            e2e |= ok_e and ok_m
        trows.append(dict(target_id=t["target_id"], found=int(found), entry_ok=int(entry_ok), map_ok=int(map_ok),
                          e2e_hit=int(e2e)))
    write_tsv(out / "test_targets.tsv", ["target_id", "found", "entry_ok", "map_ok", "e2e_hit"], trows)
    for name, cs in (("selection " + m + v, cs_sel), ("v1 baseline A0L2", cs_base)):
        t = total(cs)
        r = ratios(t)
        say(name, {k: t.get(k, 0) for k in t})
        for lab, k, n in (("P", t["loc_tp"], t["loc_tp"] + t["loc_fp"]), ("R", t["loc_tp"], t["loc_tp"] + t["loc_fn"]),
                          ("A", t["link_tp"], t["loc_tp_nonneutral"])):
            lo, hi = wilson(k, n)
            say(f"  {lab} {k}/{n} = {k / n if n else 0:.3f}  95% Wilson [{lo:.3f}, {hi:.3f}]")
        say(f"  link_F1 {r['link_F1']:.3f}")
    if side_rows:
        k, n = sum(r["e2e_hit"] for r in trows), len(trows)
        lo, hi = wilson(k, n)
        say(f"E (selection) {k}/{n} = {k / n if n else 0:.3f}  95% Wilson [{lo:.3f}, {hi:.3f}]")
    return 0


def side_moved(vol, side):
    from archival_structures.analysis.index_links import side_reading_order
    return int(side_reading_order(vol.lines(side), SG_HAND_COLUMN_ORDER_V5)[1])


if __name__ == "__main__":
    sys.exit(main())
