"""Step 15i audit of the pilot layer: invariants (each with its expected value) and R6 manifest counts.

    .venv/bin/python audit.py [--layer data/text_layer_v2_dev/15m-a] [--out audit.json]
    .venv/bin/python audit.py --self-test      # builds the 15hj fixture layer in a tmp dir and audits it
"""
import argparse
import collections
import gzip
import json
import re
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

HERE = Path(__file__).parent
SH = Path("/Users/Shared/claude_projects/data_scopes")
LOTA = SH / "language-of-the-archive"
HAND = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
PRINT = ["455", "456"]
TYPES9 = {"main_text", "page_number", "running_header", "date_header", "catch_word", "signature_mark",
          "marginalia", "index", "other"}
TABLES = {  # table -> file name pattern
    "scans": "scans/{inv}.scans.tsv.gz", "regions_meta": "regions_meta/{inv}.tsv.gz",
    "lines": "lines/{inv}.lines.tsv.gz", "layout/scans": "layout/scans/{inv}.tsv.gz",
    "layout/regions": "layout/regions/{inv}.tsv.gz", "layout/lines": "layout/lines/{inv}.tsv.gz",
    "types/segments": "types/segments/{inv}.tsv.gz", "types/regions": "types/regions/{inv}.tsv.gz",
    "units/units": "units/units/{inv}.tsv.gz", "units/segments": "units/segments/{inv}.tsv.gz",
    "units/doc_id_map": "units/doc_id_map/{inv}.tsv.gz", "join/units": "join/units/{inv}.tsv.gz",
    "join/segments": "join/segments/{inv}.tsv.gz"}
CHANGE_RE = re.compile(r"unchanged|(merged|split_at_gutter|split_at_column)(\+(merged|split_at_gutter|split_at_column))*")
INSIDE_PX = 5


class Audit:
    def __init__(self):
        self.items = []

    def add(self, name, expected, actual, ok, note=""):
        self.items.append({"invariant": name, "expected": expected, "actual": actual, "pass": bool(ok), "note": note})


def layer_root(base):
    return next(Path(base).glob("*_text_layer_v*"))


def read(root, table, inv):
    return list(read_tsv_gz(root / TABLES[table].format(inv=inv)))


def source_xml_names(src):
    src = Path(src)
    if src.is_dir():
        names = [p.name for p in src.iterdir()]
    elif src.name.endswith((".tar.gz", ".tgz", ".tar")):
        with tarfile.open(src) as tf:
            names = [Path(m.name).name for m in tf.getmembers() if m.isfile()]
    else:
        with zipfile.ZipFile(src) as zf:
            names = [Path(n).name for n in zf.namelist() if not n.endswith("/")]
    return {n for n in names if n.endswith(".xml") and not n.startswith("._")}


def raw_checks(root, inv_by_table):
    """Per file: valid UTF-8, no CR, every row has the header's number of tabs. Header per table."""
    bad, headers = [], collections.defaultdict(set)
    for table, invs in inv_by_table.items():
        for inv in invs:
            p = root / TABLES[table].format(inv=inv)
            raw = gzip.open(p, "rb").read()
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                bad.append(f"{p.name}:utf8")
                continue
            if "\r" in text:
                bad.append(f"{p.name}:CR")
            lines = text.split("\n")
            if lines and lines[-1] == "":
                lines.pop()
            ncol = lines[0].count("\t")
            headers[table].add(lines[0])
            n_bad = sum(1 for ln in lines if ln.count("\t") != ncol)
            if n_bad:
                bad.append(f"{p.name}:tabs:{n_bad}")
    return bad, {t: len(h) for t, h in headers.items()}


def audit_layer(root, invs, sources, A, wbc_chars=None):
    """Invariants over one layer root; returns {inv: counts} (R6)."""
    counts = {}
    sources = sources or {}
    tabs = {t: invs for t in TABLES}
    bad, nheaders = raw_checks(root, tabs)
    A.add(f"{root.name}: utf8, no CR, tab count = header's", [], bad, not bad)
    A.add(f"{root.name}: one header per table across inventories", {t: 1 for t in TABLES}, nheaders,
          all(v == 1 for v in nheaders.values()))
    for inv in invs:
        c = counts[inv] = {}
        scans = read(root, "scans", inv)
        meta = read(root, "regions_meta", inv)
        lines = read(root, "lines", inv)
        lreg = read(root, "layout/regions", inv)
        units = read(root, "units/units", inv)
        dmap = read(root, "units/doc_id_map", inv)
        jun = {(r["scan_id"], r["unit_id"]): r for r in read(root, "join/units", inv)}
        jseg = read(root, "join/segments", inv)
        tseg = read(root, "types/segments", inv)
        tag = f"[{inv}]"
        if inv in sources:
            xml = source_xml_names(sources[inv])
            have = {Path(r["member"]).name for r in scans}
            A.add(f"{tag} every XML member has a scan row", 0, len(xml - have), xml <= have,
                  f"{len(xml)} members, {len(scans)} scan rows")
        units_by_scan = collections.Counter(u["scan_id"] for u in units)
        no_unit = [r["scan_id"] for r in scans if r["status"] == "ok" and int(r["n_lines_text"]) > 0 and units_by_scan[r["scan_id"]] == 0]
        A.add(f"{tag} every scan with text lines has >= 1 unit", 0, len(no_unit), not no_unit)
        old_ids = {r["old_doc_id"] for r in dmap}
        miss = [r["region_id"] for r in meta if r["region_id"] not in old_ids]
        A.add(f"{tag} every regions_meta row is in doc_id_map", 0, len(miss), not miss)
        meta_ids = {r["region_id"] for r in meta}
        bad_same = [r["layout_region_id"] for r in lreg
                    if r["change"] == "unchanged" and not (r["source_region_ids"] == r["layout_region_id"]
                                                          and r["layout_region_id"] in meta_ids)]
        A.add(f"{tag} unchanged layout regions keep their id", 0, len(bad_same), not bad_same)
        uid = collections.Counter(u["unit_id"] for u in units)
        dup = [k for k, v in uid.items() if v > 1]
        A.add(f"{tag} unit_id unique", 0, len(dup), not dup)
        # value sets
        sets = {
            "layout/regions.page": {r["page"] for r in lreg} - {"verso", "recto", "single"},
            "layout/regions.layout_role": {r["layout_role"] for r in lreg} - {"main", "extra", "empty"},
            "layout/regions.change": {r["change"] for r in lreg if r["change"] and not CHANGE_RE.fullmatch(r["change"])},
            "units.page": {r["page"] for r in units} - {"verso", "recto", "single"},
            "units.unit_type": {r["unit_type"] for r in units} - TYPES9,
            "doc_id_map.old_status": {r["old_status"] for r in dmap} - {"identical", "trimmed", "changed", "empty"},
            "doc_id_map.link": {r["link"] for r in dmap} - {"same_id", "part", "none"},
            "doc_id_map.none_only_on_empty": {r["old_doc_id"] for r in dmap
                                             if r["link"] == "none" and not (r["old_status"] == "empty" and r["unit_id"] == "")},
        }
        A.add(f"{tag} values lie in their sets", {k: [] for k in sets}, {k: sorted(v)[:5] for k, v in sets.items()},
              all(not v for v in sets.values()))
        # n_segments and offsets
        nseg = collections.Counter((r["scan_id"], r["unit_id"]) for r in jseg)
        bad_n = [k for k, u in jun.items() if nseg[k] != next(
            int(x["n_segments"]) for x in units if x["scan_id"] == k[0] and x["unit_id"] == k[1])] if False else []
        n_by_unit = {(u["scan_id"], u["unit_id"]): int(u["n_segments"]) for u in units}
        bad_n = [k for k, v in n_by_unit.items() if nseg[k] != v]
        A.add(f"{tag} n_segments equals join/segments rows", 0, len(bad_n), not bad_n)
        ltext = {(r["scan_id"], r["line_id"]): r["text"] for r in lines}
        ubox = {(u["scan_id"], u["unit_id"]): tuple(int(u[k]) for k in "xywh") for u in units}
        sbox = {(r["scan_id"], r["line_id"], r["seg_idx"]): tuple(int(r[k]) for k in "xywh") for r in tseg}
        n_off = n_box = n_nojoin = 0
        merges = collections.Counter()
        for s in jseg:
            ut = jun.get((s["scan_id"], s["unit_id"]))
            if ut is None:
                n_nojoin += 1
                continue
            cs, ce, lo = int(s["char_start"]), int(s["char_end"]), int(s["line_offset"])
            if ut["text"][cs:ce] != ltext[(s["scan_id"], s["line_id"])][lo:lo + ce - cs]:
                n_off += 1
            sb, ub = sbox[(s["scan_id"], s["line_id"], s["seg_idx"])], ubox[(s["scan_id"], s["unit_id"])]
            if (sb[0] < ub[0] - INSIDE_PX or sb[1] < ub[1] - INSIDE_PX or
                    sb[0] + sb[2] > ub[0] + ub[2] + INSIDE_PX or sb[1] + sb[3] > ub[1] + ub[3] + INSIDE_PX):
                n_box += 1
        A.add(f"{tag} offset invariant (unit text span = line text slice)", 0, n_off, n_off == 0)
        A.add(f"{tag} segment box inside unit box ({INSIDE_PX} px)", 0, n_box, n_box == 0)
        A.add(f"{tag} every join segment has a join unit", 0, n_nojoin, n_nojoin == 0)
        # merges with lower-case right side
        by_unit = collections.defaultdict(list)
        for s in jseg:
            by_unit[(s["scan_id"], s["unit_id"])].append(s)
        wbc = next(iter(jun.values()))["word_break_chars"] if jun else ""
        for k, segs in by_unit.items():
            segs.sort(key=lambda s: int(s["seg_order"]))
            text = jun[k]["text"]
            for a, b in zip(segs, segs[1:]):
                if a["boundary_kind"] == "merge":
                    ch = text[int(b["char_start"]):].lstrip(wbc)[:1]
                    merges["total"] += 1
                    merges["lower"] += ch.islower()
                    merges["upper_or_digit"] += ch.isupper() or ch.isdigit()
        share = merges["lower"] / merges["total"] if merges["total"] else None
        c["merges"] = dict(merges)
        c["merge_lower_share"] = None if share is None else round(share, 4)
        A.add(f"{tag} merges with lower-case right side (share, target >= 0.95, reported)", ">= 0.95",
              c["merge_lower_share"], share is None or share >= 0.95, f"{merges['total']} merges")
        # R6 counts
        sc = collections.Counter(r["status"] for r in scans)
        c.update({
            "scans": len(scans), "scan_status": dict(sc),
            "scans_without_text": len(scans) - sc["ok"],
            "lines": len(lines), "lines_with_text": sum(1 for r in lines if r["text"].strip()),
            "legacy_regions": len(meta), "legacy_regions_without_lines": sum(1 for r in meta if int(r["n_lines"]) == 0),
            "layout_regions": sum(1 for r in lreg if r["layout_role"] != "empty"),
            "layout_regions_empty": sum(1 for r in lreg if r["layout_role"] == "empty"),
            "units": len(units), "units_per_type": dict(collections.Counter(u["unit_type"] for u in units)),
            "old_status": dict(collections.Counter({r["old_doc_id"]: r["old_status"] for r in dmap}.values())),
            "whitespace_repairs": sum(1 for r in lines if "wordfix" in r.get("edit_flags", "") or "glue" in r.get("edit_flags", "")),
            "segments": len(tseg), "word_break_chars": wbc})
        gr = {r["gutter_rule"] for r in read(root, "layout/scans", inv)}
        c["gutter_rule"] = sorted(gr)
        ss = root / "split_stats" / f"{inv}.json"
        if ss.exists():
            j = json.loads(ss.read_text())
            c["split_lines"] = j["n_split_bigram"] + j["n_split_hyphen"]
            c["split_stats"] = {k: j[k] for k in ("n_split_bigram", "n_split_hyphen", "n_degenerate_splits",
                                                  "n_alignment_failures") if k in j}
        js = json.loads((root / "join" / "stats" / f"{inv}.json").read_text())
        c["join_offset_violations"] = js.get("offset_violations", js.get("n_offset_violations"))
        c["gaps_scans_without_text"] = dict(collections.Counter(r["status"] for r in scans if r["status"] != "ok"))
    return counts


def lota_keys(roots, A, doc_maps):
    import pandas as pd
    g = pd.read_csv(LOTA / "gold/02d-hand-print-differences-partial-extended-rescaled.tsv", sep="\t",
                    usecols=["unit_id"], dtype=str)
    d = pd.read_csv(LOTA / "output/parallel_text/1.10.94-1.01.02/difference_units.tsv.gz", sep="\t",
                    usecols=["unit_id", "hand_doc_id"], dtype=str).drop_duplicates("unit_id")
    m = g.merge(d, on="unit_id", how="left")
    found = sum(1 for x in m.hand_doc_id if isinstance(x, str) and x in doc_maps)
    A.add("LotA keys: 02d hand_doc_ids found in doc_id_map", len(m), found, found == len(m),
          f"{len(m)} rows, {m.hand_doc_id.isna().sum()} without hand_doc_id")
    invs = set(HAND + PRINT)
    n = {}
    for name in ("02-region-quality-done", "02c-short-region-quality-done"):
        t = pd.read_csv(LOTA / f"gold/{name}.tsv", sep="\t", usecols=["region_id", "inv_num"], dtype=str)
        n[name] = int(t.inv_num.isin(invs).sum())
    A.add("LotA gold 02-/02c- region ids in pilot inventories (count)", {k: 0 for k in n}, n,
          all(v == 0 for v in n.values()), "planning: those files cover other inventories")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", default="data/text_layer_v2_dev/15m-a")
    ap.add_argument("--out", default=str(HERE / "audit.json"))
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)
    A = Audit()
    if args.self_test:
        return self_test()
    base = Path(args.layer)
    hand, prn = layer_root(base / "hand"), layer_root(base / "print")
    src = Path("data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02")
    sources = {i: next(src.glob(f"NL-HaNA_1.01.02_{i}*")) for i in HAND}
    sources.update({i: SH / f"shared-data/NL-HaNA_1.10.94/pagexml/NL-HaNA_1.10.94_{i}.zip" for i in PRINT})
    counts = {}
    counts.update(audit_layer(hand, HAND, sources, A))
    counts.update(audit_layer(prn, PRINT, sources, A))
    doc_maps = set()
    for root, invs in ((hand, HAND), (prn, PRINT)):
        for inv in invs:
            doc_maps.update(r["old_doc_id"] for r in read(root, "units/doc_id_map", inv))
    lota_keys(None, A, doc_maps)
    manifest = {n: json.loads((r / "manifest.json").read_text()) for n, r in (("hand", hand), ("print", prn))}
    wbc = {n: json.loads((r / "wbc" / "corpus_default.json").read_text()) for n, r in (("hand", hand), ("print", prn))}
    out = {"layer": str(base), "invariants": A.items,
           "n_pass": sum(i["pass"] for i in A.items), "n_fail": sum(not i["pass"] for i in A.items),
           "counts": counts, "word_break_stat": {n: manifest[n].get("stages", {}).get("wbc", {}).get("word_break_stat",
                                                                                                    "default") for n in manifest},
           "corpus_default": {n: {k: v for k, v in w.items() if k in ("default_set", "pinned", "sanity_pass",
                                                                   "derived_default_set", "pooled_rule_sanity_pass")}
                              for n, w in wbc.items()}}
    Path(args.out).write_text(json.dumps(out, indent=1, ensure_ascii=False))
    for i in A.items:
        if not i["pass"]:
            print("FAIL", i["invariant"], i["expected"], i["actual"], i["note"])
    print(out["n_pass"], "pass,", out["n_fail"], "fail ->", args.out)
    return 0


def self_test():
    from archival_structures.text_layer import build
    fx = Path(__file__).resolve().parents[4] / "tests/fixtures/text_layer/sg/9999"
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        import shutil
        shutil.copytree(fx, td / "px" / "NL-HaNA_1.01.02_9999")
        rc = build.main(["--pagexml-dir", str(td / "px"), "--out-dir", str(td / "out"), "--preset", "sg_1.01.02_v2",
                         "--stage", "all", "--workers", "1"])
        assert rc == 0
        A = Audit()
        root = layer_root(td / "out")
        c = audit_layer(root, ["9999"], {"9999": td / "px" / "NL-HaNA_1.01.02_9999"}, A)
        fails = [i for i in A.items if not i["pass"] and "merges with lower" not in i["invariant"]]
        print(len(A.items), "invariants,", len(fails), "failing:", [(i["invariant"], i["actual"]) for i in fails])
        assert not fails and c["9999"]["scans"] > 0
    print("self-test ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
