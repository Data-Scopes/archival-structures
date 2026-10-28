"""Step 19e: copy of step-15w/audit.py plus the v5 checks (`audit_v5`, run on a sub-layer named *_v5).
Original docstring: Step 15w audit of a text layer: the phase-09 invariants (step 15i/15m) without fixed inventory lists,
plus the v4 volume-mode invariants for a sub-layer that has `mode/`.

    .venv/bin/python -I audit.py --layer data/text_layer_v2_dev/15w-a [--out audit.json]
    .venv/bin/python -I audit.py --self-test      # builds the 9999 fixture (v2 and v4) in a tmp dir and audits it

Discovery: sub-layers are `<layer>/*/NL-HaNA_*_text_layer_v*`; the inventories of a sub-layer are the
ones with a `done` row for every stage seen in its `status.tsv`; sources are found from
`manifest.json` `stages.extract.argv` (`--pagexml-dir`, links resolved, inventories matched with the
preset's `source_pattern`). An unreachable source skips its source-dependent invariant (noted).
The share of merges with a lower-case right side is *reported*: it stays in the list (`reported: true`,
pass/fail computed as in 15m, so 15m-a reproduces its 4 known failures) and is left out of
`n_fail_judged`.
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

from archival_structures.text_layer.io import discover_inventories_v2, read_tsv_gz
from archival_structures.text_layer.presets import PRESETS

HERE = Path(__file__).parent
SH = Path("/Users/Shared/claude_projects/data_scopes")
LOTA = SH / "language-of-the-archive"
PILOT = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281 455 456".split()
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
MODES = {"hand", "print"}
METHODS = {"box_height", "default", "fixed", "pinned"}   # step 15y: pinned


class Audit:
    def __init__(self):
        self.items = []

    def add(self, name, expected, actual, ok, note="", reported=False):
        self.items.append({"invariant": name, "expected": expected, "actual": actual, "pass": bool(ok),
                           "note": note, "reported": reported})


def sub_layers(base):
    """All `<base>/*/NL-HaNA_*_text_layer_v*` roots (sorted), or `<base>/NL-HaNA_*` when base is one level up."""
    found = sorted(Path(base).glob("*/NL-HaNA_*_text_layer_v*"))
    return found or sorted(Path(base).glob("NL-HaNA_*_text_layer_v*"))


def done_inventories(root):
    """Inventories whose *last* row of every stage seen in the sub-layer's status.tsv is `done`, in file order
    (step 15yr: a stage that was `done` once and failed since does not count). An inventory must have a last
    row for every stage seen in the sub-layer (15yr review: a volume with only an extract row is not built)."""
    last, order = {}, {}
    for row in (root / "status.tsv").read_text().splitlines()[1:]:
        cols = row.split("\t")
        if len(cols) < 3:
            continue
        order.setdefault(cols[0], None)
        last[(cols[0], cols[1])] = cols[2]
    stages = collections.defaultdict(dict)
    for (inv, stage), st in last.items():
        stages[inv][stage] = st
    seen = {stage for _, stage in last}
    return sorted((i for i in order if set(stages[i]) == seen and all(st == "done" for st in stages[i].values())),
                  key=lambda s: (len(s.rstrip("ABC")), s))


def archive_of(root):
    """The archive id of a sub-layer: `NL-HaNA_<archive>_text_layer_v<N>` -> <archive>, else the parent dir's name."""
    m = re.fullmatch(r"NL-HaNA_(.+)_text_layer_v\d+", Path(root).name)
    return m.group(1) if m else Path(root).parent.name


def find_sources(root):
    """{inv: source path} from the manifest's extract argv; {} when the dir or the manifest is not there."""
    try:
        man = json.loads((root / "manifest.json").read_text())
        ext = man["stages"]["extract"]
        argv = ext["argv"]
        preset = ext["preset"]
        pdir = Path(argv[argv.index("--pagexml-dir") + 1])
        pattern = PRESETS[preset].source_pattern
        return {inv: Path(p).resolve() for inv, p in discover_inventories_v2(pdir, pattern).items()}
    except (OSError, KeyError, ValueError, IndexError):
        return {}


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


def audit_layer(root, invs, sources, A, archive=""):
    """Phase-09 invariants over one layer root; returns {inv: counts} (R6)."""
    counts = {}
    sources = sources or {}
    tabs = {t: invs for t in TABLES}
    bad, nheaders = raw_checks(root, tabs)
    A.add(f"{root.name}: utf8, no CR, tab count = header's", [], bad[:20], not bad, f"{len(bad)} files")
    A.add(f"{root.name}: one header per table across inventories", {t: 1 for t in TABLES}, nheaders,
          all(v == 1 for v in nheaders.values()))
    for inv in invs:
        key = f"{archive}/{inv}"
        c = counts[key] = {}
        scans = read(root, "scans", inv)
        meta = read(root, "regions_meta", inv)
        lines = read(root, "lines", inv)
        lreg = read(root, "layout/regions", inv)
        units = read(root, "units/units", inv)
        dmap = read(root, "units/doc_id_map", inv)
        jun = {(r["scan_id"], r["unit_id"]): r for r in read(root, "join/units", inv)}
        jseg = read(root, "join/segments", inv)
        tseg = read(root, "types/segments", inv)
        tag = f"[{key}]"
        if inv in sources:
            try:
                xml = source_xml_names(sources[inv])
            except OSError:
                xml = None
            if xml is not None:
                have = {Path(r["member"]).name for r in scans}
                A.add(f"{tag} every XML member has a scan row", 0, len(xml - have), xml <= have,
                      f"{len(xml)} members, {len(scans)} scan rows")
        units_by_scan = collections.Counter(u["scan_id"] for u in units)
        no_unit = [r["scan_id"] for r in scans if r["status"] == "ok" and int(r["n_lines_text"]) > 0 and units_by_scan[r["scan_id"]] == 0]
        A.add(f"{tag} every scan with text lines has >= 1 unit", 0, len(no_unit), not no_unit, ",".join(no_unit[:5]))
        old_ids = {r["old_doc_id"] for r in dmap}
        miss = [r["region_id"] for r in meta if r["region_id"] not in old_ids]
        A.add(f"{tag} every regions_meta row is in doc_id_map", 0, len(miss), not miss, ",".join(miss[:5]))
        meta_ids = {r["region_id"] for r in meta}
        bad_same = [r["layout_region_id"] for r in lreg
                    if r["change"] == "unchanged" and not (r["source_region_ids"] == r["layout_region_id"]
                                                          and r["layout_region_id"] in meta_ids)]
        A.add(f"{tag} unchanged layout regions keep their id", 0, len(bad_same), not bad_same, ",".join(bad_same[:5]))
        uid = collections.Counter(u["unit_id"] for u in units)
        dup = [k for k, v in uid.items() if v > 1]
        A.add(f"{tag} unit_id unique", 0, len(dup), not dup, ",".join(dup[:5]))
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
        n_by_unit = {(u["scan_id"], u["unit_id"]): int(u["n_segments"]) for u in units}
        bad_n = [k for k, v in n_by_unit.items() if nseg[k] != v]
        A.add(f"{tag} n_segments equals join/segments rows", 0, len(bad_n), not bad_n, ",".join(k[1] for k in bad_n[:5]))
        ltext = {(r["scan_id"], r["line_id"]): r["text"] for r in lines}
        ubox = {(u["scan_id"], u["unit_id"]): tuple(int(u[k]) for k in "xywh") for u in units}
        sbox = {(r["scan_id"], r["line_id"], r["seg_idx"]): tuple(int(r[k]) for k in "xywh") for r in tseg}
        n_off = n_box = n_nojoin = 0
        ex_off, ex_box = [], []
        merges = collections.Counter()
        for s in jseg:
            ut = jun.get((s["scan_id"], s["unit_id"]))
            if ut is None:
                n_nojoin += 1
                continue
            cs, ce, lo = int(s["char_start"]), int(s["char_end"]), int(s["line_offset"])
            if ut["text"][cs:ce] != ltext[(s["scan_id"], s["line_id"])][lo:lo + ce - cs]:
                n_off += 1
                ex_off.append(s["unit_id"])
            sb, ub = sbox[(s["scan_id"], s["line_id"], s["seg_idx"])], ubox[(s["scan_id"], s["unit_id"])]
            if (sb[0] < ub[0] - INSIDE_PX or sb[1] < ub[1] - INSIDE_PX or
                    sb[0] + sb[2] > ub[0] + ub[2] + INSIDE_PX or sb[1] + sb[3] > ub[1] + ub[3] + INSIDE_PX):
                n_box += 1
                ex_box.append(s["unit_id"])
        A.add(f"{tag} offset invariant (unit text span = line text slice)", 0, n_off, n_off == 0, ",".join(ex_off[:5]))
        A.add(f"{tag} segment box inside unit box ({INSIDE_PX} px)", 0, n_box, n_box == 0, ",".join(ex_box[:5]))
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
              c["merge_lower_share"], share is None or share >= 0.95, f"{merges['total']} merges", reported=True)
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


def audit_v4(root, invs, A, archive=""):
    """The v4 volume-mode invariants of one sub-layer (call only when `root/mode` exists).
    Returns {inv: (mode, method)}."""
    modes = {}
    name = root.name
    for pdir in ("layout", "types", "units"):
        p = root / pdir / "params.json"
        keys = sorted(json.loads(p.read_text())) if p.is_file() else None
        A.add(f"{name}: {pdir}/params.json has the keys hand and print", True, keys,
              keys is not None and {"hand", "print"} <= set(keys))
    for inv in invs:
        key = f"{archive}/{inv}"
        tag = f"[{key}]"
        mp = root / "mode" / f"{inv}.json"
        if not mp.is_file():
            A.add(f"{tag} mode/<inv>.json exists", True, False, False)
            continue
        m = json.loads(mp.read_text())
        mode, method = m.get("mode"), m.get("method")
        modes[key] = (mode, method)
        A.add(f"{tag} mode in {{hand, print}}, method in {{box_height, default, fixed, pinned}}", "valid",
              [mode, method], mode in MODES and method in METHODS)
        if method == "box_height":
            cut, value = m["params"]["cut"], m["value"]
            A.add(f"{tag} box_height: print iff value <= cut", "print" if value <= cut else "hand",
                  mode, mode == ("print" if value <= cut else "hand"), f"value {value} cut {cut}")
        if method == "pinned":   # step 15y: the measured result stays recorded; the pin sets the mode
            A.add(f"{tag} pinned: box_height_mode recorded", "hand|print", m.get("box_height_mode"),
                  m.get("box_height_mode") in MODES, f"measured {m.get('box_height_mode')} -> pin {mode}")
        n_exp = int(m["n_scans"]) - int(m["n_parse_fail"])
        sp = root / "mode" / "scans" / f"{inv}.tsv.gz"
        n_rows = sum(1 for _ in read_tsv_gz(sp)) if sp.is_file() else None
        A.add(f"{tag} mode/scans rows = n_scans - n_parse_fail", n_exp, n_rows, n_rows == n_exp)
        A.add(f"{tag} mode/regions exists", True, (root / "mode" / "regions" / f"{inv}.tsv.gz").is_file(),
              (root / "mode" / "regions" / f"{inv}.tsv.gz").is_file())
        has_model = (root / "split_model" / f"{inv}.json").is_file()
        sg = m.get("split_gate")
        split_on = mode == "print" and (sg is None or sg.get("split") is not False)
        A.add(f"{tag} split_model exists iff mode print and split_gate.split is not False", split_on, has_model,
              has_model == split_on)
        if sg is not None:
            exp = sg["share"] is None or sg["share"] >= sg["min_share"]
            A.add(f"{tag} split_gate.split == (share is None or share >= min_share)", exp, sg["split"],
                  sg["split"] == exp, f"share {sg['share']} n_rows {sg['n_rows']}")
        if mode == "hand" or (sg is not None and sg.get("split") is False):
            n_split = sum(1 for r in read(root, "lines", inv) if r.get("split_from"))
            A.add(f"{tag} no split_from in a hand or gated inventory", 0, n_split, n_split == 0)
        methods = collections.Counter(r["unit_method"].split(":")[0] for r in read(root, "units/units", inv))
        wrong = {k: v for k, v in methods.items() if mode in MODES and k != mode}
        A.add(f"{tag} unit methods all '{mode}:*'", {}, wrong, not wrong)
    return modes


def lota_keys(A, doc_maps, invs_present):
    import pandas as pd
    g = pd.read_csv(LOTA / "gold/02d-hand-print-differences-partial-extended-rescaled.tsv", sep="\t",
                    usecols=["unit_id"], dtype=str)
    d = pd.read_csv(LOTA / "output/parallel_text/1.10.94-1.01.02/difference_units.tsv.gz", sep="\t",
                    usecols=["unit_id", "hand_doc_id"], dtype=str).drop_duplicates("unit_id")
    m = g.merge(d, on="unit_id", how="left")
    inv_of = m.hand_doc_id.str.extract(r"^NL-HaNA_[\d.]+_(\d+[A-Z]?)_", expand=False)
    scope = m[inv_of.isin(invs_present) | m.hand_doc_id.isna()]
    found = sum(1 for x in scope.hand_doc_id if isinstance(x, str) and x in doc_maps)
    A.add("LotA keys: 02d hand_doc_ids found in doc_id_map", len(scope), found, found == len(scope),
          f"{len(scope)} of {len(m)} rows in scope, {m.hand_doc_id.isna().sum()} without hand_doc_id")
    pilot = set(PILOT) & set(invs_present)
    n = {}
    for name in ("02-region-quality-done", "02c-short-region-quality-done"):
        t = pd.read_csv(LOTA / f"gold/{name}.tsv", sep="\t", usecols=["region_id", "inv_num"], dtype=str)
        n[name] = int(t.inv_num.isin(pilot).sum())
    A.add("LotA gold 02-/02c- region ids in pilot inventories (count)", {k: 0 for k in n}, n,
          all(v == 0 for v in n.values()), "planning: those files cover other inventories")


def audit_v5(root, invs, A, archive=""):
    """Step 19e: the v5 invariants of one sub-layer. Reads mode/<inv>.json for hand or print.
    Returns {"hand": n, "print": n, "flagged_sides": n, "index_units_on_flagged": n}."""
    name = root.name
    n = collections.Counter()
    bad_pages, bad_dup, bad_method, bad_index = [], [], [], []
    for inv in invs:
        mode = json.loads((root / "mode" / f"{inv}.json").read_text()).get("mode")
        n[mode] += 1
        pp = root / "layout" / "pages" / f"{inv}.tsv.gz"
        if (mode == "hand") != pp.is_file():
            bad_pages.append(f"{inv}:{mode}:{pp.is_file()}")
        seen = set()
        for r in read(root, "layout/regions", inv):
            k = (r["scan_id"], r["layout_region_id"])
            if k in seen:
                bad_dup.append(f"{inv}:{k}")
            seen.add(k)
        flagged = set()
        if pp.is_file():
            flagged = {(r["scan_id"], r["page"]) for r in read_tsv_gz(pp) if r["index_side"] == "1"}
            n["flagged_sides"] += len(flagged)
        for u in read(root, "units/units", inv):
            on = (u["scan_id"], u["page"]) in flagged
            if u["unit_method"].startswith("hand:index") and not on:
                bad_method.append(f"{inv}:{u['unit_id']}")
            if u["unit_type"] == "index" and on:
                bad_index.append(f"{inv}:{u['unit_id']}")
            if u["unit_type"] == "index" and on:
                n["index_units_on_flagged"] += 1
    A.add(f"{name}: hand inventories have layout/pages, print inventories have none", "none wrong",
          bad_pages[:5], not bad_pages)
    A.add(f"{name}: no duplicate layout_region_id within a scan", "none", bad_dup[:5], not bad_dup)
    A.add(f"{name}: every hand:index* unit_method sits on a side with index_side == 1", "none", bad_method[:5],
          not bad_method)
    A.add(f"{name}: no unit of type index on a flagged side", "none", bad_index[:5], not bad_index)
    return dict(n)


def audit_base(base, A):
    """Audit every sub-layer under `base`; returns (counts, modes, manifests, doc_maps, inventories)."""
    counts, modes, doc_maps, all_invs, sub_info = {}, {}, set(), [], {}
    for root in sub_layers(base):
        invs = done_inventories(root)
        sources = find_sources(root)
        reachable = {i: p for i, p in sources.items() if i in invs and p.exists()}
        archive = archive_of(root)
        counts.update(audit_layer(root, invs, reachable, A, archive))
        if (root / "mode").is_dir():
            modes.update(audit_v4(root, invs, A, archive))
            if re.search(r"_v5$", root.name):
                counts[f"{archive}/v5"] = audit_v5(root, invs, A, archive)
        for inv in invs:
            doc_maps.update(r["old_doc_id"] for r in read(root, "units/doc_id_map", inv))
        all_invs += invs
        sub_info[str(root)] = {"inventories": len(invs), "v4": (root / "mode").is_dir(),
                             "sources_reachable": len(reachable)}
    return counts, modes, doc_maps, all_invs, sub_info


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", default="data/text_layer_v2_dev/15w-a")
    ap.add_argument("--out", default=str(HERE / "audit.json"))
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    A = Audit()
    base = Path(args.layer)
    counts, modes, doc_maps, invs, sub_info = audit_base(base, A)
    if set(PILOT) & set(invs):
        lota_keys(A, doc_maps, invs)
    out = {"layer": str(base), "sub_layers": sub_info, "invariants": A.items,
           "n_pass": sum(i["pass"] for i in A.items), "n_fail": sum(not i["pass"] for i in A.items),
           "n_fail_judged": sum(not i["pass"] and not i["reported"] for i in A.items),
           "n_inventories": len(invs), "modes": {k: list(v) for k, v in modes.items()}, "counts": counts}
    Path(args.out).write_text(json.dumps(out, indent=1, ensure_ascii=False))
    for i in A.items:
        if not i["pass"]:
            print("FAIL" + (" (reported)" if i["reported"] else ""), i["invariant"], i["expected"], i["actual"], i["note"])
    print(out["n_pass"], "pass,", out["n_fail"], "fail,", out["n_fail_judged"], "judged ->", args.out)
    return 0


def self_test():
    import shutil
    from archival_structures.text_layer import build
    fx = Path(__file__).resolve().parents[4] / "tests/fixtures/text_layer/sg/9999"
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        shutil.copytree(fx, td / "px" / "NL-HaNA_1.01.02_9999")
        import dataclasses
        from archival_structures.text_layer.presets import PRESETS
        orig = PRESETS["sg_1.01.02_v4"]
        pinned = dataclasses.replace(orig, volume_mode={**orig.volume_mode, "pins": {"9999": "print"}})
        try:
            for tag, preset in (("v2", "sg_1.01.02_v2"), ("v4", "sg_1.01.02_v4"), ("v4pin", "sg_1.01.02_v4")):
                PRESETS["sg_1.01.02_v4"] = pinned if tag == "v4pin" else orig
                rc = build.main(["--pagexml-dir", str(td / "px"), "--out-dir", str(td / tag / "x"), "--preset", preset,
                                 "--stage", "all", "--workers", "1"])
                assert rc == 0
                A = Audit()
                counts, modes, *_ = audit_base(td / tag, A)
                fails = [i for i in A.items if not i["pass"] and not i["reported"]]
                print(tag, len(A.items), "invariants,", len(fails), "failing:", [(i["invariant"], i["actual"]) for i in fails])
                assert not fails and counts["1.01.02/9999"]["scans"] > 0
                assert (tag != "v2") == bool(modes), modes
                if tag == "v4pin":
                    assert modes["1.01.02/9999"] == ("print", "pinned"), modes
                if tag != "v2":
                    assert sum("mode" in i["invariant"] or "split" in i["invariant"] for i in A.items) >= 5
            # step 15yr: two archives with the same inventory number keep separate keys
            src = td / "v4" / "x" / "NL-HaNA_1.01.02_text_layer_v4"
            for name in ("a/NL-HaNA_1.01.02_text_layer_v4", "b/NL-HaNA_1.10.94_text_layer_v4"):
                shutil.copytree(src, td / "two" / name)
            A = Audit()
            counts, modes, *_ = audit_base(td / "two", A)
            fails = [i for i in A.items if not i["pass"] and not i["reported"]]
            assert not fails, fails
            assert {"1.01.02/9999", "1.10.94/9999"} <= set(modes) and set(modes) == set(counts), (modes, counts)
            print("two", len(A.items), "invariants,", len(fails), "failing; keys", sorted(modes))
            # a stage that failed after a done row is not done
            shutil.copytree(src, td / "fail" / "x" / "NL-HaNA_1.01.02_text_layer_v4")
            with open(td / "fail" / "x" / "NL-HaNA_1.01.02_text_layer_v4" / "status.tsv", "a") as fh:
                fh.write("\t".join(["9999", "extract", "failed"] + [""] * 9) + "\n")
            A = Audit()
            counts, modes, *_ = audit_base(td / "fail", A)
            assert counts == {} and modes == {}, (counts, modes)
            print("fail: counts empty")
            # 15yr review: an inventory with an extract row only (a run that stopped after extract) is not built
            part = td / "fail" / "x" / "NL-HaNA_1.01.02_text_layer_v4" / "status.tsv"
            part.write_text(part.read_text().splitlines()[0] + "\n" + "\t".join(["9998", "extract", "done"] + [""] * 9)
                            + "\n" + "\t".join(["9999", "layout", "done"] + [""] * 9) + "\n")
            assert done_inventories(part.parent) == [], done_inventories(part.parent)
            print("partial: not built")
        finally:
            PRESETS["sg_1.01.02_v4"] = orig
    print("self-test ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
