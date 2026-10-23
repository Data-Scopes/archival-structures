"""
Build a shared text layer (default preset: voc_v1, the GLOBALISE VOC PageXML v2.1 layer).

Turns PageXML (Laypa + Loghi) into per-inventory region TSVs, plus a raw-lines sidecar and
per-inventory word-break characters. CPU only; no GPU. The logic lives in the other modules of
`archival_structures.text_layer`; this module is the resumable driver.

HANDOFF -- running the full build on another machine
----------------------------------------------------
Install, in a fresh venv (Python >= 3.11, which archival-structures requires):

    python -m venv .venv
    .venv/bin/python -m pip install -e /path/to/archival-structures   # pulls in pagexml-tools>=0.9.0

(archival-structures 0.8.0 on PyPI predates the text layer. Use a checkout until the next
release, then `pip install archival-structures`. The install creates the console script
`.venv/bin/as-build-text-layer`; call it by that path, or activate the venv.
`python -m archival_structures.text_layer.build` is the same driver. The driver asserts
pagexml-tools >= 0.9.0 and records the exact version in manifest.json, and for a git
checkout also its HEAD and dirty files, for both pagexml-tools and archival-structures.)

First run AS's `tests/test_text_layer_*.py` (from a checkout of the repository). A failure
is a finding to report back, not a test to relax:

    python -m pytest tests/test_text_layer_*.py

Then the three stages, in order (D = the directory holding <inv>.zip files or
<inv>/ directories, O = output directory, N = worker processes):

    as-build-text-layer --pagexml-dir D --out-dir O --stage extract --workers N
    as-build-text-layer --pagexml-dir D --out-dir O --stage wbc     --workers N
    as-build-text-layer --pagexml-dir D --out-dir O --stage join    --workers N

(or `python -m archival_structures.text_layer.build ...`; `--stage all` runs the three in
sequence. The layer directory is <O>/<preset name>/, by default O/voc_text_layer_v1/.) Each
stage is resumable: rerun the same command after a crash or kill and it skips the inventories
marked done in O/<preset name>/status.tsv. `wbc` refuses to run until `extract` is done for
every inventory in the run (override: --allow-partial, recorded in the manifest). Progress goes
to O/<preset name>/progress.log.

Expected runtime: run `--estimate --workers N` first; it sums the PageXML bytes
under D (zip central directories only, no parsing) and prints the predicted
wall time from the throughput fit in report/74 §6:

    seconds per inventory and worker = 0.44 + 0.105 x MB of uncompressed XML
    (extract 0.436 + 0.0912/MB, wbc 0.005 + 0.0038/MB, join 0.0098/MB)

The local fit predicted 0.5 min for the 22 annotated inventories on 8 workers
against 0.57 min measured. For ~6,914 inventories with the local size
distribution (~398 GB XML, ~116 GB of zips) that is ~12.4 CPU-hours: about
0.6 h wall on 20-22 workers at the laptop's per-worker speed (10.5 MB/s), so
budget 1-2 h on a slower CPU. Check the first minutes of `progress.log`
against it.

Expected output size: 0.023 x the uncompressed XML bytes for lines/ +
regions/ together (gzip), i.e. ~9 GB for the full holdings; regions_meta/,
wbc/, join_stats/ and extract_stats/ are small (< 1 GB together).

The `wbc` stage prints the pooled distribution and checks the sanity anchors
(`¬` in, `:` and `.` out, on pooled counts). If they fail, `join` refuses to
run: stop and send back wbc/ instead of passing --ignore-sanity. The corpus
default set will be re-derived from all inventories in the run; the local
{¬, „} from the 22 is provisional. It overwrites wbc/corpus_default.json and
wbc/wbc_per_inventory.tsv, so never point --out-dir at an existing layer with a subset of its
inventories.

Send back:
  * small, for review: O/<preset name>/{wbc/, manifest.json, status.tsv,
    join_stats/, extract_stats/, progress.log}
  * O/<preset name>/{regions/, lines/} to wherever the shared layer lives
    (the user decides). regions_meta/ is an intermediate and can stay behind.

v2 presets only: `--stage layout` (step 15fp) reads scans/, regions_meta/ and lines/ of an
existing layer and writes O/<preset name>/layout/ (page split, region merge, columns, roles; see
`archival_structures.text_layer.layout`). Like `--stage wordfix`, it needs no --pagexml-dir and is
not part of `--stage all`. Run it on a copy of the layer.

Smoke test (local, before anything larger):

    as-build-text-layer --pagexml-dir pagexml --out-dir output/smoke \
        --stage all --limit 3 --workers 1
"""

import argparse
import datetime as dt
import functools
import json
import platform
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from archival_structures.text_layer.extract import (
    LINES_COLUMNS, META_COLUMNS, ROW_ORDER_STATEMENT, extract_inventory)
from archival_structures.text_layer.io import (
    discover_inventories, discover_inventories_v2, input_bytes, inventory_sort_key, write_json_atomic, write_plain_tsv)
from archival_structures.text_layer.extract_v2 import (
    LINE_COLUMNS_V2, REGION_COLUMNS_V2, SCAN_COLUMNS_V2, extract_inventory_v2)
from archival_structures.text_layer.join import ALL_GUARDS, REGION_COLUMNS, join_inventory
from archival_structures.text_layer.join_v2 import (
    WORD_BREAK_STATS_COLUMNS_V2, join_inventory_v2, write_word_break_stats_part_v2)
from archival_structures.text_layer.presets import PRESETS, predict_seconds, predict_wall
from archival_structures.text_layer.provenance import (
    check_pagexml_tools_version, package_info, pagexml_tools_info)
from archival_structures.text_layer.word_breaks import (
    DEFAULT_WORD_BREAK_RULE, WORD_BREAK_STATS_COLUMNS, char_label, decide_word_breaks, format_stat,
    pooled_stats, read_word_break_sets, write_word_break_stats_part)

STAGES = ("extract", "wbc", "join")
STATUS_COLUMNS = ["inventory_number", "stage", "status", "n_rows", "n_lines", "n_scans",
                  "n_errors", "seconds", "input_bytes", "input_xml_bytes", "finished_utc", "error"]


def now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Run:
    def __init__(self, args, preset):
        self.args = args
        self.preset = preset
        self.force = args.force
        self.root = Path(args.out_dir) / preset.name
        self.root.mkdir(parents=True, exist_ok=True)
        self.status_path = self.root / "status.tsv"
        self.log_path = self.root / "progress.log"
        if not self.status_path.exists():
            self.status_path.write_text("\t".join(STATUS_COLUMNS) + "\n", encoding="utf-8")

    def log(self, message):
        line = f"{now()} {message}"
        print(line, flush=True)
        with open(self.log_path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def status(self):
        """Latest row per (inventory, stage)."""
        latest = {}
        with open(self.status_path, encoding="utf-8") as handle:
            header = handle.readline().rstrip("\n").split("\t")
            for line in handle:
                row = dict(zip(header, line.rstrip("\n").split("\t")))
                latest[(row["inventory_number"], row["stage"])] = row
        return latest

    def record(self, result, sizes):
        stored, xml = sizes.get(result["inventory_number"], ("", ""))
        row = {"input_bytes": stored, "input_xml_bytes": xml, "finished_utc": now(), **result}
        row["error"] = str(row.get("error", "")).replace("\t", " ").replace("\n", " | ")[:500]
        with open(self.status_path, "a", encoding="utf-8") as handle:
            handle.write("\t".join(str(row.get(c, "")) for c in STATUS_COLUMNS) + "\n")

    def update_manifest(self, stage, extra):
        path = self.root / "manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        info = package_info("archival-structures", "archival_structures")
        manifest.update({
            "layer": self.preset.name,
            "row_order": ROW_ORDER_STATEMENT,
            "columns": {"regions": REGION_COLUMNS, "lines": LINES_COLUMNS,
                        "regions_meta": META_COLUMNS},
            "format": "tab-separated, UTF-8, header row, csv.QUOTE_NONE; tabs/CR/LF inside text "
                      "sanitised to one space each (counts in extract_stats/ and join_stats/)",
            "untagged_region_tag": self.preset.untagged_tag,
            "format_version": self.preset.format_version,
        })
        if self.preset.format_version == 2:
            manifest["columns"] = {"scans": SCAN_COLUMNS_V2, "regions_meta": REGION_COLUMNS_V2,
                                   "lines": LINE_COLUMNS_V2}
            manifest["row_order"] = ("Rows are in PageXML document order (scans sorted by file "
                                     "name, regions depth-first, lines in XML order); reading-order "
                                     "indices are columns.")
        previous = manifest.get("stages", {}).get(stage)
        if previous is not None:
            manifest.setdefault("history", {}).setdefault(stage, []).append(previous)
        manifest.setdefault("stages", {})[stage] = {
            "finished_utc": now(),
            "git_head": info.get("git_head", ""),
            "archival_structures": info,
            "preset": self.args.preset,
            "pagexml_tools": pagexml_tools_info(),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "argv": sys.argv[1:],
            "params": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(self.args).items()},
            **extra,
        }
        write_json_atomic(path, manifest)


def select_inventories(args):
    preset = PRESETS[args.preset]
    if preset.format_version == 2:
        available = discover_inventories_v2(args.pagexml_dir, preset.source_pattern)
    else:
        available = discover_inventories(args.pagexml_dir)
    wanted = None
    if args.inventories:
        wanted = [s.strip() for s in args.inventories.split(",") if s.strip()]
    elif args.inventories_file:
        wanted = [s.strip() for s in Path(args.inventories_file).read_text().split() if s.strip()]
    if wanted is not None:
        missing = [inv for inv in wanted if inv not in available]
        if missing:
            sys.exit(f"inventories not under {args.pagexml_dir}: {missing}")
        selected = {inv: available[inv] for inv in sorted(wanted, key=inventory_sort_key)}
    else:
        selected = available
    if args.limit:
        selected = dict(list(selected.items())[:args.limit])
    return selected


def run_pool(run, func, tasks, sizes, label):
    """tasks: [(inv, args_tuple)], longest first. Records every result."""
    failures = []
    total = len(tasks)
    if total == 0:
        run.log(f"[{label}] nothing to do")
        return failures
    run.log(f"[{label}] {total} inventories, {run.args.workers} workers")
    started = time.monotonic()

    def handle(inv, result=None, exc=None, done=0):
        if exc is not None:
            failures.append(inv)
            run.record({"inventory_number": inv, "stage": label, "status": "failed",
                        "error": "".join(traceback.format_exception_only(type(exc), exc))}, sizes)
            run.log(f"[{label}] FAILED {inv}: {exc!r}")
        else:
            run.record(result, sizes)
            elapsed = time.monotonic() - started
            run.log(f"[{label}] {done}/{total} {inv} rows={result.get('n_rows')} "
                    f"{result['seconds']:.1f}s elapsed={elapsed / 60:.1f}min")

    if run.args.workers <= 1:
        for i, (inv, fargs) in enumerate(tasks, 1):
            try:
                handle(inv, func(*fargs), done=i)
            except Exception as exc:  # noqa: BLE001 -- record and continue
                handle(inv, exc=exc)
        return failures
    with ProcessPoolExecutor(max_workers=run.args.workers) as pool:
        futures = {pool.submit(func, *fargs): inv for inv, fargs in tasks}
        for i, future in enumerate(as_completed(futures), 1):
            inv = futures[future]
            try:
                handle(inv, future.result(), done=i)
            except Exception as exc:  # noqa: BLE001
                handle(inv, exc=exc)
    return failures


def recorded_done(status, inv, stage):
    row = status.get((inv, stage))
    return row is not None and row["status"] == "done"


def is_done(status, inv, stage, force):
    """Done and not to be redone (--force redoes the selected stages)."""
    return not force and recorded_done(status, inv, stage)


def stage_extract(run, inventories, sizes):
    status = run.status()
    todo = []
    for inv, source in inventories.items():
        done = (is_done(status, inv, "extract", run.force)
                and (run.root / "lines" / f"{inv}.lines.tsv.gz").exists()
                and (run.root / "regions_meta" / f"{inv}.tsv.gz").exists()
                and (run.root / "extract_stats" / f"{inv}.json").exists())
        if not done:
            todo.append((inv, (inv, str(source), str(run.root))))
    run.log(f"[extract] {len(inventories) - len(todo)} already done, {len(todo)} to do")
    todo.sort(key=lambda t: -sizes[t[0]][1])
    if run.preset.format_version == 2:
        todo = [(inv, fargs + (run.preset,)) for inv, fargs in todo]
        if run.preset.merged_line_split == "on":
            from archival_structures.text_layer.line_split import extract_inventory_v2_split
            func = extract_inventory_v2_split
        else:
            func = extract_inventory_v2
    else:
        func = functools.partial(
            extract_inventory, untagged_tag=run.preset.untagged_tag,
            scan_id_prefix=run.preset.scan_id_prefix)
    failures = run_pool(run, func, todo, sizes, "extract")
    run.update_manifest("extract", {"n_inventories": len(inventories), "failed": failures})
    return failures


def stage_wordfix(run, inventories, sizes):
    """v2 stage (step 15f): glued-word repair of lines/<inv>.lines.tsv.gz, in place and idempotent
    (`text` is recomputed from `text_raw`; earlier `wordsplit:*` flags are replaced)."""
    import collections
    from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz
    from archival_structures.text_layer.word_split import (
        WordSplitParams, apply_word_split, build_vocabulary, token_core)
    preset = run.preset
    params = WordSplitParams(**dict(preset.word_split_params or {}))
    if run.args.word_params:
        params = WordSplitParams(**json.loads(run.args.word_params))
    out = run.root / "word_split"
    out.mkdir(parents=True, exist_ok=True)
    lines_dir = run.root / "lines"
    invs = [inv for inv in inventories if (lines_dir / f"{inv}.lines.tsv.gz").exists()]
    missing = [inv for inv in inventories if inv not in set(invs)]
    if missing:
        sys.exit(f"stage wordfix: no lines table for {missing[:5]}; run extract first")
    if run.args.word_vocab:
        vocab = collections.Counter()
        for row in read_tsv_gz(run.args.word_vocab):
            vocab[row["word"]] = int(row["count"])
    else:
        vocab = build_vocabulary(row["text_raw"] for inv in invs
                                 for row in read_tsv_gz(lines_dir / f"{inv}.lines.tsv.gz"))
    write_tsv_gz(out / "vocab.tsv.gz", ["word", "count"],
                 sorted(vocab.items(), key=lambda kv: (-kv[1], kv[0])))
    write_json_atomic(out / "params.json", {**params.to_dict(), "vocab_types": len(vocab),
                                            "vocab_tokens": sum(vocab.values()),
                                            "vocab_source": str(run.args.word_vocab or "selected inventories")})

    def long_tokens(text):
        return sum(len(token_core(t)[1]) >= 18 for t in text.split(" "))

    failures, stats = [], {}
    for inv in invs:
        path = lines_dir / f"{inv}.lines.tsv.gz"
        rows = list(read_tsv_gz(path))
        edits = []
        new = apply_word_split(rows, vocab, params, edits)
        header = list(rows[0].keys()) if rows else LINE_COLUMNS_V2
        changed = sum(a["text"] != a["text_raw"] for a in new)
        stats[inv] = {
            "lines": len(rows), "lines_changed": changed, "tokens_split": len(edits),
            "long_tokens_before": sum(long_tokens(r["text_raw"]) for r in rows),
            "long_tokens_after": sum(long_tokens(r["text"]) for r in new)}
        write_tsv_gz(path, header, ([r[c] for c in header] for r in new))
        write_tsv_gz(out / "edits" / f"{inv}.tsv.gz",
                     ["line_id", "token_index", "original", "replacement"], edits)
        run.log(f"[wordfix] {inv} {stats[inv]}")
    write_json_atomic(out / "stats.json", stats)
    run.update_manifest("wordfix", {"n_inventories": len(invs), "word_split_params": params.to_dict()})
    return failures


def stage_layout(run, inventories, sizes):
    """v2 stage (step 15fp): page layout tables under layout/; the input tables are only read."""
    from archival_structures.analysis.page_structure import LayoutParams
    from archival_structures.text_layer.layout import layout_inventory
    cli = json.loads(run.args.layout_params) if run.args.layout_params else {}
    params = LayoutParams.from_dict({**(run.preset.layout_params or {}), **cli})
    needed = ("scans/{}.scans.tsv.gz", "regions_meta/{}.tsv.gz", "lines/{}.lines.tsv.gz")
    missing = [inv for inv in inventories
               if not all((run.root / n.format(inv)).exists() for n in needed)]
    if missing:
        sys.exit(f"stage layout: no scans/regions_meta/lines tables for {missing[:5]}; run extract first")
    out = run.root / "layout"
    out.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out / "params.json", params.to_dict())
    stats_path = out / "stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8")) if stats_path.exists() else {}
    failures = []
    for inv in inventories:
        start = time.monotonic()
        try:
            stats[inv] = layout_inventory(inv, run.root, params)
        except Exception as exc:  # noqa: BLE001 -- record and continue
            failures.append(inv)
            run.record({"inventory_number": inv, "stage": "layout", "status": "failed",
                        "error": "".join(traceback.format_exception_only(type(exc), exc))}, sizes)
            run.log(f"[layout] FAILED {inv}: {exc!r}")
            continue
        run.record({"inventory_number": inv, "stage": "layout", "status": "done",
                    "n_rows": stats[inv]["n_regions_out"], "n_lines": stats[inv]["n_lines"],
                    "n_scans": stats[inv]["n_scans"], "seconds": round(time.monotonic() - start, 3)},
                   sizes)
        run.log(f"[layout] {inv} regions {stats[inv]['n_regions_in']} -> "
                f"{stats[inv]['n_regions_out']} merges={stats[inv]['n_merges']} "
                f"gutter_splits={stats[inv]['n_gutter_splits']} gutter_rel={stats[inv]['gutter_rel']}")
    write_json_atomic(stats_path, dict(sorted(stats.items(), key=lambda kv: inventory_sort_key(kv[0]))))
    run.update_manifest("layout", {"n_inventories": len(inventories), "failed": failures,
                                   "layout_params": params.to_dict()})
    return failures


def stage_types(run, inventories, sizes):
    """v2 stage (step 15g): line-segment and region types under types/; inputs are only read."""
    from archival_structures.text_layer.types import TypeParams, type_inventory
    cli = json.loads(run.args.types_params) if run.args.types_params else {}
    params = TypeParams.from_dict({**(run.preset.types_params or {}), **cli})
    needed = ("scans/{}.scans.tsv.gz", "lines/{}.lines.tsv.gz", "layout/lines/{}.tsv.gz",
              "layout/regions/{}.tsv.gz")
    missing = [inv for inv in inventories
               if not all((run.root / n.format(inv)).exists() for n in needed)]
    if missing:
        sys.exit(f"stage types: no scans/lines/layout tables for {missing[:5]}; run layout first")
    out = run.root / "types"
    out.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out / "params.json", params.to_dict())
    stats_path = out / "stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8")) if stats_path.exists() else {}
    failures = []
    for inv in inventories:
        start = time.monotonic()
        try:
            stats[inv] = type_inventory(inv, run.root, params)
        except Exception as exc:  # noqa: BLE001 -- record and continue
            failures.append(inv)
            run.record({"inventory_number": inv, "stage": "types", "status": "failed",
                        "error": "".join(traceback.format_exception_only(type(exc), exc))}, sizes)
            run.log(f"[types] FAILED {inv}: {exc!r}")
            continue
        run.record({"inventory_number": inv, "stage": "types", "status": "done",
                    "n_rows": stats[inv]["n_segments"], "n_lines": stats[inv]["n_lines"],
                    "seconds": round(time.monotonic() - start, 3)}, sizes)
        run.log(f"[types] {inv} segments {stats[inv]['n_segments']} "
                f"types={stats[inv]['segments_per_type']} splits={stats[inv]['splits_per_rule']}")
    write_json_atomic(stats_path, dict(sorted(stats.items(), key=lambda kv: inventory_sort_key(kv[0]))))
    run.update_manifest("types", {"n_inventories": len(inventories), "failed": failures,
                                  "types_params": params.to_dict()})
    return failures


def stage_units(run, inventories, sizes):
    """v2 stage (step 15h): units, reading order, doc_id map and REPUBLIC crosswalk under units/;
    inputs are only read."""
    from archival_structures.text_layer.units import (
        UnitParams, unit_inventory)
    cli = json.loads(run.args.unit_params) if run.args.unit_params else {}
    params = UnitParams.from_dict({**(run.preset.unit_params or {}), **cli})
    print_mode = run.preset.merged_line_split == "on"
    republic_dir = Path(run.args.republic_dir) if run.args.republic_dir else None
    needed = ("scans/{}.scans.tsv.gz", "regions_meta/{}.tsv.gz", "lines/{}.lines.tsv.gz",
              "layout/lines/{}.tsv.gz", "layout/regions/{}.tsv.gz", "types/segments/{}.tsv.gz")
    missing = [inv for inv in inventories
               if not all((run.root / n.format(inv)).exists() for n in needed)]
    if missing:
        sys.exit(f"stage units: no scans/lines/layout/types tables for {missing[:5]}; run types first")
    out = run.root / "units"
    out.mkdir(parents=True, exist_ok=True)
    republic_used = None
    if print_mode and republic_dir is None:
        run.log("[units] WARNING: no --republic-dir given: no REPUBLIC crosswalk")
    elif print_mode:
        have = [inv for inv in inventories if (republic_dir / f"paragraphs-{inv}.tsv.gz").exists()]
        if len(have) < len(inventories):
            lacking = [inv for inv in inventories if inv not in have]
            run.log(f"[units] WARNING: no paragraphs-<inv>.tsv.gz in {republic_dir} for "
                    f"{lacking[:5]}: no REPUBLIC crosswalk for those inventories")
        if have:
            republic_used = str(republic_dir)
    write_json_atomic(out / "params.json", {**params.to_dict(), "republic_dir": republic_used})
    stats_path = out / "stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8")) if stats_path.exists() else {}
    failures = []
    for inv in inventories:
        start = time.monotonic()
        try:
            stats[inv] = unit_inventory(inv, run.root, params, print_mode=print_mode,
                                        republic_path=(republic_dir / f"paragraphs-{inv}.tsv.gz"
                                                        if republic_dir else None))
        except Exception as exc:  # noqa: BLE001 -- record and continue
            failures.append(inv)
            run.record({"inventory_number": inv, "stage": "units", "status": "failed",
                        "error": "".join(traceback.format_exception_only(type(exc), exc))}, sizes)
            run.log(f"[units] FAILED {inv}: {exc!r}")
            continue
        run.record({"inventory_number": inv, "stage": "units", "status": "done",
                    "n_rows": stats[inv]["n_units"], "n_lines": 0,
                    "seconds": round(time.monotonic() - start, 3)}, sizes)
        run.log(f"[units] {inv} units {stats[inv]['n_units']} per_type={stats[inv]['units_per_type']} "
                f"status={stats[inv]['old_status']} collisions={stats[inv]['n_id_collisions']}")
    write_json_atomic(stats_path, dict(sorted(stats.items(), key=lambda kv: inventory_sort_key(kv[0]))))
    run.update_manifest("units", {"n_inventories": len(inventories), "failed": failures,
                                  "unit_params": params.to_dict()})
    return failures


def extracted(run, inventories):
    status = run.status()
    done = [inv for inv in inventories if recorded_done(status, inv, "extract")]
    missing = [inv for inv in inventories if inv not in set(done)]
    if missing and not run.args.allow_partial:
        sys.exit(f"stage 1 (extract) is not done for {len(missing)} inventories "
                 f"(e.g. {missing[:5]}); finish it or pass --allow-partial")
    return done, missing


def layer_inventories(run):
    """Inventories that have a lines table in the layer (what a full wbc run covers)."""
    lines_dir = run.root / "lines"
    return {p.name[:-len(".lines.tsv.gz")] for p in lines_dir.glob("*.lines.tsv.gz")}


def stage_wbc(run, inventories, sizes):
    import pandas as pd

    v2 = run.preset.format_version == 2
    in_layer = layer_inventories(run)
    if set(inventories) < in_layer and not run.args.allow_subset_wbc:
        sys.exit(f"stage wbc: this run covers {len(inventories)} of the {len(in_layer)} inventories "
                 f"in the layer, and wbc/ (stats, corpus default, per-inventory sets) would be "
                 f"overwritten from the subset. Run all of them, or pass --allow-subset-wbc")
    if v2:
        needed = ("lines/{}.lines.tsv.gz", "types/segments/{}.tsv.gz", "units/units/{}.tsv.gz",
                  "units/segments/{}.tsv.gz")
        absent = [inv for inv in inventories
                  if not all((run.root / n.format(inv)).exists() for n in needed)]
        if absent:
            sys.exit(f"stage wbc: no lines/types/units tables for {absent[:5]}; run units first")
        done, missing = list(inventories), []
        worker = functools.partial(write_word_break_stats_part_v2, robust=True)
        columns = WORD_BREAK_STATS_COLUMNS_V2
    else:
        done, missing = extracted(run, inventories)
        worker = write_word_break_stats_part
        columns = WORD_BREAK_STATS_COLUMNS
    status = run.status()
    todo = [(inv, (inv, str(run.root))) for inv in done
            if not (is_done(status, inv, "wbc", run.force) and (run.root / "wbc" / "parts" / f"{inv}.tsv").exists())]
    todo.sort(key=lambda t: -sizes[t[0]][1] if t[0] in sizes else 0)
    run.log(f"[wbc] {len(done) - len(todo)} stats parts already done, {len(todo)} to do")
    failures = run_pool(run, worker, todo, sizes, "wbc")
    if failures:
        sys.exit(f"[wbc] stats failed for {failures}; fix and rerun")

    parts = [pd.read_csv(run.root / "wbc" / "parts" / f"{inv}.tsv", sep="\t", quoting=3,
                         dtype={"inventory_number": str, "char": str}, keep_default_na=False,
                         na_values=[])
             for inv in done]
    stats = pd.concat(parts, ignore_index=True)
    write_plain_tsv(run.root / "wbc" / "wbc_stats.tsv", columns,
                        [[format_stat(v) for v in row] for row in stats[columns].itertuples(index=False)])

    pooled = pooled_stats(stats)
    cols = ["char_name", "n_inventories", "end_freq", "end_rel_frac", "start_rel_frac",
            "n_end_attached", "cont_lower_frac", "join_vocab_frac", "n_both_ends", "n_both_ends_any", "n_start_only",
            "n_digit_both_sides"]
    with pd.option_context("display.width", 250, "display.max_columns", 20):
        print("\nPooled distribution over this run's inventories (top 25 line-final candidates):")
        print(pooled[cols].head(25).round(3).to_string())
    if run.args.wbc_stats_only:
        run.log("[wbc] --wbc-stats-only: stopping before the decision")
        return []

    inventory_lines = {inv: _nonempty_lines(stats, inv) for inv in done}
    if v2:
        per_inventory, corpus = decide_v2(run, stats, inventory_lines)
    else:
        per_inventory, corpus = decide_word_breaks(stats, inventory_lines)
    corpus["allow_partial"] = bool(missing)
    corpus["n_inventories_missing_extract"] = len(missing)
    write_json_atomic(run.root / "wbc" / "corpus_default.json", corpus)
    cols = ["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default", "char_status"]
    write_plain_tsv(run.root / "wbc" / "wbc_per_inventory.tsv", cols,
                        [[r[c] for c in cols] for r in per_inventory])
    n_diff = sum(r["differs_from_default"] for r in per_inventory)
    run.log(f"[wbc] corpus default {corpus['default_set']} (pooled rule {corpus['pooled_rule_set']}), "
            f"sanity_pass={corpus['sanity_pass']}, {n_diff}/{len(per_inventory)} inventories differ")
    extra = ({"word_break_stat": run.preset.word_break_stat, "pinned": bool(run.preset.wbc_pinned)}
             if v2 else {})
    run.update_manifest("wbc", {"n_inventories": len(done), "allow_partial": bool(missing),
                                "missing_extract": missing[:100],
                                "default_set": corpus["default_set"],
                                "pooled_rule_set": corpus["pooled_rule_set"],
                                "sanity_pass": corpus["sanity_pass"], **extra})
    if not corpus["sanity_pass"]:
        run.log("[wbc] SANITY ANCHORS FAILED -- stop and ask before joining")
    return []


def decide_v2(run, stats, inventory_lines):
    """v2 decision (step 15hj): the preset's rule, sanity anchors and statistic. Also writes both
    variants' per-inventory sets (wbc/wbc_per_inventory_{default,robust}.tsv) for comparison."""
    preset = run.preset
    rule = dict(preset.word_break_rule or DEFAULT_WORD_BREAK_RULE)
    cols = ["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default", "char_status"]
    results = {}
    for name in ("default", "robust"):
        frame = stats
        if name == "robust":
            frame = stats.copy()
            frame["all_freq"] = stats["all_freq_robust"]
            frame["end_rel_frac"] = stats["end_rel_frac_robust"]
        results[name] = decide_word_breaks(frame, inventory_lines, rule,
                                           sanity_in=list(preset.wbc_sanity_in),
                                           sanity_out=list(preset.wbc_sanity_out))
        write_plain_tsv(run.root / "wbc" / f"wbc_per_inventory_{name}.tsv", cols,
                        [[r[c] for c in cols] for r in results[name][0]])
        write_json_atomic(run.root / "wbc" / f"corpus_default_{name}.json", results[name][1])
    if preset.word_break_stat not in results:
        sys.exit(f"unknown word_break_stat {preset.word_break_stat!r}")
    rows, corpus = results[preset.word_break_stat]
    if preset.wbc_pinned:
        rows, corpus = _apply_pinned(rows, corpus, preset)
    return rows, corpus


def _apply_pinned(rows, corpus, preset):
    """Step 15hj2 (UA-010 (c)): replace the derived sets by the preset's pinned sets."""
    pinned = dict(preset.wbc_pinned)
    default = "".join(sorted(pinned["default"]))
    new_rows = []
    for r in rows:
        chosen = "".join(sorted(pinned.get(r["inventory_number"], pinned["default"])))
        derived = set(r["wbc_set"])
        status = {}
        for item in r["char_status"].split(" "):
            if item:
                c, _, st = item.rpartition(":")
                status[c] = st
        parts = []
        for c in sorted(set(status) | derived | set(chosen)):
            tags = [status[c]] if c in status else []
            if c in chosen:
                tags.append("pinned_in")
            else:
                tags.append("pinned_out")
            parts.append(f"{c}:{'/'.join(tags)}")
        new_rows.append({**r, "mode": "pinned", "wbc_set": chosen,
                         "differs_from_default": sorted(chosen) != sorted(default),
                         "char_status": " ".join(parts)})
    new = dict(corpus)
    chars = sorted(default)
    new["derived_default_set"] = corpus["default_set"]
    new["pooled_rule_sanity_pass"] = corpus["sanity_pass"]
    new["default_set"] = chars
    new["default_set_names"] = [char_label(c) for c in chars]
    new["pinned"] = pinned
    new["sanity_pass"] = (all(c in chars for c in corpus["sanity_in"])
                          and not any(c in chars for c in corpus["sanity_out"]))
    new["rule_text"] = corpus["rule_text"] + " Sets pinned by the preset (wbc_pinned)."
    return new_rows, new


def _nonempty_lines(stats, inv):
    rows = stats[stats["inventory_number"] == inv]
    if len(rows):
        return int(rows["n_lines"].iloc[0])
    return 0  # no line-final candidate at all: an inventory with (almost) no text


def stage_join(run, inventories, sizes):
    v2 = run.preset.format_version == 2
    done, missing = (list(inventories), []) if v2 else extracted(run, inventories)
    if not (run.root / "wbc" / "wbc_per_inventory.tsv").exists():
        sys.exit("stage 2 (wbc) has not been run")
    sets, default_set = read_word_break_sets(run.root)
    corpus = json.loads((run.root / "wbc" / "corpus_default.json").read_text(encoding="utf-8"))
    if not corpus["sanity_pass"] and not run.args.ignore_sanity:
        sys.exit("the WBC sanity anchors failed (see wbc/corpus_default.json); stop and ask. "
                 "--ignore-sanity overrides (recorded in the manifest)")
    status = run.status()
    todo = []
    for inv in done:
        if inv not in sets:
            if not run.args.allow_partial:
                sys.exit(f"{inv} has no WBC decision; rerun --stage wbc or pass --allow-partial")
            pins = run.preset.wbc_pinned
            if v2 and pins:   # 15ir: the preset's own pin, as the wbc stage would have applied
                sets[inv] = set(pins.get(inv, pins["default"]))
            else:
                sets[inv] = default_set
        wbcs = sets[inv]
        lml = ""
        if v2:
            lml_pins = run.preset.join_lead_marks_lower   # step 15l, per inventory
            lml = lml_pins.get(inv, lml_pins.get("default", "")) if lml_pins else ""
            stats_path = run.root / "join" / "stats" / f"{inv}.json"
            out_path = run.root / "join" / "units" / f"{inv}.tsv.gz"
        else:
            stats_path = run.root / "join_stats" / f"{inv}.json"
            out_path = run.root / "regions" / f"{inv}.tsv.gz"
        previous = json.loads(stats_path.read_text(encoding="utf-8")) if stats_path.exists() else {}
        current = (is_done(status, inv, "join", run.force)
                   and out_path.exists()
                   and previous.get("wbc_set") == "".join(sorted(wbcs))
                   and previous.get("guards") == list(ALL_GUARDS)
                   and (not v2 or (previous.get("lead_marks", "") == run.preset.join_lead_marks
                                   and previous.get("lead_marks_lower", "") == lml
                                   and previous.get("upper_guard", False)
                                   == run.preset.join_upper_guard)))
        if not current:
            todo.append((inv, (inv, str(run.root), wbcs, run.preset.join_lead_marks, lml,
                               run.preset.join_upper_guard) if v2
                         else (inv, str(run.root), wbcs, run.args.write_events)))
    todo.sort(key=lambda t: -sizes[t[0]][1] if t[0] in sizes else 0)
    run.log(f"[join] {len(done) - len(todo)} already done with the current WBC set and guards, "
            f"{len(todo)} to do")
    failures = run_pool(run, join_inventory_v2 if v2 else join_inventory, todo, sizes, "join")
    run.update_manifest("join", {"n_inventories": len(done), "allow_partial": bool(missing),
                                 "failed": failures, "write_events": run.args.write_events,
                                 "ignore_sanity": run.args.ignore_sanity})
    return failures


def estimate(inventories, sizes, workers, preset):
    xml = [sizes[inv][1] for inv in inventories]
    stored = sum(sizes[inv][0] for inv in inventories)
    print(f"{len(inventories)} inventories, {stored / 1e9:.2f} GB on disk, "
          f"{sum(xml) / 1e9:.2f} GB uncompressed PageXML")
    fit = preset.throughput_fit
    if fit is None or preset.output_bytes_per_xml_byte is None:
        print(f"preset {preset.name} has no throughput fit; no prediction")
        return
    for stage in STAGES:
        print(f"  {stage:8s} seconds/inventory = {fit[stage]['intercept_s']:.3f} + "
              f"{fit[stage]['s_per_mb']:.4f} x MB_xml  "
              f"-> wall {predict_wall(xml, workers, fit, stages=(stage,)) / 60:.1f} min on {workers} workers")
    total = predict_wall(xml, workers, fit)
    print(f"  all three stages: {total / 3600:.2f} h ({total / 60:.1f} min) on {workers} workers "
          f"(single-worker CPU time {sum(predict_seconds(b, s, fit) for b in xml for s in STAGES) / 3600:.1f} h)")
    print(f"  expected output (lines/ + regions/): ~{preset.output_bytes_per_xml_byte * sum(xml) / 1e9:.1f} GB")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pagexml-dir", type=Path, default=None,
                        help="required except for --stage wordfix, layout, types and units")
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--preset", choices=sorted(PRESETS), default="voc_v1",
                        help="layer preset: output directory name, untagged-region tag, scan-id "
                             "prefix and throughput fit")
    parser.add_argument("--stage", choices=STAGES + ("wordfix", "layout", "types", "units", "all"), default="all")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--inventories", help="comma-separated inventory numbers (str, e.g. 1539,9626A)")
    group.add_argument("--inventories-file", help="file with one inventory number per line")
    parser.add_argument("--limit", type=int, default=0, help="only the first N inventories (smoke test)")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--estimate", action="store_true",
                        help="print predicted wall time from input bytes; parses nothing")
    parser.add_argument("--allow-partial", action="store_true",
                        help="let wbc/join run although extract is not done everywhere (recorded)")
    parser.add_argument("--allow-subset-wbc", action="store_true",
                        help="stage wbc: run although the inventories are a strict subset of those in "
                             "the layer (wbc/ is then rewritten from the subset)")
    parser.add_argument("--ignore-sanity", action="store_true",
                        help="stage join: run although the WBC sanity anchors failed (recorded)")
    parser.add_argument("--force", action="store_true",
                        help="redo the selected stage(s) even for inventories marked done")
    parser.add_argument("--wbc-stats-only", action="store_true",
                        help="stage wbc: compute and print the statistics, decide nothing")
    parser.add_argument("--write-events", action="store_true",
                        help="stage join: also write join_events/<inv>.tsv.gz (guard firings, "
                             "merges, diagnostics) for error analysis")
    parser.add_argument("--word-vocab", default=None,
                        help="stage wordfix: vocab.tsv.gz (word, count) instead of building one")
    parser.add_argument("--word-params", default=None,
                        help="stage wordfix: JSON WordSplitParams overriding the preset's")
    parser.add_argument("--layout-params", default=None,
                        help="stage layout: JSON LayoutParams overriding the defaults")
    parser.add_argument("--types-params", default=None,
                        help="stage types: JSON TypeParams overriding the preset's")
    parser.add_argument("--unit-params", default=None,
                        help="stage units: JSON UnitParams overriding the preset's")
    parser.add_argument("--republic-dir", default=None,
                        help="stage units (print): directory with REPUBLIC paragraphs-<inv>.tsv.gz "
                             "for the line-id crosswalk (without it, print builds have no crosswalk)")
    args = parser.parse_args(argv)
    if args.pagexml_dir is None and args.stage not in ("wordfix", "layout", "types", "units") + (
            ("wbc", "join") if PRESETS[args.preset].format_version == 2 else ()):
        parser.error("--pagexml-dir is required")
    if args.stage in ("wordfix", "layout", "types", "units") and PRESETS[args.preset].format_version != 2:
        parser.error(f"--stage {args.stage} needs a v2 preset")
    v2_preset = PRESETS[args.preset].format_version == 2
    local_stages = ("wordfix", "layout", "types", "units") + (("wbc", "join") if v2_preset else ())

    if args.stage in local_stages:
        lines_dir = Path(args.out_dir) / PRESETS[args.preset].name / "lines"
        found = sorted((p.name[:-len(".lines.tsv.gz")] for p in lines_dir.glob("*.lines.tsv.gz")),
                       key=inventory_sort_key)
        wanted = None
        if args.inventories:
            wanted = [s.strip() for s in args.inventories.split(",") if s.strip()]
        elif args.inventories_file:
            wanted = [s.strip() for s in Path(args.inventories_file).read_text().split() if s.strip()]
        inventories = {inv: lines_dir / f"{inv}.lines.tsv.gz" for inv in (wanted or found)}
        sizes = {}
    else:
        inventories = select_inventories(args)
        sizes = {inv: input_bytes(src) for inv, src in inventories.items()}
    if args.estimate:
        estimate(inventories, sizes, args.workers, PRESETS[args.preset])
        return 0
    version = check_pagexml_tools_version()
    run = Run(args, PRESETS[args.preset])
    run.log(f"start stage={args.stage} inventories={len(inventories)} workers={args.workers} "
            f"pagexml-tools={version}")
    preset = PRESETS[args.preset]
    if preset.format_version == 2:
        stages = ("extract",) if args.stage in ("all", "extract") else (args.stage,)
        if args.stage == "all":
            if preset.word_split == "on":
                stages += ("wordfix",)
            stages += ("layout", "types", "units", "wbc", "join")
    else:
        stages = STAGES if args.stage == "all" else (args.stage,)
    failures = []
    for stage in stages:
        failures += {"extract": stage_extract, "wbc": stage_wbc, "join": stage_join,
                     "wordfix": stage_wordfix, "layout": stage_layout,
                     "types": stage_types, "units": stage_units}[stage](
            run, inventories, sizes) or []
    run.log(f"end stage={args.stage} failures={len(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
