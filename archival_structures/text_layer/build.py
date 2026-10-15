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
    discover_inventories, input_bytes, inventory_sort_key, write_json_atomic, write_plain_tsv)
from archival_structures.text_layer.join import ALL_GUARDS, REGION_COLUMNS, join_inventory
from archival_structures.text_layer.presets import PRESETS, predict_seconds, predict_wall
from archival_structures.text_layer.provenance import (
    check_pagexml_tools_version, package_info, pagexml_tools_info)
from archival_structures.text_layer.word_breaks import (
    WORD_BREAK_STATS_COLUMNS, decide_word_breaks, format_stat, pooled_stats,
    read_word_break_sets, write_word_break_stats_part)

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
        })
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
    failures = run_pool(run, functools.partial(
        extract_inventory, untagged_tag=run.preset.untagged_tag,
        scan_id_prefix=run.preset.scan_id_prefix), todo, sizes, "extract")
    run.update_manifest("extract", {"n_inventories": len(inventories), "failed": failures})
    return failures


def extracted(run, inventories):
    status = run.status()
    done = [inv for inv in inventories if recorded_done(status, inv, "extract")]
    missing = [inv for inv in inventories if inv not in set(done)]
    if missing and not run.args.allow_partial:
        sys.exit(f"stage 1 (extract) is not done for {len(missing)} inventories "
                 f"(e.g. {missing[:5]}); finish it or pass --allow-partial")
    return done, missing


def stage_wbc(run, inventories, sizes):
    import pandas as pd

    done, missing = extracted(run, inventories)
    status = run.status()
    todo = [(inv, (inv, str(run.root))) for inv in done
            if not (is_done(status, inv, "wbc", run.force) and (run.root / "wbc" / "parts" / f"{inv}.tsv").exists())]
    todo.sort(key=lambda t: -sizes[t[0]][1])
    run.log(f"[wbc] {len(done) - len(todo)} stats parts already done, {len(todo)} to do")
    failures = run_pool(run, write_word_break_stats_part, todo, sizes, "wbc")
    if failures:
        sys.exit(f"[wbc] stats failed for {failures}; fix and rerun")

    parts = [pd.read_csv(run.root / "wbc" / "parts" / f"{inv}.tsv", sep="\t", quoting=3,
                         dtype={"inventory_number": str, "char": str}, keep_default_na=False,
                         na_values=[])
             for inv in done]
    stats = pd.concat(parts, ignore_index=True)
    write_plain_tsv(run.root / "wbc" / "wbc_stats.tsv", WORD_BREAK_STATS_COLUMNS,
                        [[format_stat(v) for v in row] for row in stats[WORD_BREAK_STATS_COLUMNS].itertuples(index=False)])

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
    run.update_manifest("wbc", {"n_inventories": len(done), "allow_partial": bool(missing),
                                "missing_extract": missing[:100],
                                "default_set": corpus["default_set"],
                                "pooled_rule_set": corpus["pooled_rule_set"],
                                "sanity_pass": corpus["sanity_pass"]})
    if not corpus["sanity_pass"]:
        run.log("[wbc] SANITY ANCHORS FAILED -- stop and ask before joining")
    return []


def _nonempty_lines(stats, inv):
    rows = stats[stats["inventory_number"] == inv]
    if len(rows):
        return int(rows["n_lines"].iloc[0])
    return 0  # no line-final candidate at all: an inventory with (almost) no text


def stage_join(run, inventories, sizes):
    done, missing = extracted(run, inventories)
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
            sets[inv] = default_set
        wbcs = sets[inv]
        stats_path = run.root / "join_stats" / f"{inv}.json"
        previous = json.loads(stats_path.read_text(encoding="utf-8")) if stats_path.exists() else {}
        current = (is_done(status, inv, "join", run.force)
                   and (run.root / "regions" / f"{inv}.tsv.gz").exists()
                   and previous.get("wbc_set") == "".join(sorted(wbcs))
                   and previous.get("guards") == list(ALL_GUARDS))
        if not current:
            todo.append((inv, (inv, str(run.root), wbcs, run.args.write_events)))
    todo.sort(key=lambda t: -sizes[t[0]][1])
    run.log(f"[join] {len(done) - len(todo)} already done with the current WBC set and guards, "
            f"{len(todo)} to do")
    failures = run_pool(run, join_inventory, todo, sizes, "join")
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
    parser.add_argument("--pagexml-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--preset", choices=sorted(PRESETS), default="voc_v1",
                        help="layer preset: output directory name, untagged-region tag, scan-id "
                             "prefix and throughput fit")
    parser.add_argument("--stage", choices=STAGES + ("all",), default="all")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--inventories", help="comma-separated inventory numbers (str, e.g. 1539,9626A)")
    group.add_argument("--inventories-file", help="file with one inventory number per line")
    parser.add_argument("--limit", type=int, default=0, help="only the first N inventories (smoke test)")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--estimate", action="store_true",
                        help="print predicted wall time from input bytes; parses nothing")
    parser.add_argument("--allow-partial", action="store_true",
                        help="let wbc/join run although extract is not done everywhere (recorded)")
    parser.add_argument("--ignore-sanity", action="store_true",
                        help="stage join: run although the WBC sanity anchors failed (recorded)")
    parser.add_argument("--force", action="store_true",
                        help="redo the selected stage(s) even for inventories marked done")
    parser.add_argument("--wbc-stats-only", action="store_true",
                        help="stage wbc: compute and print the statistics, decide nothing")
    parser.add_argument("--write-events", action="store_true",
                        help="stage join: also write join_events/<inv>.tsv.gz (guard firings, "
                             "merges, diagnostics) for error analysis")
    args = parser.parse_args(argv)

    inventories = select_inventories(args)
    sizes = {inv: input_bytes(src) for inv, src in inventories.items()}
    if args.estimate:
        estimate(inventories, sizes, args.workers, PRESETS[args.preset])
        return 0
    version = check_pagexml_tools_version()
    run = Run(args, PRESETS[args.preset])
    run.log(f"start stage={args.stage} inventories={len(inventories)} workers={args.workers} "
            f"pagexml-tools={version}")
    stages = STAGES if args.stage == "all" else (args.stage,)
    failures = []
    for stage in stages:
        failures += {"extract": stage_extract, "wbc": stage_wbc, "join": stage_join}[stage](
            run, inventories, sizes) or []
    run.log(f"end stage={args.stage} failures={len(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
