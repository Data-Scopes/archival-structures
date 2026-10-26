#!/usr/bin/env python
"""
Hand-off script for the full `text_layer_v3` run (phase 09, step 15n; UA-015).

Builds the SG text layer from PageXML that may live on an external drive: one input dir per
side is assembled from symlinks, the inputs and the free space are checked, the build runs
through `coreshare` and the finished layer is audited. Run it from the AS root, with the AS
virtualenv (`.venv/bin/python scripts/run_text_layer_v3_full.py ...`). It never writes into a
`--*-src` dir.

What to put on the drive (any number of dirs; names must match exactly):
  hand:   NL-HaNA_1.01.02_<inv>-pagexml.tar.gz  (also .zip, or a dir NL-HaNA_1.01.02_<inv>)
  print:  NL-HaNA_1.10.94_<inv>.zip             (also .tar.gz, or a dir NL-HaNA_1.10.94_<inv>)
`<inv>` is the inventory number with an optional capital letter, e.g. 3349, 11182, 9626A. Dot-files
and names that do not match are ignored. When several sources hold the same inventory, the first
`--*-src` wins and the duplicate is logged. Print inventories need `paragraphs-<inv>.tsv.gz`
(REPUBLIC) in `--republic-dir` for the line-id crosswalk; without it the units stage runs for them
without a crosswalk (warned about).

Order of work (placeholders: <drive>/sg_hand, <drive>/sg_print, <drive>/republic, <drive>/layer):
  1. check only, builds nothing (counts, bytes, predicted size and time, free space):
       .venv/bin/python scripts/run_text_layer_v3_full.py --check-only \\
           --hand-src <drive>/sg_hand --print-src <drive>/sg_print \\
           --republic-dir <drive>/republic --out-dir <drive>/layer
  2. smoke run of one inventory per side, into a separate out dir:
       ... same arguments, with --limit 1 --out-dir <drive>/layer_smoke
  3. the full run: the arguments of step 1 without --check-only.
`--hand-src` and `--print-src` can be given several times. `--only hand|print` runs one side;
`--hand-inventories-file F` / `--print-inventories-file F` (one inventory per line) restrict a side;
`--want N --min M` is the coreshare request (default 8 and 2; the build uses the granted count).

After an interruption (Ctrl-C, a crash, a full disk, a pulled cable) rerun the same command: every
stage marks each inventory done in `<out-dir>/<side>/*/status.tsv` and a rerun skips what is done.

Logs: `<out-dir>/run_v3.log` (this script's lines with UTC timestamps and the build output),
`<out-dir>/<side>/*/progress.log` (the build's own log), `<out-dir>/audit.json` (the audit).

The audit (`reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py`) is written for the pilot:
its inventory lists are the 14 hand and 2 print pilot inventories, it reads the sources from
`data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02` and LotA gold files, and the 4 lower-case merge
share invariants (3351, 3823, 11182, 11281 as in 15m; 455, 456) are pilot findings. On the full
run it can fail, or report failures; the script logs the outcome and does not fail on it.

Predictions are rough. Both are per uncompressed XML byte (an archive counts as its size times
the pilot compression ratio 4.16). Output size is XML bytes times the pilot ratio. Wall time is a
range: the lower end is max(worker-seconds / --want, the largest single inventory), the upper end
3 times that (the pilot hand run took 2.9 times its lower end: stage barriers, one task per
inventory). The pilot read from local disk; a USB drive may be slower, and the inventory mix differs.
"""

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from archival_structures.text_layer.io import discover_inventories_v2
from archival_structures.text_layer.presets import PRESETS

BUILD = ".venv/bin/as-build-text-layer"
CORESHARE = "/Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py"
AUDIT = "reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py"
SIDES = {"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}
SPACE_FACTOR = 1.2
MIB = 2 ** 20

# Pilot numbers, `data/text_layer_v2_dev/15m-a/{hand,print}` (14 and 2 inventories), from status.tsv:
# XML bytes = sum of `input_xml_bytes` of the extract rows (hand 3,156,766,230; print 1,009,177,478);
# output = `du -sk` (hand 312,056 KiB; print 85,768 KiB); worker-seconds = sum of `seconds` over all
# stages (hand 381.3; print 443.5). The 8 archives (6 hand tars, 2 print zips) hold 4.16 XML bytes per
# archive byte (hand 4.175, print 4.148). Pilot wall time (15n-a, hand 7 workers): 154 s, 2.9 x the
# lower bound below; print (2 inventories): 289 s, 1.3 x.
ARCHIVE_XML_RATIO = 4.16
OUTPUT_PER_XML_BYTE = {"hand": 312_056 * 1024 / 3_156_766_230, "print": 85_768 * 1024 / 1_009_177_478}
WORKER_SECONDS_PER_XML_BYTE = {"hand": 381.3 / 3_156_766_230, "print": 443.5 / 1_009_177_478}
SLOW_FACTOR = 3


class Log:
    """Console plus an optional append-only log file; own lines carry a UTC timestamp."""

    def __init__(self):
        self.path = None

    def __call__(self, msg):
        line = f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}"
        print(line, flush=True)
        self.raw(line)

    def raw(self, line):
        if self.path:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line.rstrip("\n") + "\n")


def find_root():
    """The AS root is the cwd with a pyproject.toml naming `archival-structures`."""
    pyproject = Path.cwd() / "pyproject.toml"
    if pyproject.is_file() and re.search(r'^name\s*=\s*"archival-structures"', pyproject.read_text(),
                                         re.M):
        return Path.cwd()
    return None


def entry_bytes(path):
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.stat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


def xml_bytes(path):
    """Uncompressed XML bytes of an inventory entry: a dir's file sizes, an archive's size x ratio."""
    nbytes = entry_bytes(path)
    return nbytes * ARCHIVE_XML_RATIO if Path(path).is_file() else nbytes


def link_side(side, srcs, work_dir, log):
    """(Re)create `<work_dir>/<side>/` with one symlink per inventory; the first source wins.
    Returns {inv: link path} in inventory order."""
    pattern = PRESETS[SIDES[side]].source_pattern
    target = Path(work_dir) / side
    target.mkdir(parents=True, exist_ok=True)
    mode = target.stat().st_mode
    if not mode & 0o020:  # only the owner may chmod; a dir another user made is often group-writable already
        try:
            os.chmod(target, mode | 0o020)
        except PermissionError:
            log(f"[{side}] cannot make {target} group-writable (not its owner); continuing")
    for old in target.iterdir():
        if old.is_symlink():
            old.unlink()
        else:
            raise SystemExit(f"{old} in the work dir is not a link; remove it or use another --work-dir")
    chosen = {}      # inv -> source entry
    for src in srcs:
        for inv, entry in discover_inventories_v2(src, pattern).items():
            if inv in chosen:
                log(f"[{side}] inventory {inv} also in {entry} (ignored); used {chosen[inv]}")
            else:
                chosen[inv] = entry
    links = {}
    for inv, entry in chosen.items():
        link = target / entry.name
        link.symlink_to(Path(os.path.abspath(entry)))
        links[inv] = link
    return links


def read_subset(path):
    return [ln.strip() for ln in Path(path).read_text().splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")]


def select(links, subset, limit):
    """Inventories the build will do: the subset (if any), then the first `limit`."""
    invs = [i for i in links if subset is None or i in subset]
    return invs[:limit] if limit else invs


def republic_missing(invs, republic_dir):
    return [i for i in invs if not (Path(republic_dir) / f"paragraphs-{i}.tsv.gz").is_file()]


def span(low, high):
    return f"{low / 60:.0f}-{high / 60:.0f} min" if high < 7200 else f"{low / 3600:.1f}-{high / 3600:.1f} h"


def predict(side, xml_sizes, want):
    """(output bytes, lower and upper wall seconds) for a list of per-inventory XML byte counts."""
    rate = WORKER_SECONDS_PER_XML_BYTE[side]
    low = max(sum(xml_sizes) * rate / max(want, 1), max(xml_sizes, default=0) * rate)
    return sum(xml_sizes) * OUTPUT_PER_XML_BYTE[side], low, SLOW_FACTOR * low


def run_cmd(cmd, log):
    """Run `cmd`, stream its output to the console and the log; return the exit code."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        log.raw(line)
    return proc.wait()


def tree_bytes(path):
    return entry_bytes(path) if Path(path).exists() else 0


def side_summary(out_dir, side):
    """(inventories with every stage done, failed or unfinished rows) from the status.tsv files."""
    done, bad = {}, 0
    for status in (Path(out_dir) / side).glob("*/status.tsv"):
        for row in status.read_text().splitlines()[1:]:
            cols = row.split("\t")
            if len(cols) < 3:
                continue
            if cols[2] == "done":
                done.setdefault(cols[0], set()).add(cols[1])
            else:
                bad += 1
    n_stages = max((len(s) for s in done.values()), default=0)
    return sum(len(s) == n_stages for s in done.values()), bad


def build_cmds(side, args, work_dir, out_dir, subset_file):
    common = [BUILD, "--pagexml-dir", str(Path(work_dir) / side), "--out-dir", str(Path(out_dir) / side),
              "--preset", SIDES[side], "--stage", "all"]
    if subset_file:
        common += ["--inventories-file", str(subset_file)]
    if args.limit:
        common += ["--limit", str(args.limit)]
    estimate = common + ["--estimate", "--workers", str(args.want)]
    real = common[:]
    if side == "print" and args.republic_dir:
        real += ["--republic-dir", str(args.republic_dir)]
    coreshare = ["python3", CORESHARE, "run", "--project", "archival-structures", "--label", f"v3-{side}",
                 "--want", str(args.want), "--min", str(args.min), "--wait", "--"]
    return estimate, coreshare + real + ["--workers", "{workers}"]


def parse_args(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hand-src", action="append", default=[], type=Path, metavar="DIR",
                    help="source dir for sg_1.01.02_v3; repeatable, first has priority")
    ap.add_argument("--print-src", action="append", default=[], type=Path, metavar="DIR",
                    help="source dir for sg_1.10.94_v3; repeatable, first has priority")
    ap.add_argument("--republic-dir", type=Path, default=None,
                    help="dir with paragraphs-<inv>.tsv.gz for the print inventories (warn if absent)")
    ap.add_argument("--out-dir", required=True, type=Path, help="layer base: <out-dir>/hand and /print")
    ap.add_argument("--work-dir", type=Path, default=Path("data/text_layer_v3_full_inputs"),
                    help="local dir for the input links (default %(default)s)")
    ap.add_argument("--hand-inventories-file", type=Path, default=None)
    ap.add_argument("--print-inventories-file", type=Path, default=None)
    ap.add_argument("--only", choices=sorted(SIDES), default=None, help="run one side")
    ap.add_argument("--limit", type=int, default=0, help="only the first N inventories per side (smoke)")
    ap.add_argument("--check-only", action="store_true", help="do the checks, build nothing")
    ap.add_argument("--want", type=int, default=8)
    ap.add_argument("--min", type=int, default=2)
    ap.add_argument("--ignore-space", action="store_true", help="run although the free-space check fails")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    log = Log()
    if find_root() is None:
        print("run this script from the archival-structures root (pyproject.toml not found here)",
              file=sys.stderr)
        return 2
    sides = [s for s in SIDES if args.only in (None, s)]
    srcs = {"hand": args.hand_src, "print": args.print_src}
    subset_files = {"hand": args.hand_inventories_file, "print": args.print_inventories_file}

    # Checks: directories.
    errors = []
    for side in sides:
        if not srcs[side]:
            errors.append(f"no --{side}-src given")
        errors += [f"--{side}-src {d} does not exist" for d in srcs[side] if not Path(d).is_dir()]
        if subset_files[side] and not Path(subset_files[side]).is_file():
            errors.append(f"{subset_files[side]} does not exist")
    if args.republic_dir and not args.republic_dir.is_dir():
        errors.append(f"--republic-dir {args.republic_dir} does not exist")
    if not args.out_dir.is_dir():
        errors.append(f"--out-dir {args.out_dir} does not exist")
    else:
        probe = args.out_dir / ".write_test_v3"
        try:
            probe.write_text("x")
            probe.unlink()
        except OSError as exc:
            errors.append(f"--out-dir {args.out_dir} is not writable: {exc}")
    if errors:
        for msg in errors:
            print("ERROR:", msg, file=sys.stderr)
        return 2
    log.path = args.out_dir / "run_v3.log"
    log(f"start: sides={','.join(sides)} limit={args.limit} check_only={args.check_only} "
        f"want={args.want} min={args.min} out={args.out_dir}")

    # Link, count, predict.
    total_out, plan = 0.0, {}
    for side in sides:
        links = link_side(side, srcs[side], args.work_dir, log)
        subset = read_subset(subset_files[side]) if subset_files[side] else None
        if subset is not None:
            missing = [i for i in subset if i not in links]
            if missing:
                log(f"ERROR [{side}] inventories in {subset_files[side]} not found in the sources: "
                    f"{' '.join(missing)}")
                return 2
        chosen = select(links, subset, args.limit)
        sizes = [xml_bytes(links[i]) for i in chosen]
        out_bytes, low, high = predict(side, sizes, args.want)
        total_out += out_bytes
        plan[side] = chosen
        log(f"[{side}] {len(links)} inventories found, {len(chosen)} to build; input about "
            f"{sum(sizes) / MIB:,.0f} MiB of XML; predicted output {out_bytes / MIB:,.0f} MiB; predicted wall "
            f"time {span(low, high)} with {args.want} workers (rough: pilot ratios)")
        if side == "print":
            if args.republic_dir:
                gone = republic_missing(chosen, args.republic_dir)
                log(f"[print] REPUBLIC paragraphs for {len(chosen) - len(gone)}/{len(chosen)} inventories"
                    + (f"; WARNING no crosswalk for {' '.join(gone)}" if gone else ""))
            else:
                log("[print] WARNING no --republic-dir: print builds have no crosswalk")
    free = shutil.disk_usage(args.out_dir).free
    ok = free >= SPACE_FACTOR * total_out
    log(f"free space {free / MIB:,.0f} MiB; need {SPACE_FACTOR} x {total_out / MIB:,.0f} MiB: "
        f"{'ok' if ok else 'TOO LOW'}")
    if not ok and not args.ignore_space:
        log("stop: not enough free space (--ignore-space overrides)")
        return 2
    if args.check_only:
        log("check-only: nothing built")
        return 0

    # Build.
    for side in sides:
        estimate, real = build_cmds(side, args, args.work_dir, args.out_dir, subset_files[side])
        log(f"[{side}] estimate: {' '.join(estimate)}")
        rc = run_cmd(estimate, log)
        if rc:
            log(f"[{side}] WARNING --estimate exited {rc}; continuing")
        log(f"[{side}] build: {' '.join(real)}")
        rc = run_cmd(real, log)
        if rc:
            log(f"[{side}] build exited {rc}; log: {log.path}; rerun the same command to resume")
            return rc

    # Audit and summary.
    if set(sides) == set(SIDES):
        audit_json = args.out_dir / "audit.json"
        rc = run_cmd([".venv/bin/python", "-I", AUDIT, "--layer", str(args.out_dir), "--out", str(audit_json)],
                     log)
        if audit_json.is_file():
            res = json.loads(audit_json.read_text())
            log(f"audit: {res['n_pass']} pass, {res['n_fail']} fail (pilot invariants can fail) -> {audit_json}")
        else:
            log(f"audit: exited {rc}, no audit.json (the audit is pilot-specific, see the docstring)")
    else:
        log("audit skipped: it needs both sides")
    for side in sides:
        done, bad = side_summary(args.out_dir, side)
        log(f"summary [{side}]: {done} inventories done, {bad} failed or unfinished rows, output "
            f"{tree_bytes(Path(args.out_dir) / side) / MIB:,.0f} MiB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
