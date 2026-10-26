#!/usr/bin/env python
"""
Hand-off script for the full `text_layer_v4` run (phase 12, step 15w).

Builds the SG text layer, v4, from PageXML that may live on an external drive. One input dir per
archive is assembled from symlinks, the inputs and the free space are checked, the build runs
through `coreshare` and the finished layer is audited. Run it from the AS root, with the AS
virtualenv (`.venv/bin/python scripts/run_text_layer_v4_full.py ...`). It never writes into a
`--src` dir.

v4 decides hand or print per volume: the build measures the line-box height of the volume's own
PageXML (cut 87.75 px, raw pixels, as in step 15u) in a pre-pass of the extract stage, picks the
hand or the print rules for the layout, types and units stages, and records the decision under
`<layer>/mode/` (`mode/<inv>.json`: mode, method, value, margin; `mode/scans/`, `mode/regions/`
for the error analysis). Inventories of 1.10.94 are fixed `print`; the measured value is recorded
next to it.

What to put on the drive (any number of dirs per archive; names must match exactly):
  1.01.02:  NL-HaNA_1.01.02_<inv>-pagexml.tar.gz  (also .zip, or a dir NL-HaNA_1.01.02_<inv>)
  1.10.94:  NL-HaNA_1.10.94_<inv>.zip             (also .tar.gz, or a dir NL-HaNA_1.10.94_<inv>)
`<inv>` is the inventory number with an optional capital letter, e.g. 3349, 11182, 9626A. Dot-files
and names that do not match are ignored. When several sources hold the same inventory of an
archive, the first `--src` for it wins and the duplicate is logged. An archive without any `--src`
is not built (give `--only` to be explicit).
Optional: `paragraphs-<inv>.tsv.gz` (REPUBLIC) in `--republic-dir 1.10.94=DIR`, for the line-id
crosswalk of 1.10.94 (without it the units stage runs without a crosswalk: warned about).
For 1.01.02 no REPUBLIC crosswalk is used (it matches 0.5% of the lines, F12-15v.5): leave
`--republic-dir 1.01.02=...` out. The units stage then logs one REPUBLIC warning for the print-mode
1.01.02 inventories: expected.

Order of work (placeholders: <1.01.02-dir> holds every NL-HaNA_1.01.02_<inv>-pagexml.tar.gz, hand
and print mixed; <drive> is the output mount point; the 1.10.94 inputs are local):
  1. check only, builds nothing (counts, bytes, predicted size and time, free space):
       .venv/bin/python scripts/run_text_layer_v4_full.py --check-only \\
           --src 1.01.02=<1.01.02-dir> \\
           --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml \\
           --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 \\
           --out-dir <drive>/layer
  2. smoke run of one inventory per archive, into a separate out dir:
       ... same arguments, with --limit 1 --out-dir <drive>/layer_smoke
  3. the full run: the arguments of step 1 without --check-only.
`--src` and `--republic-dir` can be given several times (`ARCHIVE=DIR`). `--only 1.01.02|1.10.94` runs
one archive; `--inventories-file ARCHIVE=F` (one inventory per line) restricts an archive;
`--want N --min M` is the coreshare request (default 8 and 2; the build uses the granted count).

After an interruption (Ctrl-C, a crash, a full disk, a pulled cable) rerun the same command: every
stage marks each inventory done in `<out-dir>/<archive>/*/status.tsv` and a rerun skips what is done.

Logs: `<out-dir>/run_v4.log` (this script's lines with UTC timestamps and the build output),
`<out-dir>/<archive>/*/progress.log` (the build's own log), `<out-dir>/audit.json` (the audit).

The audit (`reports/data/phase-12-hand-print-box-height/step-15w/audit.py`) discovers the layers
under `--out-dir` (`*/NL-HaNA_*_text_layer_v*`), the inventories from each `status.tsv` and the
sources from each `manifest.json`. It checks the phase-09 invariants (tables, ids, offsets, boxes,
value sets) and, for a layer with `mode/`, the v4 ones: a mode file per inventory (`box_height`:
`print` iff value <= cut), scan and region tables, a `split_model/` iff `print` and `split_gate.split` is not false, no split lines in a
hand or gated inventory, `split_gate.split` equal to share >= min_share, unit methods `print:*` only in print and `hand:*` only in hand inventories, and
`hand` and `print` parameter sets in `layout/`, `types/` and `units/params.json`. The share of
merges with a lower-case right side is reported, not judged. The script logs the audit outcome and
does not fail on it.

Predictions are rough. Both are per uncompressed XML byte (an archive counts as its size times
the compression ratio 4.2). Output size is XML bytes times the per-archive output ratio. Wall time
is a range: the lower end is max(worker-seconds / --want, the largest single inventory), the upper
end 3 times that (stage barriers, one task per inventory). The constants were measured on the dev
build `15w-a` (see the comment at the constants); the inventory mix of the full run differs, and a
USB drive may be slower than the local disk.
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
AUDIT = "reports/data/phase-12-hand-print-box-height/step-15w/audit.py"
ARCHIVES = {"1.01.02": "sg_1.01.02_v4", "1.10.94": "sg_1.10.94_v4"}
SPACE_FACTOR = 1.2
MIB = 2 ** 20

# Constants measured on `data/text_layer_v2_dev/15w-a` (196 volumes of 1.01.02, 2 of 1.10.94; report step 15w),
# from status.tsv: XML bytes = sum of `input_xml_bytes` of the extract rows (1.01.02 25,297,913,805; 1.10.94
# 1,009,177,478); output = `du -sk` (1.01.02 2,753,304 KiB; 1.10.94 87,496 KiB); worker-seconds = sum of `seconds`
# over all stages, the extract stage including the mode pre-pass (1.01.02 7,418.6; 1.10.94 471.9, 2 workers).
# Rates in 1.01.02 by mode: hand 1.395e-7 worker-s per XML byte (111 volumes, 10.5 GiB), print 4.173e-7 (85 volumes,
# 13.0 GiB: the merged-line split and the print unit rules cost about 3 times as much). The dev mix is 52% print by
# bytes; the full 1.01.02 run is mostly hand (silver: 4.6% of scans print), so the constant assumes 10% print:
# 0.9 * 1.395e-7 + 0.1 * 4.173e-7. Output per XML byte: hand 0.107, print 0.115, archive 0.1115 (1.01.02), 0.0888 (1.10.94).
# Wall time of that run (2 workers): 1.01.02 5,340 s = 1.4 x its lower bound; 1.10.94 about 2 x (SLOW_FACTOR = 3 is conservative).
# Step 15wr (split gate): re-measured on `data/text_layer_v2_dev/15wr-a`: 53 of the 85 print volumes are no longer split, so
# the 1.01.02 print rate falls from 4.173e-7 to 2.402e-7 worker-s per XML byte (hand 1.44e-7, within noise of 1.395e-7);
# 1.01.02 worker-seconds 4,990 (was 7,419). Output 1.01.02 2,647,572 KiB = 0.1072 per XML byte; 1.10.94 86,036 KiB (0.0873).
# The constant is now 0.9 * 1.395e-7 + 0.1 * 2.402e-7.
# The 182 staged tars hold 4.31 XML bytes per archive byte (v3 pilot: 4.16, hand tars 4.175, print zips 4.148).
ARCHIVE_XML_RATIO = 4.2
OUTPUT_PER_XML_BYTE = {"1.01.02": 0.1072, "1.10.94": 0.0888}
WORKER_SECONDS_PER_XML_BYTE = {"1.01.02": 0.9 * 1.395e-7 + 0.1 * 2.402e-7, "1.10.94": 4.676e-7}
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


def link_archive(archive, srcs, work_dir, log):
    """(Re)create `<work_dir>/<archive>/` with one symlink per inventory; the first source wins.
    Returns {inv: link path} in inventory order."""
    pattern = PRESETS[ARCHIVES[archive]].source_pattern
    target = Path(work_dir) / archive
    target.mkdir(parents=True, exist_ok=True)
    mode = target.stat().st_mode
    if not mode & 0o020:  # only the owner may chmod; a dir another user made is often group-writable already
        try:
            os.chmod(target, mode | 0o020)
        except PermissionError:
            log(f"[{archive}] cannot make {target} group-writable (not its owner); continuing")
    for old in target.iterdir():
        if old.is_symlink():
            old.unlink()
        else:
            raise SystemExit(f"{old} in the work dir is not a link; remove it or use another --work-dir")
    chosen = {}      # inv -> source entry
    for src in srcs:
        for inv, entry in discover_inventories_v2(src, pattern).items():
            if inv in chosen:
                log(f"[{archive}] inventory {inv} also in {entry} (ignored); used {chosen[inv]}")
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


def predict(archive, xml_sizes, want):
    """(output bytes, lower and upper wall seconds) for a list of per-inventory XML byte counts."""
    rate = WORKER_SECONDS_PER_XML_BYTE[archive]
    low = max(sum(xml_sizes) * rate / max(want, 1), max(xml_sizes, default=0) * rate)
    return sum(xml_sizes) * OUTPUT_PER_XML_BYTE[archive], low, SLOW_FACTOR * low


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


def archive_summary(out_dir, archive):
    """(inventories with every stage done, failed or unfinished rows) from the status.tsv files."""
    done, bad = {}, 0
    for status in (Path(out_dir) / archive).glob("*/status.tsv"):
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


def build_cmds(archive, args, work_dir, out_dir, subset_file):
    common = [BUILD, "--pagexml-dir", str(Path(work_dir) / archive), "--out-dir", str(Path(out_dir) / archive),
              "--preset", ARCHIVES[archive], "--stage", "all"]
    if subset_file:
        common += ["--inventories-file", str(subset_file)]
    if args.limit:
        common += ["--limit", str(args.limit)]
    estimate = common + ["--estimate", "--workers", str(args.want)]
    real = common[:]
    if args.republic_dir.get(archive):
        real += ["--republic-dir", str(args.republic_dir[archive])]
    coreshare = ["python3", CORESHARE, "run", "--project", "archival-structures", "--label", f"v4-{archive}",
                 "--want", str(args.want), "--min", str(args.min), "--wait", "--"]
    return estimate, coreshare + real + ["--workers", "{workers}"]


def archive_pair(text):
    """`ARCHIVE=VALUE` argument; the archive must be one of ARCHIVES."""
    archive, sep, value = text.partition("=")
    if not sep or archive not in ARCHIVES or not value:
        raise argparse.ArgumentTypeError(f"expected ARCHIVE=VALUE with ARCHIVE in {sorted(ARCHIVES)}, got {text!r}")
    return archive, Path(value)


def parse_args(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--src", action="append", default=[], type=archive_pair, metavar="ARCHIVE=DIR",
                    help="source dir of an archive (1.01.02 or 1.10.94); repeatable, first has priority")
    ap.add_argument("--republic-dir", action="append", default=[], type=archive_pair, metavar="ARCHIVE=DIR",
                    help="dir with paragraphs-<inv>.tsv.gz for the archive (1.10.94 only, see the docstring)")
    ap.add_argument("--inventories-file", action="append", default=[], type=archive_pair, metavar="ARCHIVE=F",
                    help="file with one inventory per line, restricts the archive")
    ap.add_argument("--out-dir", required=True, type=Path, help="layer base: <out-dir>/<archive>")
    ap.add_argument("--work-dir", type=Path, default=Path("data/text_layer_v4_full_inputs"),
                    help="local dir for the input links (default %(default)s)")
    ap.add_argument("--only", choices=sorted(ARCHIVES), default=None, help="run one archive")
    ap.add_argument("--limit", type=int, default=0, help="only the first N inventories per archive (smoke)")
    ap.add_argument("--check-only", action="store_true", help="do the checks, build nothing")
    ap.add_argument("--want", type=int, default=8)
    ap.add_argument("--min", type=int, default=2)
    ap.add_argument("--ignore-space", action="store_true", help="run although the free-space check fails")
    args = ap.parse_args(argv)
    srcs, republic, subsets = {}, {}, {}
    for archive, path in args.src:
        srcs.setdefault(archive, []).append(path)
    for archive, path in args.republic_dir:
        republic[archive] = path
    for archive, path in args.inventories_file:
        subsets[archive] = path
    args.src, args.republic_dir, args.inventories_file = srcs, republic, subsets
    return args


def main(argv=None):
    args = parse_args(argv)
    log = Log()
    if find_root() is None:
        print("run this script from the archival-structures root (pyproject.toml not found here)",
              file=sys.stderr)
        return 2
    archives = [a for a in ARCHIVES if args.only in (None, a) and (args.only or a in args.src)]
    srcs, subset_files = args.src, args.inventories_file

    # Checks: directories.
    errors = []
    if not archives:
        errors.append("no --src given")
    for archive in archives:
        if not srcs.get(archive):
            errors.append(f"no --src {archive}=DIR given")
        errors += [f"--src {archive}={d} does not exist" for d in srcs.get(archive, []) if not Path(d).is_dir()]
        if subset_files.get(archive) and not Path(subset_files[archive]).is_file():
            errors.append(f"{subset_files[archive]} does not exist")
    for archive, path in args.republic_dir.items():
        if not path.is_dir():
            errors.append(f"--republic-dir {archive}={path} does not exist")
    if not args.out_dir.is_dir():
        errors.append(f"--out-dir {args.out_dir} does not exist")
    else:
        probe = args.out_dir / ".write_test_v4"
        try:
            probe.write_text("x")
            probe.unlink()
        except OSError as exc:
            errors.append(f"--out-dir {args.out_dir} is not writable: {exc}")
    if errors:
        for msg in errors:
            print("ERROR:", msg, file=sys.stderr)
        return 2
    log.path = args.out_dir / "run_v4.log"
    log(f"start: archives={','.join(archives)} limit={args.limit} check_only={args.check_only} "
        f"want={args.want} min={args.min} out={args.out_dir}")

    # Link, count, predict.
    total_out, plan = 0.0, {}
    for archive in archives:
        links = link_archive(archive, srcs[archive], args.work_dir, log)
        subset = read_subset(subset_files[archive]) if subset_files.get(archive) else None
        if subset is not None:
            missing = [i for i in subset if i not in links]
            if missing:
                log(f"ERROR [{archive}] inventories in {subset_files[archive]} not found in the sources: "
                    f"{' '.join(missing)}")
                return 2
        chosen = select(links, subset, args.limit)
        sizes = [xml_bytes(links[i]) for i in chosen]
        out_bytes, low, high = predict(archive, sizes, args.want)
        total_out += out_bytes
        plan[archive] = chosen
        log(f"[{archive}] {len(links)} inventories found, {len(chosen)} to build; input about "
            f"{sum(sizes) / MIB:,.0f} MiB of XML; predicted output {out_bytes / MIB:,.0f} MiB; predicted wall "
            f"time {span(low, high)} with {args.want} workers (rough: dev-build ratios)")
        if archive == "1.10.94":      # fixed print: every inventory wants its REPUBLIC paragraphs
            if args.republic_dir.get(archive):
                gone = republic_missing(chosen, args.republic_dir[archive])
                log(f"[{archive}] REPUBLIC paragraphs for {len(chosen) - len(gone)}/{len(chosen)} inventories"
                    + (f"; WARNING no crosswalk for {' '.join(gone)}" if gone else ""))
            else:
                log(f"[{archive}] WARNING no --republic-dir {archive}=DIR: print builds have no crosswalk")
        elif args.republic_dir.get(archive):
            log(f"[{archive}] note: --republic-dir only matters for the print-mode inventories "
                f"(not checked here); the 1.01.02 crosswalk matches 0.5% of the lines (F12-15v.5)")
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
    for archive in archives:
        estimate, real = build_cmds(archive, args, args.work_dir, args.out_dir, subset_files.get(archive))
        log(f"[{archive}] estimate: {' '.join(estimate)}")
        rc = run_cmd(estimate, log)
        if rc:
            log(f"[{archive}] WARNING --estimate exited {rc}; continuing")
        log(f"[{archive}] build: {' '.join(real)}")
        rc = run_cmd(real, log)
        if rc:
            log(f"[{archive}] build exited {rc}; log: {log.path}; rerun the same command to resume")
            return rc

    # Audit and summary.
    audit_json = args.out_dir / "audit.json"
    rc = run_cmd([".venv/bin/python", "-I", AUDIT, "--layer", str(args.out_dir), "--out", str(audit_json)], log)
    if audit_json.is_file():
        res = json.loads(audit_json.read_text())
        log(f"audit: {res['n_pass']} pass, {res['n_fail']} fail ({res.get('n_fail_judged', '?')} judged; "
            f"reported shares excluded) -> {audit_json}")
    else:
        log(f"audit: exited {rc}, no audit.json")
    for archive in archives:
        done, bad = archive_summary(args.out_dir, archive)
        log(f"summary [{archive}]: {done} inventories done, {bad} failed or unfinished rows, output "
            f"{tree_bytes(Path(args.out_dir) / archive) / MIB:,.0f} MiB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
