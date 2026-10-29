#!/usr/bin/env python
"""
Stage the files that a review needs from a full `text_layer_v4` run (`scripts/run_text_layer_v4_full.py`)
into a local dir. The run's out dir is usually on an external drive, so the user runs this, from the AS root:

    .venv/bin/python scripts/stage_text_layer_v4_full.py --from <out-dir> --to data/text_layer_v4_full --dry-run
    .venv/bin/python scripts/stage_text_layer_v4_full.py --from <out-dir> --to data/text_layer_v4_full

It copies the following, and skips a file whose size and mtime already match:
- `<out-dir>/audit.json` and `run_v4.log`;
- for each sub-layer `<archive>/NL-HaNA_*_text_layer_v*`:
  - `status.tsv`, `manifest.json`, `progress.log` and every `*/params.json`;
  - `mode/`, `scans/`, `extract_stats/`, `split_model/`, `split_stats/`;
  - `wbc/` without `wbc/parts/`;
- every per-inventory file (`<dir>/<inv>.*`, at any depth) of two groups of inventories:
  - those named in a failing audit invariant that is judged, i.e. not `reported`. A tag is `[INV]` (every
    archive) or `[ARCHIVE/INV]` (that archive's layer only, the audit format since step 15yr);
  - those given with `--inventories`, each `INV` (every archive) or `ARCHIVE/INV`. The default is the six
    inventories that had a `parse_error` scan in the first run: 1, 9302, 11102, 11103, 11191, 11265.

It then mirrors deletions (step 15yr): a file in the destination that has no source counterpart and is
(1) under a mirrored directory (`mode/`, `scans/`, `extract_stats/`, `split_model/`, `split_stats/`,
`wbc/` without `wbc/parts/`), or (2) a per-inventory file of one of that layer's wanted inventories, is
deleted. Nothing else is: not `audit.json`, `run_v4.log`, `status.tsv`, `manifest.json`, `progress.log`,
`*/params.json`, nor files of other inventories. If more than `--max-deletions` files (default 100) are
scheduled, it prints the count and the first 20, deletes nothing and exits with code 1. With `--dry-run`
it prints `would delete` and deletes nothing.

There are at most `--max-inventories` of these; the rest are listed but not copied. It prints the failing
invariants, what it copies and the total size. It never writes under `--from`. Created files and dirs get
group write permission.
"""

import argparse
import json
import os
import re
import shutil
import stat
import sys
from pathlib import Path

ROOT_FILES = ("audit.json", "run_v4.log")
LAYER_FILES = ("status.tsv", "manifest.json", "progress.log")
LAYER_DIRS = ("mode", "scans", "extract_stats", "split_model", "split_stats", "wbc")
SKIP_DIRS = {Path("wbc/parts")}
PARSE_ERROR_INVS = ("1", "9302", "11102", "11103", "11191", "11265")
TAG_RE = re.compile(r"\[(?:([0-9.]+)/)?([0-9]+[A-Z]?)\]")


def same(src, dst):
    if not dst.is_file():
        return False
    a, b = src.stat(), dst.stat()
    return a.st_size == b.st_size and int(a.st_mtime) == int(b.st_mtime)


def make_group_writable(path, stop):
    while path != stop and stop in path.parents:
        mode = path.stat().st_mode
        if not mode & stat.S_IWGRP:
            path.chmod(mode | stat.S_IWGRP)
        path = path.parent


class Stager:
    def __init__(self, src_root, dst_root, dry_run):
        self.src_root, self.dst_root, self.dry_run = src_root, dst_root, dry_run
        self.n_copy = self.n_skip = self.bytes = 0
        self.seen = set()   # destination paths that have a source counterpart

    def file(self, src):
        dst = self.dst_root / src.relative_to(self.src_root)
        self.seen.add(dst)
        if same(src, dst):
            self.n_skip += 1
            return
        self.n_copy += 1
        self.bytes += src.stat().st_size
        if self.dry_run:
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        make_group_writable(dst, self.dst_root)

    def tree(self, src_dir, layer):
        for dirpath, dirnames, filenames in os.walk(src_dir):
            rel = Path(dirpath).relative_to(layer)
            dirnames[:] = [d for d in dirnames if rel / d not in SKIP_DIRS and not d.startswith(".")]
            for name in filenames:
                if not name.startswith("."):
                    self.file(Path(dirpath) / name)


def scheduled_deletions(stager, layer, wanted):
    """Destination files of ``layer`` with no source counterpart: under a mirrored dir, or per-inventory
    files of ``wanted`` inventories (at any depth). Dot-files and ``SKIP_DIRS`` are left alone."""
    dst_layer = stager.dst_root / layer.relative_to(stager.src_root)
    found = set()

    def walk(top, test):
        for dirpath, dirnames, filenames in os.walk(top):
            rel = Path(dirpath).relative_to(dst_layer)
            dirnames[:] = [d for d in dirnames if rel / d not in SKIP_DIRS and not d.startswith(".")]
            for name in filenames:
                path = Path(dirpath) / name
                if not name.startswith(".") and path not in stager.seen and test(name):
                    found.add(path)

    for d in LAYER_DIRS:
        if (dst_layer / d).is_dir():
            walk(dst_layer / d, lambda name: True)
    if wanted and dst_layer.is_dir():
        walk(dst_layer, lambda name: "." in name and name.split(".", 1)[0] in wanted)
    return sorted(found)


def failing_inventories(audit):
    """[(archive or None, inv)] named in judged failures, in order of appearance; prints every failing
    invariant."""
    invs = []
    fails = [i for i in audit["invariants"] if not i["pass"]]
    print(f"audit: {audit['n_pass']} pass, {audit['n_fail']} fail, {audit['n_fail_judged']} judged, "
          f"{audit['n_inventories']} inventories")
    for item in fails:
        kind = "reported" if item["reported"] else "JUDGED"
        print(f"  {kind}: {item['invariant']} | expected {item['expected']} | actual {str(item['actual'])[:200]}"
              + (f" | {item['note']}" if item["note"] else ""))
        if not item["reported"]:
            invs += [(a or None, i) for a, i in TAG_RE.findall(item["invariant"]) if (a or None, i) not in invs]
    return invs


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from", dest="src", type=Path, required=True, help="the full run's --out-dir")
    ap.add_argument("--to", dest="dst", type=Path, default=Path("data/text_layer_v4_full"))
    ap.add_argument("--inventories", nargs="*", default=list(PARSE_ERROR_INVS))
    ap.add_argument("--max-inventories", type=int, default=20)
    ap.add_argument("--max-deletions", type=int, default=100,
                    help="refuse to delete (exit 1) when more files than this are scheduled")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    src, dst = args.src.resolve(), args.dst.resolve()
    if dst == src or src in dst.parents:
        ap.error("--to must not lie under --from")
    if not args.dry_run:
        if not dst.exists():
            dst.mkdir(parents=True)
            dst.chmod(dst.stat().st_mode | stat.S_IWGRP)
        if not os.access(dst, os.W_OK):
            ap.error(f"no write access to {dst}")
    S = Stager(src, dst, args.dry_run)

    for name in ROOT_FILES:
        if (src / name).is_file():
            S.file(src / name)
        else:
            print(f"missing: {src / name}")
    invs = []
    if (src / "audit.json").is_file():
        invs = failing_inventories(json.loads((src / "audit.json").read_text()))
    for item in args.inventories:
        archive, _, inv = item.rpartition("/")
        if (archive or None, inv) not in invs:
            invs.append((archive or None, inv))
    if len(invs) > args.max_inventories:
        print(f"{len(invs)} inventories; copying the first {args.max_inventories}, not: {invs[args.max_inventories:]}")
        invs = invs[:args.max_inventories]
    print("per-inventory files for:", " ".join(f"{a}/{i}" if a else i for a, i in invs) or "none")

    deletions = []
    for layer in sorted(src.glob("*/NL-HaNA_*_text_layer_v*")):
        if not layer.is_dir():
            continue
        wanted = {i for a, i in invs if a is None or a == layer.parent.name}
        for name in LAYER_FILES:
            if (layer / name).is_file():
                S.file(layer / name)
        for p in sorted(layer.glob("*/params.json")):
            S.file(p)
        for d in LAYER_DIRS:
            if (layer / d).is_dir():
                S.tree(layer / d, layer)
        if wanted:  # one walk over the layer (thousands of inventories), not one per inventory
            for dirpath, dirnames, filenames in os.walk(layer):
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                for name in filenames:
                    if name.split(".", 1)[0] in wanted and "." in name:
                        S.file(Path(dirpath) / name)
        deletions += scheduled_deletions(S, layer, wanted)
    if len(deletions) > args.max_deletions:
        print(f"{len(deletions)} stale files scheduled for deletion, more than --max-deletions "
              f"{args.max_deletions}; deleting nothing. First 20:")
        for path in deletions[:20]:
            print("  ", path)
        return 1
    for path in deletions:
        if args.dry_run:
            print("would delete", path)
        else:
            path.unlink()
    verb = "would copy" if args.dry_run else "copied"
    dverb = "would delete" if args.dry_run else "deleted"
    print(f"{verb} {S.n_copy} files ({S.bytes / 1e6:.1f} MB); {S.n_skip} already equal; {dverb} {len(deletions)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
