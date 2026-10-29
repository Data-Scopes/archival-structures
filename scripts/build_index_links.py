"""Build the derived layer ``NL-HaNA_1.01.02_index_links_v1`` (index entry -> folio -> scan links) from a v5 text layer.

Usage (from the AS root)::

    .venv/bin/python scripts/build_index_links.py --layer <v5 layer root> --out-dir <existing dir> \\
        [--inventories-file FILE] [--limit N]

Runs with more than one worker go through ``coreshare.py run ... --workers '{workers}'`` (project rule).

Inputs read (v5 layer root = the directory holding ``layout/``, ``units/``, ...): ``manifest.json``,
``scans/<inv>.scans.tsv.gz``, ``layout/pages/<inv>.tsv.gz``, ``lines/<inv>.lines.tsv.gz``,
``types/segments/<inv>.tsv.gz``, ``units/units/<inv>.tsv.gz``, ``join/units/<inv>.tsv.gz``.
Nothing in the layer is modified. Default inventories: every one with ``layout/pages/<inv>.tsv.gz``
(``--inventories-file``: one inventory number per line instead). ``--limit N`` stops after N inventories
(smoke test).

Outputs under ``<out-dir>/NL-HaNA_1.01.02_index_links_v1/``: ``folio_map/<inv>.tsv.gz``, ``links/<inv>.tsv.gz``,
``meta/<inv>.json`` (map summary, for rebuilding ``volumes.tsv``), ``volumes.tsv`` (one row per volume with a front
index), ``manifest.json``. Inventories without a front index get no files. Resumable: an inventory whose three
files exist is skipped; ``volumes.tsv`` and the manifest are rebuilt from the per-inventory files at the end.
A volume that raises is recorded (outcome ``failed``, message in the manifest's ``last_run.failed``), gets no
files (a rerun retries it) and the run goes on; the exit code is 1 if any failed. Every finished volume appends
a line ``<UTC time>\t<inv>\t<outcome>\t<message>`` to ``<out>/progress.log``.
The locator variant and its parameters are those of ``archival_structures.analysis.index_links``.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from archival_structures.analysis.index_links import (  # noqa: E402
    DEFAULT_LOCATOR_VARIANT, LINK_COLUMNS, SG_LOCATOR_PARAMS, read_volume_inputs, volume_links)
from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz  # noqa: E402
from archival_structures.text_layer.provenance import package_info  # noqa: E402

LAYER_NAME = "NL-HaNA_1.01.02_index_links_v1"
MAP_COLUMNS = ["number", "front_scan_id", "front_page", "back_scan_id", "back_page", "position", "method",
               "anchor_dist", "confidence"]
VOLUME_COLUMNS = ("inv mode n_anchors recto_share chain_len chain_first chain_last front_first front_last end "
                  "n_entries n_locators n_linked").split()


def _paths(out: Path, inv: str):
    return (out / "folio_map" / f"{inv}.tsv.gz", out / "links" / f"{inv}.tsv.gz", out / "meta" / f"{inv}.json")


def process(layer: str, out: str, inv: str, variant: str) -> str:
    layer_p, out_p = Path(layer), Path(out)
    fm, lk, meta = _paths(out_p, inv)
    if fm.exists() and lk.exists() and meta.exists():
        return "skipped"
    inputs = read_volume_inputs(layer_p, inv)
    units = list(read_tsv_gz(layer_p / "units" / "units" / f"{inv}.tsv.gz"))
    texts = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(layer_p / "join" / "units" / f"{inv}.tsv.gz")}
    res = volume_links(inputs, units, texts, SG_LOCATOR_PARAMS[variant])
    if res is None:
        return "no_front_index"
    m, ch = res["map"], res["map"].chain
    first, last, end = res["front"]
    n_loc = len(res["links"])
    n_linked = sum(1 for r in res["links"] if r["method"] != "none")
    info = dict(inv=inv, mode=m.mode, n_anchors=m.n_anchors,
                recto_share="" if m.recto_share != m.recto_share else f"{m.recto_share:.4f}",
                chain_len=len(ch), chain_first=int(ch[0, 1]) if len(ch) else "",
                chain_last=int(ch[-1, 1]) if len(ch) else "", front_first=first, front_last=last, end=end,
                n_entries=len(res["entries"]), n_locators=n_loc, n_linked=n_linked)
    write_tsv_gz(fm, MAP_COLUMNS, [[r[c] for c in MAP_COLUMNS] for r in m.table()])
    write_tsv_gz(lk, LINK_COLUMNS, [[r[c] for c in LINK_COLUMNS] for r in res["links"]])
    meta.parent.mkdir(parents=True, exist_ok=True)
    meta.write_text(json.dumps(info))
    return "done"


def run_one(layer: str, out: str, inv: str, variant: str) -> tuple[str, str, str]:
    try:
        return inv, process(layer, out, inv, variant), ""
    except Exception as e:  # noqa: BLE001 - a failing volume must not stop the run
        return inv, "failed", f"{type(e).__name__}: {e}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--layer", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--inventories-file", type=Path)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--variant", default=DEFAULT_LOCATOR_VARIANT)
    a = ap.parse_args(argv)
    if not a.out_dir.is_dir():
        ap.error(f"--out-dir must exist: {a.out_dir}")
    if a.inventories_file:
        invs = [x.strip() for x in a.inventories_file.read_text().split() if x.strip()]
    else:
        invs = sorted(p.name.split(".")[0] for p in (a.layer / "layout" / "pages").glob("*.tsv.gz"))
    invs = sorted(invs, key=lambda s: (len(s), s))
    if a.limit:
        invs = invs[:a.limit]
    out = a.out_dir / LAYER_NAME
    out.mkdir(exist_ok=True)
    t0 = time.time()
    counts: dict = {}
    failed: dict = {}
    log = out / "progress.log"

    def record(res):
        inv, outcome, msg = res
        counts[outcome] = counts.get(outcome, 0) + 1
        if outcome == "failed":
            failed[inv] = msg
        print(inv, outcome, msg, flush=True)
        stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with open(log, "a") as lf:
            lf.write(f"{stamp}\t{inv}\t{outcome}\t{msg}\n")

    if a.workers > 1:
        with ProcessPoolExecutor(a.workers) as ex:
            futs = [ex.submit(run_one, str(a.layer), str(out), inv, a.variant) for inv in invs]
            for f in as_completed(futs):
                record(f.result())
    else:
        for inv in invs:
            record(run_one(str(a.layer), str(out), inv, a.variant))
    elapsed = time.time() - t0
    # rebuild volumes.tsv and the manifest from the per-inventory files
    metas = [json.loads(p.read_text()) for p in sorted((out / "meta").glob("*.json"))] if (out / "meta").exists() else []
    metas.sort(key=lambda d: (len(d["inv"]), d["inv"]))
    with open(out / "volumes.tsv", "w") as f:
        f.write("\t".join(VOLUME_COLUMNS) + "\n")
        for d in metas:
            f.write("\t".join(str(d[c]) for c in VOLUME_COLUMNS) + "\n")
    try:
        src = json.loads((a.layer / "manifest.json").read_text()).get("layer")
    except OSError:
        src = None
    params = dataclasses.asdict(SG_LOCATOR_PARAMS[a.variant])
    manifest = dict(layer=LAYER_NAME, created_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    source_layer_path=str(a.layer), source_layer=src,
                    package=package_info("archival-structures", "archival_structures"),
                    variant=a.variant, locator_params=params,
                    counts=dict(volumes=len(metas), n_entries=sum(d["n_entries"] for d in metas),
                                n_locators=sum(d["n_locators"] for d in metas),
                                n_linked=sum(d["n_linked"] for d in metas)),
                    last_run=dict(inventories_considered=len(invs), outcomes=counts, failed=failed,
                                  seconds=round(elapsed, 1)))
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(json.dumps(manifest["counts"]), counts, f"{elapsed:.1f}s")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
