"""Build the derived layer ``NL-HaNA_1.01.02_index_links_v1`` / ``_v2`` (index entry -> folio -> scan links) from a v5 text layer.

Usage (from the AS root)::

    .venv/bin/python scripts/build_index_links.py --layer <v5 layer root> --out-dir <existing dir> \\
        [--inventories-file FILE] [--limit N] [--version {v1,v2}]

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

``--version v2`` (step 21d; default v1, which writes exactly what it always wrote) builds
``<out-dir>/NL-HaNA_1.01.02_index_links_v2/`` with ``V2_CONFIG`` (folio map v2, method A0, locator variant L7): tables
``folio_map/`` (with opening columns), ``entries/``, ``links/`` (``LINK_COLUMNS_V2``), ``meta/``, ``volumes.tsv`` (v1
columns plus ``n_headings n_dup_dropped chain_cut``) and ``manifest.json`` (version, config, bars). An inventory is
skipped when its 4 files exist. Extra inputs: ``join/segments/<inv>.tsv.gz`` (method A0), or ``lines/``,
``units/segments/`` (methods A1-A3). ``--variant`` is v1 only (an error with v2).
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
    DEFAULT_LOCATOR_VARIANT, ENTRY_COLUMNS, LINK_COLUMNS, LINK_COLUMNS_V2, SG_LOCATOR_PARAMS, V2_CONFIG,
    front_index_run, read_side_lines, read_volume_inputs, volume_links, volume_links_v2)
from archival_structures.text_layer.presets import SG_HAND_V2  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz  # noqa: E402
from archival_structures.text_layer.provenance import package_info  # noqa: E402

LAYER_NAME = "NL-HaNA_1.01.02_index_links_v1"
LAYER_NAME_V2 = "NL-HaNA_1.01.02_index_links_v2"
MAP_COLUMNS_V2 = ["number", "front_scan_id", "front_page", "back_scan_id", "back_page", "opening_scan_id",
                  "opening_page", "position", "method", "anchor_dist", "confidence"]
# bars of steps 21b (folio map) and 21c (locators, attribution, end to end), from the phase-15 report; Wilson 95%
BARS_V2 = {
    "F_folio_on_right_scan_21b": dict(bar=">=0.95", v2="62/63 = 0.984 [0.915, 0.997]", v1="62/63", result="pass"),
    "L_precision_21c": dict(bar=">=0.95", v2="404/412 = 0.981 [0.962, 0.990]", v1="407/421 = 0.967", result="pass"),
    "L_recall_21c": dict(bar=">=0.90", v2="404/472 = 0.856 [0.821, 0.885]", v1="407/472 = 0.862", result="fail"),
    "A_attribution_21c": dict(bar=">=0.90", v2="355/398 = 0.892 [0.858, 0.919]", v1="358/401 = 0.893", result="fail"),
    "E_end_to_end_21c": dict(bar=">=0.90", v2="52/63 = 0.825 [0.714, 0.900]", v1="52/63 = 0.825", result="fail"),
}
MAP_COLUMNS = ["number", "front_scan_id", "front_page", "back_scan_id", "back_page", "position", "method",
               "anchor_dist", "confidence"]
VOLUME_COLUMNS = ("inv mode n_anchors recto_share chain_len chain_first chain_last front_first front_last end "
                  "n_entries n_locators n_linked").split()
VOLUME_COLUMNS_V2 = VOLUME_COLUMNS + ["n_headings", "n_dup_dropped", "chain_cut"]


def _paths(out: Path, inv: str):
    return (out / "folio_map" / f"{inv}.tsv.gz", out / "links" / f"{inv}.tsv.gz", out / "meta" / f"{inv}.json")


def _info(inv: str, res: dict) -> dict:
    m, ch = res["map"], res["map"].chain
    first, last, end = res["front"]
    return dict(inv=inv, mode=m.mode, n_anchors=m.n_anchors,
                recto_share="" if m.recto_share != m.recto_share else f"{m.recto_share:.4f}",
                chain_len=len(ch), chain_first=int(ch[0, 1]) if len(ch) else "",
                chain_last=int(ch[-1, 1]) if len(ch) else "", front_first=first, front_last=last, end=end)


def process(layer: str, out: str, inv: str, variant: str, version: str = "v1") -> str:
    if version == "v2":
        return process_v2(layer, out, inv)
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
    m = res["map"]
    n_loc = len(res["links"])
    n_linked = sum(1 for r in res["links"] if r["method"] != "none")
    info = _info(inv, res)
    info.update(n_entries=len(res["entries"]), n_locators=n_loc, n_linked=n_linked)
    write_tsv_gz(fm, MAP_COLUMNS, [[r[c] for c in MAP_COLUMNS] for r in m.table()])
    write_tsv_gz(lk, LINK_COLUMNS, [[r[c] for c in LINK_COLUMNS] for r in res["links"]])
    meta.parent.mkdir(parents=True, exist_ok=True)
    meta.write_text(json.dumps(info))
    return "done"


def _paths_v2(out: Path, inv: str):
    return (out / "folio_map" / f"{inv}.tsv.gz", out / "entries" / f"{inv}.tsv.gz", out / "links" / f"{inv}.tsv.gz",
            out / "meta" / f"{inv}.json")


def process_v2(layer: str, out: str, inv: str) -> str:
    layer_p, out_p = Path(layer), Path(out)
    fm, en, lk, meta = _paths_v2(out_p, inv)
    if fm.exists() and en.exists() and lk.exists() and meta.exists():
        return "skipped"
    cfg = V2_CONFIG
    inputs = read_volume_inputs(layer_p, inv)
    run = front_index_run(inputs.flags)
    if run is None:
        return "no_front_index"
    front = [s for i, s in enumerate(inputs.sides) if i <= run[1] and inputs.flags[i]]
    units = list(read_tsv_gz(layer_p / "units" / "units" / f"{inv}.tsv.gz"))
    texts, seg_map, side_lines = {}, None, {}
    if cfg["method"] == "A0":
        texts = {(r["scan_id"], r["unit_id"]): r["text"]
                 for r in read_tsv_gz(layer_p / "join" / "units" / f"{inv}.tsv.gz")}
        segs: dict = {}
        for g in read_tsv_gz(layer_p / "join" / "segments" / f"{inv}.tsv.gz"):
            segs.setdefault((g["scan_id"], g["unit_id"]), []).append(g)
        seg_map = {k: [(g["line_id"], int(g["char_start"]), int(g["char_end"]))
                       for g in sorted(v, key=lambda g: (int(g["seg_order"]), int(g["seg_idx"] or 0)))]
                   for k, v in segs.items()}
    else:
        side_lines = read_side_lines(layer_p, inv, front)
    pinned = SG_HAND_V2.wbc_pinned or {}
    wbc = pinned.get(inv) or pinned["default"]
    res = volume_links_v2(inputs, side_lines, units, texts, unit_segments=seg_map, config=cfg, wbc=wbc)
    m = res["map"]
    n_linked = sum(1 for r in res["links"] if r["method"] != "none")
    n_head = sum(1 for e in res["entries"] if e["kind"] == "heading")
    info = _info(inv, res)
    info.update(n_entries=len(res["entries"]) - n_head, n_locators=len(res["links"]), n_linked=n_linked,
                n_headings=n_head, n_dup_dropped=m.n_dup_dropped, chain_cut=int(m.chain_cut))
    write_tsv_gz(fm, MAP_COLUMNS_V2, [[r[c] for c in MAP_COLUMNS_V2] for r in m.table()])
    write_tsv_gz(en, ENTRY_COLUMNS, [[r[c] for c in ENTRY_COLUMNS] for r in res["entries"]])
    write_tsv_gz(lk, LINK_COLUMNS_V2, [[r[c] for c in LINK_COLUMNS_V2] for r in res["links"]])
    meta.parent.mkdir(parents=True, exist_ok=True)
    meta.write_text(json.dumps(info))
    return "done"


def run_one(layer: str, out: str, inv: str, variant: str, version: str = "v1") -> tuple[str, str, str]:
    try:
        return inv, process(layer, out, inv, variant, version), ""
    except Exception as e:  # noqa: BLE001 - a failing volume must not stop the run
        return inv, "failed", f"{type(e).__name__}: {e}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--layer", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--inventories-file", type=Path)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--variant", default=None, help="v1 only (default %s)" % DEFAULT_LOCATOR_VARIANT)
    ap.add_argument("--version", choices=("v1", "v2"), default="v1")
    a = ap.parse_args(argv)
    if a.version == "v2" and a.variant is not None:
        ap.error("--variant is v1 only: v2 takes its locator variant from V2_CONFIG")
    if a.variant is None:
        a.variant = DEFAULT_LOCATOR_VARIANT
    v2 = a.version == "v2"
    layer_name, vol_cols = (LAYER_NAME_V2, VOLUME_COLUMNS_V2) if v2 else (LAYER_NAME, VOLUME_COLUMNS)
    if not a.out_dir.is_dir():
        ap.error(f"--out-dir must exist: {a.out_dir}")
    if a.inventories_file:
        invs = [x.strip() for x in a.inventories_file.read_text().split() if x.strip()]
    else:
        invs = sorted(p.name.split(".")[0] for p in (a.layer / "layout" / "pages").glob("*.tsv.gz"))
    invs = sorted(invs, key=lambda s: (len(s), s))
    if a.limit:
        invs = invs[:a.limit]
    out = a.out_dir / layer_name
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
            futs = [ex.submit(run_one, str(a.layer), str(out), inv, a.variant, a.version) for inv in invs]
            for f in as_completed(futs):
                record(f.result())
    else:
        for inv in invs:
            record(run_one(str(a.layer), str(out), inv, a.variant, a.version))
    elapsed = time.time() - t0
    # rebuild volumes.tsv and the manifest from the per-inventory files
    metas = [json.loads(p.read_text()) for p in sorted((out / "meta").glob("*.json"))] if (out / "meta").exists() else []
    metas.sort(key=lambda d: (len(d["inv"]), d["inv"]))
    with open(out / "volumes.tsv", "w") as f:
        f.write("\t".join(vol_cols) + "\n")
        for d in metas:
            f.write("\t".join(str(d[c]) for c in vol_cols) + "\n")
    try:
        src = json.loads((a.layer / "manifest.json").read_text()).get("layer")
    except OSError:
        src = None
    counts_d = dict(volumes=len(metas), n_entries=sum(d["n_entries"] for d in metas),
                    n_locators=sum(d["n_locators"] for d in metas), n_linked=sum(d["n_linked"] for d in metas))
    last_run = dict(inventories_considered=len(invs), outcomes=counts, failed=failed, seconds=round(elapsed, 1))
    base = dict(layer=layer_name, created_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                source_layer_path=str(a.layer), source_layer=src,
                package=package_info("archival-structures", "archival_structures"))
    if v2:
        cfg = V2_CONFIG
        manifest = dict(base, version="v2",
                        config=dict(map_params=dataclasses.asdict(cfg["map_params"]), method=cfg["method"],
                                    variant=cfg["variant"], locator_params=dataclasses.asdict(SG_LOCATOR_PARAMS[cfg["variant"]]),
                                    column_cfg=cfg["column_cfg"]),
                        bars=BARS_V2, counts=dict(counts_d, n_headings=sum(d["n_headings"] for d in metas)),
                        last_run=last_run)
    else:
        manifest = dict(base, variant=a.variant, locator_params=dataclasses.asdict(SG_LOCATOR_PARAMS[a.variant]),
                        counts=counts_d, last_run=last_run)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(json.dumps(manifest["counts"]), counts, f"{elapsed:.1f}s")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
