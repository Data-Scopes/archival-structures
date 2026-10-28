"""Volume-mode extract (step 15v, phase 12): one hand or print mode per inventory.

A pre-pass over the PageXML builds the v2 line rows, the char-weighted median line-box height
of the volume decides the mode (``hand_print.box_height_mode``; the cut lives in the preset's
``volume_mode``), the decision is recorded under ``mode/`` and the inventory is built with the
rule fields of the chosen mode (``presets.inventory_preset``). Rules never switch inside an
inventory."""

import time
from pathlib import Path
from typing import Optional

import numpy as np

from archival_structures.analysis.hand_print import (
    BoxHeightParams, box_height_mode, region_features, scan_box_heights)
from archival_structures.text_layer.io import write_json_atomic, write_tsv_gz
from archival_structures.text_layer.presets import inventory_preset

SIDE_MIN_CHARS = 200   # step 15u: a scan has a side only with at least this many qualifying chars

MODE_SCAN_COLUMNS = ["scan_id", "scan_order", "page_w", "page_h", "n_lines_text", "value",
                     "n_chars", "n_regions", "side"]
MODE_REGION_COLUMNS = ["region_id", "scan_id", "n_lines", "n_chars", "h_med", "pitch_med", "hrel"]


def decide_mode(line_rows, preset, inv=None) -> dict:
    """Mode of one inventory from its (raw) v2 line rows. Adds ``scans`` and ``regions``.

    Step 15y: ``volume_mode["pins"]`` ({inv: "hand"|"print"}) overrides the measured mode of ``inv``;
    the measured result stays in ``box_height_mode``/``box_height_method``, ``method`` is ``pinned``.
    """
    vm = preset.volume_mode
    feats = region_features(line_rows)
    params = BoxHeightParams.from_dict(vm["params"])
    r = box_height_mode(feats, params)
    if vm["method"] == "box_height":
        result = dict(r)
    elif vm["method"] == "fixed":
        result = dict(r)
        result["box_height_mode"], result["box_height_method"] = r["mode"], r["method"]
        result["mode"], result["method"] = vm["mode"], "fixed"
    else:
        raise ValueError(f"unknown volume_mode method {vm['method']!r}")
    pins = vm.get("pins") or {}
    if inv is not None and inv in pins:
        pin = pins[inv]
        if pin not in ("hand", "print"):
            raise ValueError(f"pin for {inv} must be 'hand' or 'print', got {pin!r}")
        result["box_height_mode"], result["box_height_method"] = r["mode"], r["method"]
        result["mode"], result["method"] = pin, "pinned"
    result["scans"] = scan_box_heights(feats, params.min_region_chars)
    result["regions"] = feats
    return result


def column_start_share(line_rows, min_chars=20, min_rows=50, mid_frac=0.4):
    """Share of lines that start in the right part of the text span of their page side.

    Measures the second column of a two-column page side: per ``page_side`` (with at least
    ``min_rows`` lines of ``min_chars`` or more characters and a box), ``left`` is the 5th
    percentile of ``x``, ``right`` the 95th percentile of ``x + w``; a line counts when
    ``x >= left + mid_frac * span``. Returns ``(share, n_rows)`` pooled over the qualifying
    sides, or ``(None, n_rows_kept)`` when no side qualifies."""
    by_side = {}
    kept = 0
    for r in line_rows:
        if len(r.get("text_raw") or "") < min_chars or r.get("x") in ("", None) or r.get("w") in ("", None):
            continue
        kept += 1
        by_side.setdefault(r.get("page_side"), []).append((float(r["x"]), float(r["w"])))
    k_tot = n_tot = 0
    for rows in by_side.values():
        if len(rows) < min_rows:
            continue
        xs = np.array([a for a, _ in rows])
        xe = np.array([a + b for a, b in rows])
        left, right = np.percentile(xs, 5), np.percentile(xe, 95)
        k_tot += int((xs >= left + mid_frac * (right - left)).sum())
        n_tot += len(rows)
    if not n_tot:
        return None, kept
    return k_tot / n_tot, n_tot


def _cell(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return round(v, 4)
    return v


def write_mode(out_root, inv, result, info):
    """Write ``mode/<inv>.json``, ``mode/scans/<inv>.tsv.gz`` and ``mode/regions/<inv>.tsv.gz``."""
    root = Path(out_root) / "mode"
    cut = result["params"]["cut"]
    by_scan = {s["scan_id"]: s for s in result["scans"]}
    rows, sides = [], []
    for i, sc in enumerate(info["scans"]):
        b = by_scan.get(sc["scan_id"])
        value, n_chars, n_regions = (b["value"], b["n_chars"], b["n_regions"]) if b else (None, 0, 0)
        side = ""
        if n_chars >= SIDE_MIN_CHARS and value is not None:
            side = "print" if value <= cut else "hand"
            sides.append(side)
        rows.append([sc["scan_id"], i, _cell(sc["page_w"]), _cell(sc["page_h"]),
                     _cell(sc["n_lines_text"]), _cell(value), n_chars, n_regions, side])
    write_tsv_gz(root / "scans" / f"{inv}.tsv.gz", MODE_SCAN_COLUMNS, rows)
    write_tsv_gz(root / "regions" / f"{inv}.tsv.gz", MODE_REGION_COLUMNS,
                 [[_cell(f.get(c)) for c in MODE_REGION_COLUMNS] for f in result["regions"]])
    meta = {k: v for k, v in result.items() if k not in ("scans", "regions")}
    meta.update({"inv": inv, "n_scans": info["n_scans"], "n_parse_fail": info["n_parse_fail"],
                 "n_scans_sided": len(sides),
                 "scan_minority_share": (sum(s != result["mode"] for s in sides) / len(sides)
                                         if sides else None)})
    write_json_atomic(root / f"{inv}.json", meta)
    return meta


def read_mode(layer_root, inv) -> str:
    import json
    path = Path(layer_root) / "mode" / f"{inv}.json"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found: run the extract stage first")
    return json.loads(path.read_text(encoding="utf-8"))["mode"]


def extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False):
    """Stage 1 (v2, v4 presets): pre-pass, decide and record the mode, extract with its rules."""
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    from archival_structures.text_layer.line_split import (
        _int_row, _write_split_files, fit_split_model_from_rows, make_split_hook, prepass_rows)
    start = time.monotonic()
    rows, tags, info = prepass_rows(inv, source, preset)
    prepass_seconds = time.monotonic() - start
    decision = decide_mode(rows, preset, inv=inv)
    mode = decision["mode"]
    resolved = inventory_preset(preset, mode)
    split = resolved.merged_line_split == "on"
    gate = (preset.volume_mode or {}).get("split_gate")
    if split and gate:
        share, n = column_start_share(rows, min_chars=gate["min_chars"], min_rows=gate["min_rows"],
                                      mid_frac=gate["mid_frac"])
        split = share is None or share >= gate["min_share"]
        decision["split_gate"] = {"share": None if share is None else round(share, 4), "n_rows": n,
                                  "min_share": gate["min_share"], "split": split}
    write_mode(out_root, inv, decision, info)
    out_root = Path(out_root)
    # Step 15y: files of an earlier build of this inventory that this build does not write are stale
    stale = [out_root / "split_stats" / f"{inv}.splits.json"] if not (split and record_splits) else []
    if not split:
        stale += [out_root / "split_model" / f"{inv}.json", out_root / "split_stats" / f"{inv}.json"]
    for path in stale:
        path.unlink(missing_ok=True)
    if split:
        model = fit_split_model_from_rows([_int_row(r) for r in rows], tags,
                                          resolved.split_region_tags, resolved.untagged_tag)
        hook = make_split_hook(model)
        hook.record_splits = record_splits
        result = extract_inventory_v2(inv, source, out_root, resolved, scan_hook=hook)
        _write_split_files(inv, out_root, model, hook, result, record_splits)
    else:
        result = extract_inventory_v2(inv, source, out_root, resolved)
    result["mode"], result["mode_method"] = mode, decision["method"]
    # Step 15yr: the status row's seconds time the whole call, the pre-pass included
    result["prepass_seconds"] = round(prepass_seconds, 3)
    result["seconds"] = round(time.monotonic() - start, 3)
    return result
