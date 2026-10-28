"""Check the staged drive rebuild (UA-035 part 2) against `15z-b`, for the 16 pinned volumes.

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/after-15z-rebuild/check.py

A = `data/text_layer_v2_dev/15z-b` (37 volumes, local, pins applied), B = `data/text_layer_v4_full`
(the staged copy of the drive layer after the rebuild). For every per-inventory file of the 16 pinned
volumes (and 6 control volumes staged before and after) it compares decompressed bytes (JSON with
`seconds`/`elapsed*` removed); differing TSVs get per-column row counts. It also compares the audit's
`modes` and `counts` entries of the 16 between A and B, and between 15z-a and 15z-b (the pin effect).
Writes `check.json` next to this script.
"""
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, "reports/data/phase-09-text-layer-v3-prep/planning")
sys.path.insert(0, "reports/data/phase-12-hand-print-box-height/step-15v")
from compare import _json_norm, table_diff  # noqa: E402
from common import raw_bytes  # noqa: E402

OUT = Path(__file__).with_name("check.json")
SUB = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
A = Path("data/text_layer_v2_dev/15z-b")
A0 = Path("data/text_layer_v2_dev/15z-a")
B = Path("data/text_layer_v4_full")
PINS = Path("reports/data/phase-12-hand-print-box-height/step-15z/pins_drive.txt").read_text().split()
PIN_MODE = {i: "hand" for i in PINS if i not in ("12163", "12165")} | {"12163": "print", "12165": "print"}


def inv_of(p):
    m = re.match(r"(\d+[A-Z]?)(?:[.\-_]|$)", p.name)
    return m.group(1) if m else None


def files_for(root, invs):
    return {p.relative_to(root) for p in root.rglob("*") if p.is_file() and inv_of(p) in invs}


def compare_files(ra, rb, invs):
    res = {"equal": 0, "differ": {}, "missing": []}
    for f in sorted(files_for(ra, invs) | files_for(rb, invs)):
        pa, pb = ra / f, rb / f
        if not pa.exists() or not pb.exists():
            res["missing"].append(f"{'A' if not pa.exists() else 'B'}:{f}")
            continue
        a, b = raw_bytes(pa), raw_bytes(pb)
        same = _json_norm(a) == _json_norm(b) if f.suffix == ".json" else a == b
        if same:
            res["equal"] += 1
        elif ".tsv" in f.name:
            res["differ"][str(f)] = table_diff(pa, pb)
        else:
            res["differ"][str(f)] = "bytes"
    return res


def audit_diff(xa, xb, invs):
    out = {}
    for i in invs:
        ma, mb = xa["modes"].get(i), xb["modes"].get(i)
        ca, cb = xa["counts"].get(i, {}), xb["counts"].get(i, {})
        d = {k: [ca.get(k), cb.get(k)] for k in sorted(set(ca) | set(cb)) if ca.get(k) != cb.get(k)}
        if ma != mb or d:
            out[i] = {"modes": [ma, mb], "counts": d}
    return out


def merge_fail(x, i):
    for r in x["invariants"]:
        if r["invariant"].startswith(f"[{i}] merges with lower-case"):
            return r["pass"]
    return None


def main():
    ra, rb = A / SUB, B / SUB
    staged = {inv_of(p) for p in (rb / "units" / "doc_id_map").iterdir()}
    controls = sorted(staged - set(PINS))
    xa, xa0, xb = (json.loads((d / "audit.json").read_text()) for d in (A, A0, B))
    res = {
        "staged_inventories": sorted(staged),
        "pins": PIN_MODE,
        "modes_full": {i: xb["modes"].get(i) for i in PINS},
        "mode_mismatch": {i: xb["modes"].get(i) for i in PINS if (xb["modes"].get(i) or [None])[0] != PIN_MODE[i]},
        "print_count_full": sum(1 for m in xb["modes"].values() if m[0] == "print"),
        "files_16_vs_15z-b": compare_files(ra, rb, set(PINS)),
        "files_controls_vs_15z-b": compare_files(ra, rb, set(controls) & {inv_of(p) for p in ra.rglob("*")}),
        "audit_16_15z-b_vs_full": audit_diff(xa, xb, PINS),
        "audit_16_15z-a_vs_15z-b": audit_diff(xa0, xa, PINS),
        "merge_share_pass": {i: {"15z-a": merge_fail(xa0, i), "15z-b": merge_fail(xa, i), "full": merge_fail(xb, i)}
                             for i in PINS},
        "audit_totals": {k: {"n_pass": x["n_pass"], "n_fail": x["n_fail"], "n_fail_judged": x["n_fail_judged"]}
                         for k, x in (("15z-a", xa0), ("15z-b", xa), ("full", xb))},
    }
    OUT.write_text(json.dumps(res, indent=1), encoding="utf-8")
    for k in ("staged_inventories", "mode_mismatch", "print_count_full", "audit_totals"):
        print(k, res[k])
    for k in ("files_16_vs_15z-b", "files_controls_vs_15z-b"):
        r = res[k]
        print(f"== {k}: {r['equal']} equal, {len(r['differ'])} differ, {len(r['missing'])} missing")
        for f, d in r["differ"].items():
            print("  ", f, d)
        for f in r["missing"][:40]:
            print("   missing", f)
    for k in ("audit_16_15z-b_vs_full", "audit_16_15z-a_vs_15z-b"):
        print("==", k)
        for i, d in res[k].items():
            print("  ", i, d)
    print("== merge-share invariant pass (15z-a, 15z-b, full)")
    for i, d in res["merge_share_pass"].items():
        print("  ", i, d)


if __name__ == "__main__":
    main()
