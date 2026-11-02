"""Step 23a: reader inputs for the 40 fresh VOC volumes.  Run from the AS root:
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/dump.py [--inv X ...]
Writes dumps/<inv>_front.txt (scans 1..min(n,100); header line + every non-empty line text with its page tag)
and dumps/<inv>_numbers.tsv (one row per side of the whole volume: scan_id scan_num page texts).
`texts` = page-number texts joined by ' | ', from read_volume_inputs(..., number_tags=("page-number",)).numbers.
Only `sides` and `numbers` of VolumeInputs are used; `flags` (index_side) is never written."""
import csv, gzip, os, sys
from pathlib import Path
sys.path.insert(0, ".")
from archival_structures.analysis.index_links import read_volume_inputs

D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
LAYER = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
csv.field_size_limit(10**9)


def tsv(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def dump(inv):
    scans = sorted(tsv(LAYER / "scans" / f"{inv}.scans.tsv.gz"), key=lambda r: int(r["scan_num"]))
    page = {(r["scan_id"], r["line_id"]): r["page"] for r in tsv(LAYER / "layout" / "lines" / f"{inv}.tsv.gz")}
    by_scan = {}
    for r in tsv(LAYER / "lines" / f"{inv}.lines.tsv.gz"):
        by_scan.setdefault(r["scan_id"], []).append(r)
    v = read_volume_inputs(LAYER, inv, number_tags=("page-number",))
    sides = {sid: sorted({page.get((sid, r["line_id"]), "single") for r in ls}) for sid, ls in by_scan.items()}
    out = []
    for s in scans[:100]:
        sid = s["scan_id"]
        ls = by_scan.get(sid, [])
        out.append(f"== {sid} | #{int(s['scan_num'])} | sides: {','.join(sides.get(sid, [])) or '-'} | lines: {s['n_lines']}")
        for r in ls:
            t = r["text"].strip()
            if t:
                out.append(f"[{page.get((sid, r['line_id']), 'single')}] {t}")
    num = {s["scan_id"]: int(s["scan_num"]) for s in scans}
    (D / "dumps" / f"{inv}_front.txt").write_text("\n".join(out) + "\n")
    with open(D / "dumps" / f"{inv}_numbers.tsv", "w") as f:
        f.write("scan_id\tscan_num\tpage\ttexts\n")
        for sid, pg in v.sides:
            texts = " | ".join(t.replace("\t", " ").replace("\n", " ") for t in v.numbers.get((sid, pg), []))
            f.write(f"{sid}\t{num.get(sid, '')}\t{pg}\t{texts}\n")
    return min(len(scans), 100), len(scans)


if __name__ == "__main__":
    args = sys.argv[1:]
    invs = args[args.index("--inv") + 1:] if "--inv" in args else (D / "inventories.txt").read_text().split()
    (D / "dumps").mkdir(exist_ok=True)
    for inv in invs:
        n, tot = dump(inv)
        for p in ((D / "dumps" / f"{inv}_front.txt"), (D / "dumps" / f"{inv}_numbers.tsv")):
            os.chmod(p, os.stat(p).st_mode | 0o020)
        print(inv, "front scans", n, "of", tot)
