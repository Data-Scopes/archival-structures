"""Step 23a: write inventories.txt and symlink the 40 fresh VOC PageXML zips under
data/text_layer_voc_inputs_23a/1.04.02/. Asserts they are disjoint from the 567 phase-16 inputs.

Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/stage_inputs.py
"""
import csv
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SRC = Path("/Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1")
P16 = ROOT / "data/text_layer_voc_inputs/1.04.02"


def main():
    rows = list(csv.DictReader(open(ROOT / "reports/data/phase-17-voc-rules-v1/planning/fresh_volumes.tsv"), delimiter="\t"))
    invs = [r["inv"] for r in sorted(rows, key=lambda r: int(r["rank"]))]
    assert len(invs) == 40 and len(set(invs)) == 40, len(invs)
    old = {p.name[:-4] for p in P16.glob("*.zip")}
    assert len(old) == 567, len(old)
    assert not set(invs) & old, sorted(set(invs) & old)
    missing = [i for i in invs if not (SRC / f"{i}.zip").exists()]
    if missing:
        raise SystemExit(f"missing targets: {missing}")
    out = ROOT / "reports/data/phase-17-voc-rules-v1/step-23a/inventories.txt"
    out.write_text("\n".join(invs) + "\n")
    d = ROOT / "data/text_layer_voc_inputs_23a/1.04.02"
    d.mkdir(parents=True, exist_ok=True)
    for i in invs:
        link = d / f"{i}.zip"
        if not link.is_symlink():
            os.symlink(SRC / f"{i}.zip", link)
    for p in (ROOT / "data/text_layer_voc_inputs_23a", d, out):
        os.chmod(p, os.stat(p).st_mode | 0o020)
    print(len(invs), "staged; disjoint from", len(old))


main()
