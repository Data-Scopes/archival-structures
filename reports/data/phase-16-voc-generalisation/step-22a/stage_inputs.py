"""Step 22a: write inventories.txt and symlink the VOC PageXML zips under data/text_layer_voc_inputs/1.04.02/.

Run from the AS root: .venv/bin/python reports/data/phase-16-voc-generalisation/step-22a/stage_inputs.py
"""
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SRC = Path("/Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1")
GLOB = "1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split()


def key(i):
    m = re.fullmatch(r"(\d+)([A-Z]*)", i)
    return int(m.group(1)), m.group(2)


def main():
    gm = [l.split("\t")[0] for l in (ROOT / "reports/data/phase-16-voc-generalisation/planning/gm_split.tsv")
          .read_text().splitlines()[1:] if l.strip()]
    gm = [i for i in gm if i != "1171"]
    assert not set(gm) & set(GLOB)
    invs = sorted(set(gm) | set(GLOB), key=key)
    print(len(gm), len(GLOB), len(invs))
    missing = [i for i in invs if not (SRC / f"{i}.zip").exists()]
    if missing:
        raise SystemExit(f"missing targets: {missing}")
    out = ROOT / "reports/data/phase-16-voc-generalisation/step-22a/inventories.txt"
    out.write_text("\n".join(invs) + "\n")
    d = ROOT / "data/text_layer_voc_inputs/1.04.02"
    d.mkdir(parents=True, exist_ok=True)
    for i in invs:
        link = d / f"{i}.zip"
        if not link.is_symlink():
            os.symlink(SRC / f"{i}.zip", link)
    for p in (ROOT / "data/text_layer_voc_inputs", d):
        os.chmod(p, os.stat(p).st_mode | 0o020)
    os.chmod(out, os.stat(out).st_mode | 0o020)


main()
