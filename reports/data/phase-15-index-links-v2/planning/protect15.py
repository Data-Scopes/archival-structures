"""Phase-15 protected-tree listing: the phase-09 protect.py roots (shared-data, LotA data/gold/output, NLP
voc_text_layer, every dev layer under data/text_layer_v2_dev/, sha256 of the SG PageXML sources) plus the
index-link trees and the phase-14/15 report data. Format as protect.py (`size=N MTIME PATH`, `sha256=HEX PATH`).

    .venv/bin/python -I reports/data/phase-15-index-links-v2/planning/protect15.py OUT --skip <own paths>

`--skip` takes path prefixes (relative to the AS root) that the running step writes, e.g.
`reports/data/phase-15-index-links-v2/step-21b`; they are left out. Compare two runs with `diff`.
"""
import argparse
import importlib.util
import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "protect09", HERE.parents[1] / "phase-09-text-layer-v3-prep/planning/protect.py")
protect09 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(protect09)

EXTRA = ["data/index_links_dev", "data/index_links_full", "reports/data/phase-14-index-links",
         "reports/data/phase-15-index-links-v2"]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--skip", nargs="+", default=[], help="path prefixes this step writes")
    a = ap.parse_args(argv)
    tmp = a.out + ".09"
    protect09.main([tmp, "--skip", "\0none"])  # every dev layer is protected in phase 15
    skip = tuple(s.rstrip("/") for s in a.skip) + ("reports/data/phase-15-index-links-v2/planning",)
    with open(a.out, "w") as fh, open(tmp) as src:
        fh.write(src.read())
        for root in EXTRA:
            for line in protect09.walk(root):
                path = line.split(" ", 2)[2]
                if path.startswith(skip) or path == a.out or path == tmp:
                    continue
                fh.write(line + "\n")
    os.remove(tmp)


if __name__ == "__main__":
    main()
