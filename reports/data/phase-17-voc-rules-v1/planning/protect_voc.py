"""Phase 17: list the VOC trees that no phase-17 step may change (size + mtime per file), plus sha256 of
the frozen gold files. Run before and after every step that writes data, and `diff` the two lists.

    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT

Protected: the phase-16 VOC layer (`data/text_layer_voc_22a`, a link to T7_Data, 567 volumes), its
inputs dir (symlinks), the phase-16 gold and check files, and, once they exist, the phase-17 fresh
layer `data/text_layer_voc_23a` and the frozen 23a gold (`step-23a/gold/`). Paths that do not exist yet
are listed as `missing PATH`, so a before/after pair stays comparable."""
import hashlib
import os
import sys
from pathlib import Path

TREES = ["data/text_layer_voc_22a/", "data/text_layer_voc_inputs/", "data/text_layer_voc_23a/"]
FILES = ["reports/data/phase-16-voc-generalisation/planning/gm_anchors.tsv",
         "reports/data/phase-16-voc-generalisation/planning/gm_split.tsv",
         "reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv",
         "reports/data/phase-16-voc-generalisation/step-22b/check_result.tsv",
         "reports/data/phase-17-voc-rules-v1/planning/fresh_volumes.tsv"]
GOLD_DIR = "reports/data/phase-17-voc-rules-v1/step-23a/gold/"


def main(out):
    lines = []
    for t in TREES:
        if not os.path.exists(t):
            lines.append(f"missing {t}")
            continue
        for dp, dns, fns in os.walk(t, followlinks=False):
            dns.sort()
            for fn in sorted(fns):
                p = os.path.join(dp, fn)
                st = os.lstat(p)
                lines.append(f"size={st.st_size} {int(st.st_mtime)} {p}")
    files = FILES + (sorted(str(p) for p in Path(GOLD_DIR).glob("*")) if os.path.isdir(GOLD_DIR) else [])
    for f in files:
        if os.path.exists(f):
            lines.append(f"sha256={hashlib.sha256(Path(f).read_bytes()).hexdigest()} {f}")
        else:
            lines.append(f"missing {f}")
    Path(out).write_text("\n".join(lines) + "\n")
    os.chmod(out, 0o664)
    print(f"{len(lines)} lines -> {out}")


if __name__ == "__main__":
    main(sys.argv[1])
