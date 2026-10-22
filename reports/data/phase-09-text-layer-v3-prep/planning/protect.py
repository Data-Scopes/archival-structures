"""Hash (size + mtime) the protected trees into a file (phase 09; copy of the step-15ir script with
the skip list as an argument). Symlinks are not followed.

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 15j-

Format: `size=N MTIME PATH` per file, then `sha256=HEX PATH` for the 6 hand tar and 2 print zip
sources in shared-data. Protected: all of shared-data, LotA data/ gold/ output/, NLP
output/voc_text_layer, and every dev layer dir under data/text_layer_v2_dev/ whose name does not
start with one of the `--skip` prefixes (the step's own dirs). Compare two runs with `diff`."""
import argparse
import glob
import hashlib
import os

SH = "/Users/Shared/claude_projects/data_scopes"
ROOTS = [f"{SH}/shared-data", f"{SH}/language-of-the-archive/data", f"{SH}/language-of-the-archive/gold",
         f"{SH}/language-of-the-archive/output", f"{SH}/nlp-start-page-detection/output/voc_text_layer"]
DEV = "data/text_layer_v2_dev"


def walk(root):
    for dp, dns, fns in os.walk(root, followlinks=False):
        dns.sort()
        for fn in sorted(fns):
            p = os.path.join(dp, fn)
            try:
                st = os.lstat(p)
            except OSError:
                continue
            yield f"size={st.st_size} {int(st.st_mtime)} {p}"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--skip", nargs="+", required=True, help="dev dir name prefixes of this step")
    a = ap.parse_args(argv)
    roots = list(ROOTS)
    for name in sorted(os.listdir(DEV)):
        if any(name.startswith(s) for s in a.skip):
            continue
        p = os.path.join(DEV, name)
        if os.path.isdir(p) and not os.path.islink(p):
            roots.append(p)
    with open(a.out, "w") as fh:
        for r in roots:
            if os.path.exists(r):
                for line in walk(r):
                    fh.write(line + "\n")
        srcs = sorted(glob.glob(f"{SH}/shared-data/NL-HaNA_1.01.02/pagexml/*.tar.gz")
                      + glob.glob(f"{SH}/shared-data/NL-HaNA_1.10.94/pagexml/*.zip"))
        for p in srcs:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for blk in iter(lambda: f.read(1 << 20), b""):
                    h.update(blk)
            fh.write(f"sha256={h.hexdigest()} {p}\n")


if __name__ == "__main__":
    main()
