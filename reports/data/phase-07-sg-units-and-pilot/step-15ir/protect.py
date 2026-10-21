"""Hash (size + mtime) the protected trees into a file: `protect.py OUT`. Symlinks are not followed."""
import os
import sys

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


def main(out):
    roots = list(ROOTS)
    for name in sorted(os.listdir(DEV)):
        if name.startswith("15ir-"):
            continue
        p = os.path.join(DEV, name)
        if os.path.isdir(p) and not os.path.islink(p):
            roots.append(p)
    with open(out, "w") as fh:
        for r in roots:
            if os.path.exists(r):
                for line in walk(r):
                    fh.write(line + "\n")
        import glob
        import hashlib
        srcs = sorted(glob.glob(f"{SH}/shared-data/NL-HaNA_1.01.02/pagexml/*.tar.gz")
                      + glob.glob(f"{SH}/shared-data/NL-HaNA_1.10.94/pagexml/*.zip"))
        for p in srcs:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for blk in iter(lambda: f.read(1 << 20), b""):
                    h.update(blk)
            fh.write(f"sha256={h.hexdigest()} {p}\n")
    print("wrote", out)


if __name__ == "__main__":
    main(sys.argv[1])
