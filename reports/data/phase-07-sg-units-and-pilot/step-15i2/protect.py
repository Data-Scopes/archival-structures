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
        if name in ("15i-b", "15i-src") or name.startswith("15i2-"):
            continue
        p = os.path.join(DEV, name)
        if os.path.isdir(p) and not os.path.islink(p):
            roots.append(p)
    with open(out, "w") as fh:
        for r in roots:
            if os.path.exists(r):
                for line in walk(r):
                    fh.write(line + "\n")
    print("wrote", out)


if __name__ == "__main__":
    main(sys.argv[1])
