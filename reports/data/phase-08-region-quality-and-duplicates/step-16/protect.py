"""Record/check protected LotA outputs: `protect.py OUT` writes `size=N mtime path` for every file
under LotA output/quality and gold, plus sha256 of the two gold sheets and the lexicon cache."""
import hashlib
import os
import sys

L = "/Users/Shared/claude_projects/data_scopes/language-of-the-archive"
ROOTS = [f"{L}/output/quality", f"{L}/gold"]
SHA = [f"{L}/gold/02-region-quality-done.tsv", f"{L}/gold/02c-short-region-quality-done.tsv",
       f"{L}/output/lexicon/fasttext-dim_384-window_10-min_count_100-case_lower-vocab.txt.gz"]


def main(out):
    with open(out, "w") as fh:
        for r in ROOTS:
            for dp, dns, fns in os.walk(r, followlinks=False):
                dns.sort()
                for fn in sorted(fns):
                    p = os.path.join(dp, fn)
                    st = os.lstat(p)
                    fh.write(f"size={st.st_size} {int(st.st_mtime)} {p}\n")
        for p in SHA:
            fh.write(f"sha256={hashlib.sha256(open(p, 'rb').read()).hexdigest()} {p}\n")
    print("wrote", out)


if __name__ == "__main__":
    main(sys.argv[1])
