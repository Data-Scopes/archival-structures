"""`protect.py OUT`: size + mtime of the protected LotA trees (integrity except as-step17, quality, gold)."""
import os, sys
L = "/Users/Shared/claude_projects/data_scopes/language-of-the-archive"
ROOTS = [f"{L}/output/integrity", f"{L}/output/quality", f"{L}/gold"]
with open(sys.argv[1], "w") as fh:
    for r in ROOTS:
        for dp, dns, fns in os.walk(r, followlinks=False):
            dns[:] = sorted(d for d in dns if not (d == "as-step17" and dp.endswith("integrity")))
            for fn in sorted(fns):
                p = os.path.join(dp, fn)
                st = os.lstat(p)
                fh.write(f"size={st.st_size} mtime={int(st.st_mtime)} {p}\n")
