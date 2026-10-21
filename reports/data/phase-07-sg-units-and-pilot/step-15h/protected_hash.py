"""Hash the protected paths of step 15h: `size=N mtime path` per file (sha256 for SG source
archives). Usage: protected_hash.py OUTFILE"""
import hashlib
import os
import sys
from pathlib import Path

AS = Path("data/text_layer_v2_dev")
ROOTS = [AS / n for n in ("15d-a", "15fr-a", "15fp-a", "15fq-a", "15fq-b", "15fq-c", "15fq-ea",
                          "15fq-legacy", "15fq2-noguard", "15g-a", "15g-b", "15g-c", "15g2-all")]
ROOTS += [Path("/Users/Shared/claude_projects/data_scopes/shared-data")]
LOTA = Path("/Users/Shared/claude_projects/data_scopes/language-of-the-archive")
ROOTS += [LOTA / "data", LOTA / "gold", LOTA / "output"]
with open(sys.argv[1], "w") as out:
    for root in ROOTS:
        if not root.exists():
            out.write(f"MISSING {root}\n")
            continue
        for dp, dn, fn in os.walk(root):
            dn.sort()
            for f in sorted(fn):
                p = Path(dp) / f
                st = p.stat()
                if str(p).endswith((".tar.gz", ".zip")) and "shared-data" in str(p):
                    h = hashlib.sha256(p.read_bytes()).hexdigest()
                    out.write(f"{h} {int(st.st_mtime)} {p}\n")
                else:
                    out.write(f"size={st.st_size} {int(st.st_mtime)} {p}\n")
