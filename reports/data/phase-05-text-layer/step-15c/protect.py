"""Snapshot / verify the protected NLP reference trees (step 15c).

    protect.py snapshot SNAP.json     # sha256 of every REF file; size+mtime of V1 wbc/, manifest, status
    protect.py verify SNAP.json       # exit 1 on any difference
"""
import hashlib, json, sys
from pathlib import Path

NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
REF = NLP / "output/voc_text_layer/voc_text_layer_v1"
V1 = NLP / "data/voc_text_layer_v1"


def state():
    out = {}
    for p in sorted(REF.rglob("*")):
        if p.is_file():
            out[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
    for p in sorted(list((V1 / "wbc").rglob("*")) + [V1 / "manifest.json", V1 / "status.tsv"]):
        if p.is_file():
            st = p.stat()
            out[str(p)] = [st.st_size, st.st_mtime_ns]
    return out


if __name__ == "__main__":
    mode, snap = sys.argv[1], Path(sys.argv[2])
    cur = state()
    if mode == "snapshot":
        snap.write_text(json.dumps(cur)); print(len(cur), "files snapshotted")
    else:
        old = json.loads(snap.read_text())
        bad = [k for k in set(old) | set(cur) if old.get(k) != cur.get(k)]
        print("protected files:", len(cur), "differences:", len(bad), bad[:5])
        sys.exit(1 if bad else 0)
