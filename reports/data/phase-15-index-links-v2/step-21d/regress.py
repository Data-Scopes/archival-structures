"""Step 21d regression: the v1 rebuild (21d-v1) against 20r-a, decompressed text, meta without timing keys.
Run from the AS root: .venv/bin/python -I reports/data/phase-15-index-links-v2/step-21d/regress.py > .../regress.txt"""
import gzip, json
from pathlib import Path
A = Path("data/index_links_dev/20r-a/NL-HaNA_1.01.02_index_links_v1")
B = Path("data/index_links_dev/21d-v1/NL-HaNA_1.01.02_index_links_v1")
TIMING = {"seconds", "created_utc", "elapsed", "time"}
bad, n = [], {}
for sub in ("folio_map", "links", "meta"):
    fa, fb = sorted(p.name for p in (A / sub).iterdir()), sorted(p.name for p in (B / sub).iterdir())
    if fa != fb:
        bad.append(f"{sub}: file lists differ")
    n[sub] = len(fa)
    for name in fa:
        pa, pb = A / sub / name, B / sub / name
        if sub == "meta":
            ja = {k: v for k, v in json.loads(pa.read_text()).items() if k not in TIMING}
            jb = {k: v for k, v in json.loads(pb.read_text()).items() if k not in TIMING}
            same = ja == jb
        else:
            same = gzip.open(pa, "rt").read() == gzip.open(pb, "rt").read()
        if not same:
            bad.append(f"{sub}/{name} differs")
vol_same = (A / "volumes.tsv").read_text() == (B / "volumes.tsv").read_text()
if not vol_same:
    bad.append("volumes.tsv differs")
print("files compared:", n, "volumes.tsv equal:", vol_same)
print("RESULT:", "ALL EQUAL" if not bad else "DIFFERENCES")
for b in bad:
    print(" ", b)
