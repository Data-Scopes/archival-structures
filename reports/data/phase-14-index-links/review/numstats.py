import sys, csv
from collections import Counter
sys.path.insert(0, ".")
from archival_structures.text_layer.io import read_tsv_gz
from pathlib import Path
B = Path("data/index_links_dev/20e-a/NL-HaNA_1.01.02_index_links_v1")
vols = [r["inv"] for r in csv.DictReader(open(B/"volumes.tsv"), delimiter="\t")]
c = Counter()
for inv in vols:
    for r in read_tsv_gz(B/"links"/f"{inv}.tsv.gz"):
        n = int(r["number"]); m = r["method"]
        c["all"] += 1
        if n < 10: c["1digit"] += 1; c["1digit_"+m] += 1
        if 1500 <= n <= 1800: c["yearlike"] += 1; c["yearlike_"+m] += 1
        if n == 0: c["zero"] += 1
        c["conf_"+(r["confidence"] or "none")+"_"+m] += 1
for k in sorted(c): print(k, c[k])
