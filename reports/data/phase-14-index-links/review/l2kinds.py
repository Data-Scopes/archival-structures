import sys, csv, re, random
from collections import Counter, defaultdict
sys.path.insert(0, ".")
from archival_structures.text_layer.io import read_tsv_gz
from pathlib import Path
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
B = Path("data/index_links_dev/20e-a/NL-HaNA_1.01.02_index_links_v1")
vols = [r["inv"] for r in csv.DictReader(open(B/"volumes.tsv"), delimiter="\t")]
kinds = Counter(); samples = defaultdict(list); runkinds = Counter()
for inv in vols:
    texts = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(L/"join"/"units"/f"{inv}.tsv.gz")}
    for r in read_tsv_gz(B/"links"/f"{inv}.tsv.gz"):
        t = texts[(r["scan_id"], r["unit_id"])]
        e = int(r["char_end"]); s = int(r["char_start"])
        after = t[e:]
        m = re.match(r"[\s.,:;]*", after); sep = m.group(); rest = after[m.end():]
        before = t[max(0, s-1):s]
        if rest == "":
            k = "trailing"
        elif re.match(r"\d", rest):
            k = "run-internal"
        elif re.search(r"[.,:;]", sep):
            k = "punct+Capital"
        else:
            k = "space+Capital"
        glued = bool(re.match(r"[A-Za-z]", before))
        if glued: k2 = k + "|glued-left"
        else: k2 = k
        kinds[k2] += 1
        samples[k2].append((inv, r["number"], r["method"], t[max(0, s-40):e+30].replace("\n", " ")))
print(kinds, sum(kinds.values()))
rnd = random.Random(20261229)
for k in ("space+Capital", "space+Capital|glued-left", "trailing|glued-left", "punct+Capital"):
    print("==", k)
    for x in rnd.sample(samples[k], min(20, len(samples[k]))):
        print("  ", x)
