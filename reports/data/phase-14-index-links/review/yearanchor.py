import sys, csv
from collections import Counter
sys.path.insert(0, ".")
from archival_structures.analysis.index_links import read_volume_inputs, front_index_run
from archival_structures.analysis.folio_map import build_folio_map
from archival_structures.text_layer.io import read_tsv_gz
from pathlib import Path
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
B = Path("data/index_links_dev/20e-a/NL-HaNA_1.01.02_index_links_v1")
vols = list(csv.DictReader(open(B/"volumes.tsv"), delimiter="\t"))
tot = Counter()
for v in vols:
    inv = v["inv"]
    vi = read_volume_inputs(L, inv)
    nscan = len(vi.scan_ids)
    run = front_index_run(vi.flags)
    excl = {s for i, s in enumerate(vi.sides) if i <= run[1] or vi.flags[i]}
    m = build_folio_map(vi.scan_ids, vi.sides, vi.numbers, exclude=excl)
    ch = m.chain
    bad = [(int(p), int(n)) for p, n in ch if m.mode == "foliation" and n > nscan]
    links = list(read_tsv_gz(B/"links"/f"{inv}.tsv.gz"))
    over = [r for r in links if int(r["number"]) > nscan]
    over_linked = [r for r in over if r["method"] != "none"]
    tot["links"] += len(links); tot["over"] += len(over); tot["over_linked"] += len(over_linked)
    if bad:
        good_last = max(int(n) for p, n in ch if n <= nscan)
        good_last_pos = max(int(p) for p, n in ch if n <= nscan)
        affected = [r for r in links if r["method"] != "none" and good_last < int(r["number"])]
        conf = Counter(r["confidence"] for r in affected); meth = Counter(r["method"] for r in affected)
        tot["affected"] += len(affected)
        print(inv, "nscan", nscan, "bad anchors", bad, "last good", good_last, "@", good_last_pos,
              "| links beyond last good:", len(affected), dict(meth), dict(conf),
              "| map rows", len(m.table()))
        for r in affected[:4]:
            print("   ", r["number"], r["front_scan_id"][-4:], r["method"], r["anchor_dist"], r["confidence"])
print(dict(tot))
