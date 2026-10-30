"""Step 21d: v1 (21d-v1, == 20r-a) against v2 (21d-a), per volume. Writes compare.tsv and chain_ends.tsv here.
Columns: entries / locators / linked per version, n_headings, confidence shares of located links (high, medium, low)
and the share of `extrapolated` links, chain changes (first, last, length, n_dup_dropped, chain_cut).
chain_ends.tsv: for volumes whose chain end differs, v1 links (printed, i.e. `high`) between the new and the old chain end
and what v2 does with the same (scan, unit, number) links. Also checks: no v2 link located beyond chain +- 6.
Run from the AS root: .venv/bin/python -I reports/data/phase-15-index-links-v2/step-21d/compare_v1_v2.py"""
import csv
from collections import Counter
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

V1 = Path("data/index_links_dev/21d-v1/NL-HaNA_1.01.02_index_links_v1")
V2 = Path("data/index_links_dev/21d-a/NL-HaNA_1.01.02_index_links_v2")
OUT = Path(__file__).parent


def vols(base):
    with open(base / "volumes.tsv") as f:
        return {r["inv"]: r for r in csv.DictReader(f, delimiter="\t")}


v1, v2 = vols(V1), vols(V2)
assert list(v1) == list(v2)
hdr = ("inv n_entries_v1 n_entries_v2 n_loc_v1 n_loc_v2 d_loc n_linked_v1 n_linked_v2 n_headings high_v1 high_v2 medium_v1 "
       "medium_v2 low_v1 low_v2 extrap_v1 extrap_v2 chain_first_v1 chain_first_v2 chain_last_v1 chain_last_v2 chain_len_v1 "
       "chain_len_v2 n_dup_dropped chain_cut chain_end_changed").split()
rows, ends, beyond = [], [], 0
tot = Counter()
for inv in v1:
    a, b = v1[inv], v2[inv]
    cnt = {}
    L1 = list(read_tsv_gz(V1 / "links" / f"{inv}.tsv.gz"))
    L2 = list(read_tsv_gz(V2 / "links" / f"{inv}.tsv.gz"))
    for tag, L in (("v1", L1), ("v2", L2)):
        loc = [r for r in L if r["method"] != "none"]
        n = max(len(loc), 1)
        c = Counter(r["confidence"] for r in loc)
        cnt[tag] = [f"{c[k] / n:.4f}" for k in ("high", "medium", "low")] + [f"{sum(r['method'] == 'extrapolated' for r in loc) / n:.4f}"]
        for k in ("high", "medium", "low"):
            tot[(tag, k)] += c[k]
        tot[(tag, "extrapolated")] += sum(r["method"] == "extrapolated" for r in loc)
        tot[(tag, "located")] += len(loc)
    ch_changed = (a["chain_first"], a["chain_last"]) != (b["chain_first"], b["chain_last"])
    rows.append([inv, a["n_entries"], b["n_entries"], a["n_locators"], b["n_locators"], int(b["n_locators"]) - int(a["n_locators"]),
                 a["n_linked"], b["n_linked"], b["n_headings"], cnt["v1"][0], cnt["v2"][0], cnt["v1"][1], cnt["v2"][1],
                 cnt["v1"][2], cnt["v2"][2], cnt["v1"][3], cnt["v2"][3], a["chain_first"], b["chain_first"], a["chain_last"],
                 b["chain_last"], a["chain_len"], b["chain_len"], b["n_dup_dropped"], b["chain_cut"], int(ch_changed)])
    lo, hi = int(b["chain_first"]), int(b["chain_last"])
    beyond += sum(1 for r in L2 if r["method"] != "none" and not (lo - 6 <= int(r["number"]) <= hi + 6))
    if ch_changed:
        o_lo, o_hi = int(a["chain_first"]), int(a["chain_last"])
        v2keys = Counter()
        for r in L2:
            v2keys[(r["scan_id"], r["entry_id"], int(r["number"]), r["method"] != "none")] += 1
        for side, rng in (("last", (hi, o_hi)), ("first", (o_lo, lo))):
            lo_, hi_ = rng
            if hi_ <= lo_:
                continue
            old = [r for r in L1 if r["method"] != "none" and lo_ < int(r["number"]) <= hi_] if side == "last" else \
                  [r for r in L1 if r["method"] != "none" and lo_ <= int(r["number"]) < hi_]
            n_old = len(old)
            n_high = sum(r["confidence"] == "high" for r in old)
            old = [r for r in old if r["confidence"] == "high"]
            n_old = len(old)
            kept = sum(1 for r in old if v2keys[(r["scan_id"], r["unit_id"], int(r["number"]), True)] > 0)
            absent = sum(1 for r in old if all(v2keys[(r["scan_id"], r["unit_id"], int(r["number"]), t)] == 0 for t in (True, False)))
            ends.append([inv, side, o_lo, o_hi, lo, hi, n_old, n_high, kept, n_old - kept - absent, absent])
hdr_e = "inv end v1_first v1_last v2_first v2_last v1_located_links_in_gap v1_high_in_gap (rows below = the high ones) v2_located v2_unlocated v2_no_locator".split()
(OUT / "compare.tsv").write_text("\t".join(hdr) + "\n" + "".join("\t".join(map(str, r)) + "\n" for r in rows))
(OUT / "chain_ends.tsv").write_text("\t".join(hdr_e) + "\n" + "".join("\t".join(map(str, r)) + "\n" for r in ends))
print("volumes", len(rows), "chain-end volumes", sum(r[-1] for r in rows), "v2 links located beyond chain +-6:", beyond)
for tag in ("v1", "v2"):
    n = tot[(tag, "located")]
    print(tag, "located", n, {k: f"{tot[(tag, k)] / n:.4f}" for k in ("high", "medium", "low", "extrapolated")})
print("chain-end gaps:")
for e in ends:
    print(" ", e)
