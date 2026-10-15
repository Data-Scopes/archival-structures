"""Review EA: re-check step-14 sample numbers; fresh sample of inventories that drop U+201E."""
import gzip, random
import pandas as pd

V1 = "/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1/"
per = pd.read_csv(V1 + "wbc/wbc_per_inventory.tsv", sep="\t", quoting=3, dtype=str,
                  keep_default_na=False, na_values=[],
                  usecols=["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default"])
st = pd.read_csv(V1 + "wbc/wbc_stats.tsv", sep="\t", quoting=3,
                 dtype={"inventory_number": str, "char": str}, keep_default_na=False, na_values=[],
                 usecols=["inventory_number", "char", "n_lines", "end_freq", "n_end_attached",
                          "join_vocab_frac", "end_rel_frac", "all_freq"])
print("rows", st.shape)

# (a) the step-14 sample's quoted numbers
for inv, c in [("3008", "„"), ("2914", "„"), ("7724", "„"), ("10502", "„"), ("3132", "„"), ("1068", "¬")]:
    r = st[(st.inventory_number == inv) & (st.char == c)]
    print("(a)", inv, c, r[["n_lines", "n_end_attached", "join_vocab_frac", "end_rel_frac"]].round(3).values.tolist())

# (b) fresh sample: per_char inventories whose set lacks „
drop = per[(per["mode"] == "per_char") & ~per.wbc_set.str.contains("„")]
print("(b) per_char inventories without „:", len(drop))
g = st[st.char == "„"].set_index("inventory_number")
sub = g.loc[g.index.intersection(drop.inventory_number)]
sub = sub[sub.n_end_attached >= 30]
fail_j = sub.join_vocab_frac < 0.25
fail_r = sub.end_rel_frac < 5.0
print("    with >=30 attached:", len(sub), " fail join only", int((fail_j & ~fail_r).sum()),
      " fail rel only", int((~fail_j & fail_r).sum()), " both", int((fail_j & fail_r).sum()))
print("    end_rel_frac quartiles of the rel-failers",
      sub[fail_r].end_rel_frac.quantile([.25, .5, .75]).round(2).tolist())
print("    join_vocab_frac quartiles of the join-failers",
      sub[fail_j].join_vocab_frac.quantile([.25, .5, .75]).round(3).tolist())
# share of „ among all chars where it fails rel: is „ just frequent everywhere?
print("    median all_freq of „ (rel-failers vs passers):",
      int(sub[fail_r].all_freq.median()), int(sub[~fail_r].all_freq.median()))
rng = random.Random(505)
sample = rng.sample(sorted(sub.index, key=lambda s: (len(s), s)), 6)
print("    sample", sample)
for inv in sample:
    r = sub.loc[inv]
    print(f"== {inv} n_lines={r.n_lines} att={r.n_end_attached} jv={r.join_vocab_frac:.3f} rel={r.end_rel_frac:.2f}")
    prev, found = None, 0
    with gzip.open(V1 + f"lines/{inv}.lines.tsv.gz", "rt", encoding="utf-8") as h:
        h.readline()
        for k, line in enumerate(h):
            p = line.rstrip("\n").split("\t")
            if (prev and prev[1] == p[1] and prev[0] == p[0] and prev[4].endswith("„")
                    and len(prev[4]) > 3 and prev[4][-2].isalpha()):
                found += 1
                if found in (3, 11, 29):
                    print("   ", repr(prev[4][-45:]), "|", repr(p[4][:45]))
                if found == 29:
                    break
            prev = p
