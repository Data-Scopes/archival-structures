import filecmp, re, sys, time
from pathlib import Path
import pandas as pd
from archival_structures.text_layer.io import *
from archival_structures.text_layer.word_breaks import *

V1 = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1")
OUT = Path(sys.argv[1]); OUT.mkdir(parents=True, exist_ok=True)
names = [p.name[:-len(".lines.tsv.gz")] for p in (V1/"lines").glob("*.lines.tsv.gz")]
print("n names", len(names))
order = sorted(names, key=inventory_sort_key)
def nlp_key(inv):
    m = re.match(r"(\d+)(.*)", inv)
    return (int(m.group(1)), m.group(2)) if m else (10**9, inv)
nlp_order = sorted(names, key=nlp_key)
print("4. sort order equal:", order == nlp_order, len(order))

stats = pd.read_csv(V1/"wbc/wbc_stats.tsv", sep="\t", quoting=3, dtype={"inventory_number": str, "char": str}, keep_default_na=False, na_values=[])
nl = stats.drop_duplicates("inventory_number").set_index("inventory_number")["n_lines"].to_dict()
inventory_lines = {i: int(nl.get(i, 0)) for i in order}
print("inventories without stats rows:", sum(1 for i in order if i not in nl))
per, corpus = decide_word_breaks(stats, inventory_lines)
corpus["allow_partial"] = False
corpus["n_inventories_missing_extract"] = 0
(OUT/"wbc").mkdir(exist_ok=True)
write_json_atomic(OUT/"wbc/corpus_default.json", corpus)
cols = ["inventory_number","n_lines","mode","wbc_set","differs_from_default","char_status"]
write_plain_tsv(OUT/"wbc/wbc_per_inventory.tsv", cols, [[r[c] for c in cols] for r in per])
for f in ("corpus_default.json","wbc_per_inventory.tsv"):
    print("1.", f, "identical:", filecmp.cmp(OUT/"wbc"/f, V1/"wbc"/f, shallow=False))
print("default set", corpus["default_set"])
print("differs", sum(r["differs_from_default"] for r in per), "whole_default", sum(r["mode"]=="whole_default" for r in per))

write_plain_tsv(OUT/"wbc/wbc_stats.tsv", WORD_BREAK_STATS_COLUMNS,
    [[format_stat(v) for v in row] for row in stats[WORD_BREAK_STATS_COLUMNS].itertuples(index=False, name=None)])
print("2. stats rewrite identical:", filecmp.cmp(OUT/"wbc/wbc_stats.tsv", V1/"wbc/wbc_stats.tsv", shallow=False))

invs = "1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split() \
 + "1084 1851 2045 2118 2156 2181 2243 2404 2503 2608 2957 3252 3275 3710 3848 4345 8147 8449 8503 8715 8866 9120 9130 9294 9478 9724 10582 10926".split() + ["9626A","11013B"]
print("n parts invs", len(invs))
ok = 0
(OUT/"parts").mkdir(exist_ok=True)
t=time.time()
for inv in invs:
    rows, _ = inventory_word_break_stats(inv, V1)
    write_plain_tsv(OUT/"parts"/f"{inv}.tsv", WORD_BREAK_STATS_COLUMNS, [[format_stat(r[c]) for c in WORD_BREAK_STATS_COLUMNS] for r in rows])
    same = filecmp.cmp(OUT/"parts"/f"{inv}.tsv", V1/"wbc/parts"/f"{inv}.tsv", shallow=False)
    ok += same
    if not same: print("DIFF", inv)
print(f"3. parts identical {ok}/{len(invs)} in {time.time()-t:.0f}s")
