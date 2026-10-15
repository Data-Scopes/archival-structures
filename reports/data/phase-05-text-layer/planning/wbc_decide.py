"""C14 check: decide_wbc from v1 wbc_stats.tsv reproduces wbc_per_inventory.tsv and corpus_default.json."""
import filecmp, importlib.util, json, sys
from pathlib import Path
import pandas as pd
NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
V1 = NLP / "data" / "voc_text_layer_v1"
OUT = Path(sys.argv[1]) / "wbc"; OUT.mkdir(parents=True, exist_ok=True)
spec = importlib.util.spec_from_file_location("vtl", Path(__file__).with_name("vtl_orig.py"))
vtl = importlib.util.module_from_spec(spec); spec.loader.exec_module(vtl)
stats = pd.read_csv(V1 / "wbc/wbc_stats.tsv", sep="\t", quoting=3, dtype={"inventory_number": str, "char": str},
                    keep_default_na=False, na_values=[])
print("stats", stats.shape, "invs", stats.inventory_number.nunique())
# order: as the build script, inventories = discover order (sorted by inventory_sort_key), done = all
invs = sorted({p.name[:-len(".lines.tsv.gz")] for p in (V1 / "lines").glob("*.lines.tsv.gz")}, key=vtl.inventory_sort_key)
print("inventories", len(invs))
def nl(inv):
    rows = stats[stats["inventory_number"] == inv]
    return int(rows["n_lines"].iloc[0]) if len(rows) else 0
first = stats.drop_duplicates("inventory_number").set_index("inventory_number")["n_lines"].to_dict()
inventory_lines = {inv: int(first.get(inv, 0)) for inv in invs}
per_inv, corpus = vtl.decide_wbc(stats, inventory_lines)
corpus["allow_partial"] = False; corpus["n_inventories_missing_extract"] = 0
vtl.write_json_atomic(OUT / "corpus_default.json", corpus)
cols = ["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default", "char_status"]
vtl.write_plain_tsv(OUT / "wbc_per_inventory.tsv", cols, [[r[c] for c in cols] for r in per_inv])
for f in ["corpus_default.json", "wbc_per_inventory.tsv"]:
    print(f, "byte-identical:", filecmp.cmp(OUT / f, V1 / "wbc" / f, shallow=False))
print("default", corpus["default_set"], "differs", sum(r["differs_from_default"] for r in per_inv),
      "whole_default", sum(r["mode"] == "whole_default" for r in per_inv))
# also: stats re-written identically?
vtl.write_plain_tsv(OUT / "wbc_stats.tsv", vtl.WBC_STATS_COLUMNS,
                    [[vtl._fmt(v) for v in row] for row in stats[vtl.WBC_STATS_COLUMNS].itertuples(index=False)])
print("wbc_stats.tsv rewrite byte-identical:", filecmp.cmp(OUT / "wbc_stats.tsv", V1 / "wbc/wbc_stats.tsv", shallow=False))
