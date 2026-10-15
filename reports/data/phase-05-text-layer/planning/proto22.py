"""Full 22-inventory rebuild under AS .venv vs NLP output/voc_text_layer/voc_text_layer_v1 (local-input build)."""
import filecmp, importlib.util, json, sys, time
from pathlib import Path
import pandas as pd
NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
REF = NLP / "output/voc_text_layer/voc_text_layer_v1"
OUT = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("vtl", Path(__file__).with_name("vtl_orig.py"))
vtl = importlib.util.module_from_spec(spec); spec.loader.exec_module(vtl)
src = vtl.discover_inventories(NLP / "pagexml")
invs = sorted([p.name.split(".")[0] for p in (REF / "lines").glob("*.lines.tsv.gz")], key=vtl.inventory_sort_key)
t0 = time.time()
for inv in invs:
    vtl.extract_inventory(inv, src[inv], OUT); vtl.wbc_stats_part(inv, OUT)
print("extract+wbc parts", round(time.time() - t0), "s", flush=True)
parts = [pd.read_csv(OUT / "wbc/parts" / f"{inv}.tsv", sep="\t", quoting=3, dtype={"inventory_number": str, "char": str},
                     keep_default_na=False, na_values=[]) for inv in invs]
stats = pd.concat(parts, ignore_index=True)
vtl.write_plain_tsv(OUT / "wbc/wbc_stats.tsv", vtl.WBC_STATS_COLUMNS,
                    [[vtl._fmt(v) for v in row] for row in stats[vtl.WBC_STATS_COLUMNS].itertuples(index=False)])
first = stats.drop_duplicates("inventory_number").set_index("inventory_number")["n_lines"].to_dict()
per_inv, corpus = vtl.decide_wbc(stats, {inv: int(first.get(inv, 0)) for inv in invs})
corpus["allow_partial"] = False; corpus["n_inventories_missing_extract"] = 0
vtl.write_json_atomic(OUT / "wbc/corpus_default.json", corpus)
cols = ["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default", "char_status"]
vtl.write_plain_tsv(OUT / "wbc/wbc_per_inventory.tsv", cols, [[r[c] for c in cols] for r in per_inv])
sets, default = vtl.load_wbc_sets(OUT)
for inv in invs:
    vtl.join_inventory(inv, OUT, sets.get(inv, default), write_events=True)
print("total", round(time.time() - t0), "s")
bad = []
n = 0
for rel in ["wbc/wbc_stats.tsv", "wbc/corpus_default.json", "wbc/wbc_per_inventory.tsv"] + [
        f"{d}/{inv}{'.lines' if d == 'lines' else ''}.tsv{'' if d == 'wbc/parts' else '.gz'}"
        for inv in invs for d in ["lines", "regions_meta", "regions", "join_events", "wbc/parts"]]:
    n += 1
    if not filecmp.cmp(OUT / rel, REF / rel, shallow=False): bad.append(rel)
for inv in invs:
    for js in ["extract_stats", "join_stats"]:
        a = json.loads((OUT / js / f"{inv}.json").read_text()); b = json.loads((REF / js / f"{inv}.json").read_text())
        a.pop("source", None); b.pop("source", None); n += 1
        if a != b: bad.append(f"{js}/{inv}")
print(f"compared {n}, differing {len(bad)}: {bad[:20]}")
