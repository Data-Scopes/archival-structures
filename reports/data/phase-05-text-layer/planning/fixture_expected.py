import importlib.util, sys, gzip, json
from pathlib import Path
spec = importlib.util.spec_from_file_location("vtl", Path(__file__).with_name("vtl_orig.py"))
vtl = importlib.util.module_from_spec(spec); spec.loader.exec_module(vtl)
root = Path(sys.argv[1]); out = Path(sys.argv[2])
inv = vtl.discover_inventories(root)
print(list(inv))
print(vtl.extract_inventory("9999A", inv["9999A"], out))
for d, f in [("lines", "9999A.lines.tsv.gz"), ("regions_meta", "9999A.tsv.gz")]:
    print(repr(gzip.decompress((out / d / f).read_bytes()).decode()))
d = json.loads((out / "extract_stats/9999A.json").read_text()); d.pop("source")
d["parse_errors"] = [e.split("/")[-1] for e in d["parse_errors"]]
d["scan_id_mismatch"] = [e.split("/")[-1] for e in d["scan_id_mismatch"]]
print(d)
print(vtl.join_inventory("9999A", out, {"¬", "„"}, write_events=True))
print(repr(gzip.decompress((out / "regions/9999A.tsv.gz").read_bytes()).decode()))
print(repr(gzip.decompress((out / "join_events/9999A.tsv.gz").read_bytes()).decode()))
j = json.loads((out / "join_stats/9999A.json").read_text()); print(j["join"], j["extract_counts"])
