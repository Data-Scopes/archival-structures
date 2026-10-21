"""Step 15i2: per-inventory byte comparison (decompressed) of 15i-b against 15ir-a, all files, plus wbc sets."""
import gzip, json
from pathlib import Path
A = Path("data/text_layer_v2_dev/15ir-a"); B = Path("data/text_layer_v2_dev/15i-b")
def rd(p): return gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
def root(b, k): return next((b / k).glob("*_text_layer_v2"))
res = {"print_equal_bytes": None, "hand": {}, "other_files": {}}
ra, rb = root(A, "hand"), root(B, "hand")
INV = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
per = {i: {"equal": [], "differ": [], "missing": []} for i in INV}
other = {"equal": [], "differ": [], "missing": []}
files = sorted({p.relative_to(ra) for p in ra.rglob("*") if p.is_file()} | {p.relative_to(rb) for p in rb.rglob("*") if p.is_file()})
for f in files:
    parts = f.parts
    inv = next((i for i in INV if f.name.split(".")[0] == i), None)
    tgt = per[inv] if inv else other
    pa, pb = ra / f, rb / f
    if not pa.exists() or not pb.exists(): tgt["missing"].append(str(f))
    elif rd(pa) == rd(pb): tgt["equal"].append(str(f))
    else: tgt["differ"].append(str(f))
for i in INV:
    d = per[i]
    res["hand"][i] = {"n_equal": len(d["equal"]), "differ": d["differ"], "missing": d["missing"]}
res["other_files"] = {"n_equal": len(other["equal"]), "differ": other["differ"], "missing": other["missing"]}
pe = pd_ = 0; pdiff = []
for pa in sorted((A / "print").rglob("*")):
    if pa.is_file():
        pb = B / pa.relative_to(A)
        if pb.exists() and pa.read_bytes() == pb.read_bytes(): pe += 1
        else: pdiff.append(str(pa))
nb = sum(1 for p in (B / "print").rglob("*") if p.is_file())
res["print"] = {"files_a": pe + len(pdiff), "files_b": nb, "equal_bytes": pe, "differ": pdiff}
def wbc(r):
    out = {}
    for l in (r / "wbc" / "wbc_per_inventory.tsv").read_text().splitlines()[1:]:
        c = l.split("\t"); out[c[0]] = c
    return out
cols = (rb / "wbc" / "wbc_per_inventory.tsv").read_text().splitlines()[0].split("\t")
ia = cols.index("wbc_set") if "wbc_set" in cols else None
res["wbc_sets_b"] = {k: v[ia] for k, v in wbc(rb).items()}
res["wbc_sets_a"] = {k: v[ia] for k, v in wbc(ra).items()}
res["sanity_pass"] = json.loads(rd(rb / "wbc" / "corpus_default.json")).get("sanity_pass")
json.dump(res, open(Path(__file__).parent / "repro.json", "w"), indent=1, ensure_ascii=False)
for i in INV: print(i, res["hand"][i]["n_equal"], res["hand"][i]["differ"], res["hand"][i]["missing"])
print("other", res["other_files"]); print("print", res["print"]); print(res["wbc_sets_b"], res["sanity_pass"])
