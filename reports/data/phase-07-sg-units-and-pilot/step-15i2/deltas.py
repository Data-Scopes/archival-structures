"""Step 15i2: merges per inventory in 15i-a vs 15i-b (hand_tar), lower-case right-side share (15hj measure),
and the removed `=` merges (A merges at `=`) written to removed_eq.tsv."""
import collections, json, random
import pandas as pd
from pathlib import Path
O = Path(__file__).parent
def root(b): return next((Path(b) / "hand").glob("*_text_layer_v2"))
def rd(r, t, inv): return pd.read_csv(r / t / f"{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
def merges(r, inv):
    seg, un = rd(r, "join/segments", inv), rd(r, "join/units", inv)
    wbc = un["word_break_chars"].iloc[0]
    text = {(a, b): t for a, b, t in zip(un.scan_id, un.unit_id, un.text)}
    out = []
    for k, g in seg.groupby(["scan_id", "unit_id"], sort=False):
        g = g.sort_values("seg_order", key=lambda s: s.astype(int)).to_dict("records")
        for a, b in zip(g, g[1:]):
            if a["boundary_kind"] == "merge":
                t = text[k]; ch = t[int(b["char_start"]):].lstrip(wbc)[:1]
                out.append({"scan": k[0], "unit": k[1], "bc": a["break_char_end"], "ch": ch,
                            "ctx": t[max(0, int(a["char_end"]) - 30):int(a["char_end"])] + "|" + t[int(b["char_start"]):int(b["char_start"]) + 30]})
    return out
ra, rb = root("data/text_layer_v2_dev/15i-a"), root("data/text_layer_v2_dev/15i-b")
res = {}; removed = []
for inv in "3349 3350 3351 3352 3353 3354".split():
    ma, mb = merges(ra, inv), merges(rb, inv)
    def st(m): 
        n = len(m); lo = sum(x["ch"].islower() for x in m)
        return {"merges": n, "lower": lo, "lower_share": round(lo / n, 4) if n else None,
                "by_break_char": dict(collections.Counter(x["bc"] for x in m))}
    res[inv] = {"a": st(ma), "b": st(mb), "removed": len(ma) - len(mb),
                "eq_a": sum(x["bc"] == "=" for x in ma), "eq_b": sum(x["bc"] == "=" for x in mb)}
    removed += [dict(x, inv=inv) for x in ma if x["bc"] == "="]
tot = lambda k, f: sum(res[i][k][f] for i in res)
res["TOTAL_3349_3350_3353"] = {"removed": sum(res[i]["removed"] for i in ("3349", "3350", "3353")),
    "a": {"merges": sum(res[i]["a"]["merges"] for i in ("3349","3350","3353")), "lower": sum(res[i]["a"]["lower"] for i in ("3349","3350","3353"))},
    "b": {"merges": sum(res[i]["b"]["merges"] for i in ("3349","3350","3353")), "lower": sum(res[i]["b"]["lower"] for i in ("3349","3350","3353"))}}
t = res["TOTAL_3349_3350_3353"]
for k in "ab": t[k]["lower_share"] = round(t[k]["lower"] / t[k]["merges"], 4)
res["TOTAL_hand_tar"] = {k: {"merges": tot(k, "merges"), "lower": tot(k, "lower")} for k in "ab"}
for k in "ab": res["TOTAL_hand_tar"][k]["lower_share"] = round(res["TOTAL_hand_tar"][k]["lower"] / res["TOTAL_hand_tar"][k]["merges"], 4)
json.dump(res, open(O / "deltas.json", "w"), indent=1, ensure_ascii=False)
pd.DataFrame(removed).to_csv(O / "removed_eq.tsv", sep="\t", index=False)
for k, v in res.items(): print(k, json.dumps(v, ensure_ascii=False))
