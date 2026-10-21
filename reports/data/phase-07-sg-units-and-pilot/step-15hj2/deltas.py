"""Step 15hj2 criterion 4: merges per break char, before (15hj-a) and after (15hj-b)."""
import json
from pathlib import Path
H = Path(__file__).parent
a = json.loads((H.parent / "step-15hj" / "acceptance.json").read_text())["crit5"]
b = json.loads((H / "acceptance.json").read_text())["crit5"]
CL = ("lower", "upper", "digit", "other")

def tab(c):
    n = sum(c.get(k, 0) for k in CL)
    chars = {}
    for k, v in c.items():
        if k.startswith("char|"):
            _, ch, cls = k.split("|")
            d = chars.setdefault(ch, {"n": 0, "lower": 0})
            d["n"] += v
            d["lower"] += v if cls == "lower" else 0
    return n, c.get("lower", 0), chars

out = {}
for inv in a:
    na, la, ca = tab(a[inv]); nb, lb, cb = tab(b[inv])
    delta = {}
    for ch in sorted(set(ca) | set(cb)):
        x = ca.get(ch, {"n": 0, "lower": 0}); y = cb.get(ch, {"n": 0, "lower": 0})
        delta[ch] = {"before": [x["n"], x["lower"]], "after": [y["n"], y["lower"]],
                     "added": y["n"] - x["n"], "added_lower": y["lower"] - x["lower"]}
    out[inv] = {"before": [na, la, round(la / na, 4) if na else None],
                "after": [nb, lb, round(lb / nb, 4) if nb else None], "chars": delta}
    print(inv, "merges", na, "->", nb, f"lower {la}/{na} -> {lb}/{nb}",
          {ch: (d["before"][0], d["after"][0]) for ch, d in delta.items() if d["added"]})
(H / "deltas.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
