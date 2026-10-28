"""Phase-13 review: the proposed split v2 rule (step 19f brief) applied to the 40 B1 regions of step 19b.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/split_v2_on_b1.py

Mechanical only (no verdicts): per B1 case, whether v2 splits the region, the left / right / wide line counts, and
whether the right piece starts above the left piece (the order risk named in the brief). The 19b verdict is shown
next to it. v2: steps 1-3 of P6 unchanged (W, mh, candidates, largest gap, sx); a line is left if x < sx - tol, else
right; a left line is wide if x + w > sx + span_reach_frac * median w of the right lines (0.5); guards as P6 with
wide lines in the span count; pieces: left (incl. wide) and right.
"""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
C = {"min_lines": 3, "min_gap_frac": 0.25, "max_col_frac": 0.65, "min_col_w_frac": 0.3, "tol_h": 0.5,
     "min_overlap_h": 2.0, "max_span_frac": 0.34, "span_reach_frac": 0.5}
hand_y = set((A / "planning/hand_15y-b.txt").read_text().split())


def v2(lines):
    T = [l for l in lines if l["text"].strip()]
    if len(T) < 2 * C["min_lines"]:
        return "too few lines"
    x0 = min(l["x"] for l in T)
    W = max(l["x"] + l["w"] for l in T) - x0
    mh = statistics.median(l["h"] for l in T)
    N = sorted((l for l in T if l["w"] <= C["max_col_frac"] * W), key=lambda l: (l["x"], l["y"]))
    if len(N) < 2 * C["min_lines"]:
        return "too few candidates"
    gap, gi = max((N[i + 1]["x"] - N[i]["x"], -i) for i in range(len(N) - 1))
    if gap < C["min_gap_frac"] * W:
        return "gap guard"
    sx, tol = N[-gi + 1]["x"], C["tol_h"] * mh
    L = [l for l in T if l["x"] < sx - tol]
    R = [l for l in T if l["x"] >= sx - tol]
    if len(R) < C["min_lines"]:
        return "min_lines R"
    wr = statistics.median(l["w"] for l in R)
    wide = [l for l in L if l["x"] + l["w"] > sx + C["span_reach_frac"] * wr]
    Ln = [l for l in L if l not in wide]
    if len(Ln) < C["min_lines"]:
        return "min_lines L"
    if len(wide) > C["max_span_frac"] * len(T):
        return f"span guard ({len(wide)})"
    for part in (Ln, R):
        if statistics.median(l["w"] for l in part) < C["min_col_w_frac"] * W:
            return "width guard"
    ov = (min(max(l["y"] + l["h"] for l in Ln), max(l["y"] + l["h"] for l in R))
          - max(min(l["y"] for l in Ln), min(l["y"] for l in R)))
    if ov < C["min_overlap_h"] * mh:
        return "overlap guard"
    r_first = min(l["y"] for l in R) < min(l["y"] for l in L)
    return f"split L {len(Ln)} wide {len(wide)} R {len(R)}" + ("  R-TOP-ABOVE-L" if r_first else "")


def main():
    cases = list(csv.DictReader(open(A / "step-19b/b1_verdicts.tsv"), delimiter="\t"))
    cache = {}
    for c in cases:
        inv = c["inv"]
        if inv not in cache:
            base = DEV / ("15y-b" if inv in hand_y else "15z-b") / L4
            reg = defaultdict(list)
            for r in read_tsv_gz(base / "lines" / f"{inv}.lines.tsv.gz"):
                reg[r["region_id"]].append({"x": int(float(r["x"])), "y": int(float(r["y"])), "w": int(float(r["w"])),
                                            "h": int(float(r["h"])), "text": r["text"]})
            cache[inv] = reg
        res = v2(cache[inv][c["region_id"]])
        print(f"{c['case']} {c['side_class']:10s} 19b={c['verdict']:16s} v2: {res:40s} | {c['note'][:60]}")


if __name__ == "__main__":
    main()
