"""Write fresh_gold.tsv / fresh_sides.tsv from the hand labels below (read by the step-19g session; no rule or script labels).

Labels are stored as: default letter + {letter: [k, ...]} overrides. Lines are listed with show_side.listing, so k matches the printed listing.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import show_side as ss

def spec(n, default, **kw):
    lab = {k: default for k in range(1, n + 1)}
    for letter, ks in kw.items():
        for k in ks:
            lab[k] = {"Q": "?"}.get(letter, letter)
    return lab

R = lambda a, b: list(range(a, b + 1))
LAB = {
 101: spec(21, "S", H=[1,6,10,13,15,19,20], C=[3,5,9,17], Q=[18]),
 102: spec(51, "S", H=[8,25,36,38], C=[23,44,46,48,51]),
 103: spec(30, "S", H=R(1,14)+[18], C=[16,17,20,21,30]),
 104: spec(43, "S", H=[1,20,21,31], C=[6,8,11,13,18,23,24,25,27,28,29,30,33,34,35,39,40,43]),
 105: spec(38, "S", H=[1,6,8,15,20,21,22], C=[3,17,19,26,28,33,35,36,37]),
 106: spec(21, "C", H=[1,2], S=[3,4,5,6,9,13,18]),
 107: spec(44, "C", H=[1,23,24,32,33], S=[2,7,9,11,12,14,17,19,21,22,25,26,27,29,34,36,38,41,43]),
 110: spec(30, "C", H=[1,15,16,17,30], S=[2,4,6,8,10,13,18,20,24,26,27]),
 111: spec(50, "S", H=[1,26,31,44], C=[7,10,13,17,22,28,30,33,35,36,38,40,41,43,46,48,49,50]),
 112: spec(83, "S", H=[3,4,10,36,37,38,39,44,45,53], C=[1,9,14,16,22,26,29,34,40,42,51,58,60,63,65,72,74,76,78,81,83]),
 113: spec(39, "C", H=[1,37,38,39], S=[2,5,7,9,10,13,15,17,19,21,22,25,27,28,30,32,33,35]),
 114: spec(48, "C", H=[1,31,32,38,39,40,47,48], Q=[5,43], S=[2,6,8,11,12,15,17,23,27,33,34,37,41,45]),
 115: spec(35, "S", C=[2,5,27,31,33], H=[28]),
 116: spec(44, "C", H=[1,44], S=[2,3,4,8,15,20,21,28,31,34,40]),
 117: spec(26, "S", C=[2,4,6,8,19,23]),
 118: spec(36, "S", H=[1,12], C=[3,5,7,8,11,17,29,35]),
 119: spec(44, "C", H=[1,2,14,15], S=[3,5,6,8,11,16,21,25,28,30,33,36,38,41,42,43]),
 120: spec(93, "C", H=[1,2,45], Q=[46], S=[3,7,12,14,19,24,25,29,33,37,41,56,58,61,66,71,76,79,84,85,90]),
 121: spec(51, "S", H=[1,2,3,28], C=[8,10,13,17,21,41,51]),
 125: spec(41, "C", H=[1,2,6,7], S=[3,8,15,18,20,23,26,27,28,30,34,37,40]),
}
NOTES = {
 (105,2): "merged", (110,13): "merged", (113,2): "merged", (114,18): "merged", (115,1): "merged",
 (117,1): "merged", (117,7): "merged", (117,15): "merged", (120,3): "merged", (121,14): "merged",
 (121,16): "merged", (121,20): "merged", (113,3): "midline_start", (113,8): "midline_start",
 (113,34): "midline_start", (119,4): "midline_start", (125,10): "midline_start", (125,13): "midline_start",
 (125,32): "midline_start", (112,1): "ref_before_lemma", (112,22): "ref_before_lemma",
 (114,5): "heading or entry start", (114,43): "entry start lost",
 (101,18): "heading tail or continuation", (120,46): "entry start lost",
 (103,1): "col 1 is a list without references: H", (102,39): "sub-entry with own page ref",
}
VERD = {108: ("not index", ""), 109: ("unreadable input", ""), 121: ("index", "108"), 125: ("index", "109")}
rows = {}
with open(ss.FRESH) as f:
    head = f.readline().rstrip("\n").split("\t")
    for line in f:
        r = dict(zip(head, line.rstrip("\n").split("\t")))
        rows[int(r["read_id"])] = r
order = list(range(101, 121)) + [121, 125]
gold, sides = [], []
for rid in order:
    r = rows[rid]
    if rid in (108, 109):
        v, u = VERD[rid]
        sides.append([rid, r["group"], r["role"], v, u, "", "", "", ""])
        continue
    v, u = VERD.get(rid, ("index", ""))
    lines = ss.listing(ss.side_lines(r))
    lab = LAB[rid]
    assert len(lab) == len(lines), (rid, len(lab), len(lines))
    cnt = dict(S=0, C=0, H=0)
    nq = 0
    for k, l in enumerate(lines, 1):
        x = lab[k]
        if x == "?": nq += 1
        else: cnt[x] += 1
        gold.append([rid, r["scan_id"], r["page"], l["line_id"], k, x, NOTES.get((rid, k), "")])
    sides.append([rid, r["group"], r["role"], v, u, cnt["S"], cnt["C"], cnt["H"], nq])
out = Path(__file__).parent
with open(out / "fresh_gold.tsv", "w") as f:
    f.write("read_id\tscan_id\tpage\tline_id\tk\tlabel\tnote\n")
    for g in gold: f.write("\t".join(map(str, g)) + "\n")
with open(out / "fresh_sides.tsv", "w") as f:
    f.write("read_id\tgroup\trole\tside_verdict\tused_for\tn_S\tn_C\tn_H\tn_unclear\n")
    for s in sides: f.write("\t".join(map(str, s)) + "\n")
print(open(out / "fresh_sides.tsv").read())
tot = [sum(int(s[i]) for s in sides if s[5] != "") for i in (5, 6, 7, 8)]
print("totals S C H ?", tot, "index sides", sum(1 for s in sides if s[3] == "index"))
