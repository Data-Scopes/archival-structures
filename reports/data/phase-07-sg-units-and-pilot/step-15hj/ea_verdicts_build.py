"""Writes ea_verdicts.tsv from ea_sample.txt. The verdicts are the hand-read judgements of step 15hj
(index = line order within each group of ea_sample.txt)."""
from collections import Counter
from pathlib import Path

D = Path(__file__).parent
rows = [l.rstrip("\n").split("\t") for l in open(D / "ea_sample.txt", encoding="utf-8") if l.strip()]


def pick(g):
    return [r for r in rows if r[0] == g]


def mk(section, items, right=(), doubt=(), noise=(), right_cat="", wrong_cat="", doubt_cat=""):
    out = []
    for i, r in enumerate(items, 1):
        if i in right:
            v, c = "ok", right_cat
        elif i in noise:
            v, c = "ok", right_cat + " (OCR noise in the joined word)"
        elif i in doubt:
            v, c = "doubtful", doubt_cat
        else:
            v, c = "wrong", wrong_cat
        out.append((section, f"{r[1]} {r[4][-24:]} | {r[5][:18]}", v, c))
    return out


out = []
out += mk("merge_hand", pick("merge_hand"), right=range(1, 16), right_cat="genuine word break, joined word correct")
out += mk("merge_print", pick("merge_print"), right=[1, 2, 6, 7, 9, 10, 11, 12, 13, 14, 15], noise=[3, 8, 4, 5],
          right_cat="genuine word break, joined word correct")
out += mk("guarded", pick("guarded"), right=[1, 3, 4, 6, 11, 12, 13, 17, 18, 19, 20], doubt=[8, 9, 10],
          right_cat="guard right: not a word break",
          wrong_cat="guard wrong: genuine break with OCR-garbled letters (digit for letter)", doubt_cat="unclear")
out += mk("merge_upper_digit", pick("merge_upper_digit"), right=[3, 13, 14], noise=[20], doubt=[4],
          right_cat="genuine break, OCR upper-case letter",
          wrong_cat="wrong merge: sentence/paragraph start or hyphenated compound after the break char",
          doubt_cat="unclear")
out += mk("char_.455", pick("char_.455"), right=[2, 6, 8, 9, 10, 11, 12, 13, 14, 15, 20],
          right_cat="'.' is the OCR reading of a hyphen", wrong_cat="wrong: '.' ends a sentence or abbreviation")
out += mk("char_.3822", pick("char_.3822"), right=[1, 3], noise=[8, 9], doubt=[7],
          right_cat="'.' is the OCR reading of a hyphen", wrong_cat="wrong: '.' ends a sentence or abbreviation",
          doubt_cat="unclear")
out += mk("char_:11281", pick("char_:11281"), right=range(2, 11), doubt=[1], right_cat="':' is the scribe's hyphen",
          doubt_cat="both-ends fix on 'lager:' + ':stond'")
out += mk("char_=", pick("char_="), right=[4, 5, 6, 7, 8], noise=[1, 9], doubt=[3], right_cat="genuine break",
          wrong_cat="wrong: hyphenated compound loses its hyphen", doubt_cat="unclear")
for r in pick("print_unit_ends_dash"):
    out.append(("print_unit_ends_dash", f"{r[1]} ...{r[3][-28:]} ({r[4]} segs)", "listed",
                "for 15i: unit ends mid-word, continuation is in another unit"))
with open(D / "ea_verdicts.tsv", "w", encoding="utf-8") as f:
    f.write("section\titem\tverdict\tcategory\n")
    for r in out:
        f.write("\t".join(r) + "\n")
for k, v in sorted(Counter((r[0], r[2]) for r in out).items()):
    print(k, v)
