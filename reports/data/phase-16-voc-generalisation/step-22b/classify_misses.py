"""Hand classification of the read misses (selected cell V0-off-TL): joins the notes below to the two sample lists.
Run from the AS root with .venv/bin/python -I. Output: misses_read.tsv. The numbers are the #n in
dev_misses_context.txt / test_misses_context.txt (order of dev_misses_sample.tsv / test_misses_list.tsv)."""
import csv
from pathlib import Path

D = Path(__file__).parent
G, H, T, X, S, R, E, F, M, U = ("gold_error", "htr_misread", "tied_or_duplicated", "extrapolation_beyond_chain",
    "side_layout_offset", "restart_second_numbering", "gap_no_number_near_gold", "front_matter_excluded",
    "several_numbers_on_side", "unclear")
dev = {1: (E, "2907 on 21, 2998 on 25; none on 22-24; interpolation 1 scan late"), 2: (H, "1302 read for 1102 on the gold scan; 1101 on 1053"),
 3: (R, "56 on 61 and 57 on 65, 53 on 64: two series in a portrait-scan volume"), 4: (U, "1971 on 23, 1972 on 27, gold 25 between"),
 5: (T, "1129 on the gold scan but 1121/1131/1132/1132 around it; chain rejects it"), 6: (S, "2585 on 633, 2586 on 635, gold 634 (unnumbered scan before)"),
 7: (T, "94 on gold 201 and on 203"), 8: (U, "389 on 907, gold 893 prints 1584/24"), 9: (R, "folio 1 at 14 and a second series from about 314"),
 10: (R, "1 on 62, a second series 2,3,4 from 107"), 11: (X, "folio 1, no numbers near 9; extrapolated"), 12: (E, "3 on 23; 4 expected on 25 is blank"),
 13: (T, "76 on 87 and 88"), 14: (T, "6119 on 253 and 254"), 15: (T, "1206 on 645, 647 and 649 with a repeated 1213"),
 16: (S, "15 on 37, 16 on 39, gold 38"), 17: (X, "gold beyond the last chain point: no map"), 18: (E, "no number on 844-850"),
 19: (T, "1829 on 715 and 716"), 20: (G, "gold scan prints 1531, gold folio 1530 (printed on 973)"), 21: (X, "folio 1, extrapolated"),
 22: (G, "gold scan prints 1129 for gold folio 1124"), 23: (S, "12 on 43, gold 41 (2 scans before the printed number)"),
 24: (R, "2 on 19, again 2 on 51 and 52: second series"), 25: (H, "gold scan reads 1o. 19 for 1119; 1120 on 9, 1121 on 10"),
 26: (G, "43 on 129, 45 on 131 so folio 44 is on 130; gold says 132"), 27: (T, "1382 repeated on every recto 164-172"),
 28: (U, "9 on 29; gold 36 has no number"), 29: (S, "3874 on recto of 24, gold 23 (scan before)"),
 30: (S, "1428 on 39, gold 37; 142/1488 on 35"), 31: (E, "no numbers near 33"), 32: (T, "203 on 455, 457 and 459"),
 33: (T, "65 on the gold scan, but a repeated 211 and a short chain (13)"), 34: (R, "1 on 16, gold 33 is a later series"),
 35: (H, "54 on 163 dropped next to 55 on 161 and 53"), 36: (M, "gold scan: 2, 61, 1, 3; the first match (2) wins"),
 37: (M, "gold scan: 0, 3679, Seert; the first match (0) wins"), 38: (T, "823 on 769, 835 on 769 and 771; gold folio not printed"),
 39: (M, "gold scan: 402, 401; the first match (402, a repeated header) wins"), 40: (E, "625 on 244, 627 on 248; none on 245-247")}
test = {1: (T, "1465 on 461, 463 and 465"), 2: (T, "10 on 37 and on 60"), 3: (T, "1037 repeated on 75-79"), 4: (E, "only 15 on 43; none near 41"),
 5: (E, "172 on 179, gold 177, short chain (30)"), 6: (R, "75 on the gold scan and on 193: two series"), 7: (M, "gold scan: 4, 442; first match wins; 442 also on 73"),
 8: (M, "gold scan: 389, 8, 387; first match wins"), 9: (G, "gold scan prints 94, gold folio 95"), 10: (S, "1306 on 61, gold 60 (blank scan before)"),
 11: (E, "almost no numbers on 16-23"), 12: (T, "154 on 417 and 425"), 13: (R, "47,48,49 at 211-215 and again at 63-65"),
 14: (M, "425: 2172, 2155; first match 2172 wins; 2156 expected on 427"), 15: (R, "231/233 on 43/45 and 232 on 63: two series"),
 16: (T, "605 on 637 and 639, 607 on 651"), 17: (H, "111 read for 121 on 297; 121 on 301"), 18: (R, "1169-1193 on 13-19 are a separate early series; chain starts at 240"),
 19: (R, "folio 1 at 855, no number there; new series after 687@716"), 20: (G, "1923 on 467, gold 468 for 1916 (printed on 453)"),
 21: (M, "gold scan: 1467., 1463.; first match 1467 (repeated header) wins"), 22: (E, "45 on 63, 46 expected on the blank recto 64"),
 23: (E, "168 on 19, 4 on 23; none on 21"), 24: (T, "1275 on 20 and 21"), 25: (G, "gold scan prints 863, folio 865 printed on 905"),
 26: (S, "1910 on 453, 191 on 455, gold 454"), 27: (G, "3631 printed on 709, gold scan 695 prints 18"), 28: (X, "folio 1; extrapolated"),
 29: (G, "12 on 29, 14 on 33 so 13 is on 31; gold says 14 on 31"), 30: (G, "1344 on 623, 1101 printed on 137; gold 624")}


def rows(sample, notes, split):
    out = []
    for i, r in enumerate(csv.DictReader(open(D / sample), delimiter="\t"), 1):
        cat, note = notes[i]
        out.append([f"{split}{i}", r["inv"], r["folio"], r["gold_scan"].rsplit("_", 1)[1],
                    r["pred_scan"].rsplit("_", 1)[1] if r["pred_scan"] else "", cat, note])
    return out


with open(D / "misses_read.tsv", "w", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["read_id", "inv", "folio", "gold_scan", "pred_scan", "category", "note"])
    w.writerows(rows("dev_misses_sample.tsv", dev, "dev") + rows("test_misses_list.tsv", test, "test"))
