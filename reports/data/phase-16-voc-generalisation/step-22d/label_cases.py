"""Hand labels for the rows of errors_dump.txt (read by the step-22d session); writes cases_read.tsv."""
import csv
from pathlib import Path
HERE = Path(__file__).resolve().parent
NH, NF, RE, HT, MP, SD, UN, CO, VM = ("neighbour", "not_a_folio", "range_end_taken", "htr", "map_error", "subdoc_no_gold",
                                      "unclear", "coincidence_no_folio", "verso_or_connector_miss")
dev = [NH, RE, RE, NH, NF, NF, NF, NF, MP, NF, HT, HT, NH, NH, MP, NH, NH, MP, MP, MP, MP, UN, UN, SD, SD, NH, NH, NF, NF, NH]
dev_notes = {10: "year after 'Nov.' read as folio", 29: "the line-start number 2062 (after a quote mark) is missed; the year is taken",
             12: "'Aao 1741' misread as 174:1"}
test = [MP, MP, UN, RE, RE, UN, NF, MP, UN, NF, RE, NF, MP, NF, NF, NF, MP, MP, NF, NF, NF, NF, MP, MP, NF, NF, RE, MP, NF, MP]
lines = (HERE / "errors_dump.txt").read_text().splitlines()
sec = None
rows, i_dev, i_test, i_cov = [], 0, 0, 0
cov = [HT, VM, VM, VM, HT, VM, VM, CO, CO, CO, CO, CO, CO, CO, CO]
note_cov = {1: "number glued to a word (1Wingurla)", 2: "range end followed by 'verso': 'a 149 verso' not matched",
            3: "'180 verso a 199 verso'", 4: "'259 verso a 262 verso'", 6: "'656 a 668 verso'", 7: "'749 a 749 verso'"}
note_cov[5] = "noise line 'a3'"
for ln in lines:
    if ln.startswith("## dev non-hits"): sec = "dev"; continue
    if ln.startswith("## test non-hits"): sec = "test"; continue
    if ln.startswith("## dev lines"): sec = "cov"; continue
    f = ln.split("\t")
    if sec == "dev":
        rows.append([f[0], f[1], "dev", "C3", "nonhit", f[2], dev[i_dev], dev_notes.get(i_dev + 1, f[5]), f[-1][2:80]]); i_dev += 1
    elif sec == "test":
        rows.append([f[0], f[1], "test", "C3", "nonhit", f[2], test[i_test], f[5], f[-1][2:80]]); i_test += 1
    else:
        rows.append([f[0], f[1], "dev", "C3", "no_locator", "", cov[i_cov], note_cov.get(i_cov + 1, "digits in the line are dates, not folios"), f[-1][2:80]]); i_cov += 1
assert (i_dev, i_test, i_cov) == (30, 30, 15), (i_dev, i_test, i_cov)
with open(HERE / "cases_read.tsv", "w", newline="") as fh:
    w = csv.writer(fh, delimiter="\t", lineterminator="\n")
    w.writerow(["inv", "scan", "split", "cell", "kind", "locator", "category", "note", "line"])
    w.writerows(rows)
(HERE / "cases_read.tsv").chmod(0o664)
from collections import Counter
for k in ("nonhit", "no_locator"):
    for s in ("dev", "test"):
        c = Counter(r[6] for r in rows if r[4] == k and r[2] == s)
        if c: print(k, s, dict(c))
