"""Step 15x: write verdicts.tsv (bar A sample + diagnostic) and pages.tsv (bar C) from the hand readings.
The verdicts were set by reading reads_AC2.txt / diag_nouv_plak.txt (full text of every differing unit); this
script only records them. Every sampled cut is `right` except the ones listed in WRONG_A."""
import collections, csv, sys
sys.path.insert(0, "reports/data/phase-12-hand-print-box-height/step-15x")
from bars import Vol
S = "reports/data/phase-12-hand-print-box-height/step-15x"
WRONG_A = {"b982b4114bd1": "'En hier op gedelibereert zynde' opens a decision clause after a 2-line page-top tail"}
rows = list(csv.DictReader(open(f"{S}/sampleA.tsv"), delimiter="\t"))
with open(f"{S}/verdicts.tsv", "w") as f:
    f.write("bar\tinv\tscan_id\tline_id\tverdict\tnote\n")
    for r in rows:
        tag = r["line_id"][-12:]
        v = "wrong" if tag in WRONG_A else "right"
        f.write(f"A\t{r['inv']}\t{r['scan_id']}\t{r['line_id']}\t{v}\t{WRONG_A.get(tag, 'continues the sentence beside the initial')}\n")
    diag = [("nouvelles", "12087", "12087_0220", "933c2896ea20", "wrong", "unreadable HTR (unclear counts as wrong)"),
            ("nouvelles", "12088", "12088_0433", "aac3db279619", "right", "mid-sentence"),
            ("nouvelles", "12088", "12088_0569", "7eba035128ba", "right", "mid-sentence"),
            ("nouvelles", "12100", "12100_0167", "c3eded0f4dcc", "right", "mid-sentence"),
            ("nouvelles", "12103", "12103_0178", "f294ce428509", "right", "mid-sentence"),
            ("nouvelles", "12103", "12103_0260", "2fb9e47afd7c", "wrong", "'Dat. 29 Juny' dateline after a signature"),
            ("nouvelles", "12105", "12105_0109", "03d2557d8a0e", "right", "mid-sentence"),
            ("nouvelles", "12106", "12106_0312", "b92ac7d2a220", "wrong", "'Ik ben met veel hoogagtinge' closing formula"),
            ("nouvelles", "12133", "12133_0135", "7745de4e5cd2", "right", "mid-sentence"),
            ("nouvelles", "12144", "12144_0429", "42a08ab3554f", "right", "mid-sentence"),
            ("plakkaten", "12162", "12162_0124", "74f161266774", "right", "price list item continues"),
            ("plakkaten", "12166", "12166_0197", "798ba71bd1bd", "wrong", "garbled fragment 'orlaet' (unclear)"),
            ("plakkaten", "12166", "12166_0294", "5b9a37850bd9", "right", "end of clause"),
            ("plakkaten", "12169", "12169_0521", "fd2f9925d4d8", "right", "mid-sentence"),
            ("plakkaten", "12176", "12176_0059", "c040bc601c86", "right", "mid-sentence"),
            ("plakkaten", "12176", "12176_0478", "87f526fcb324", "wrong", "garbage line (unclear)"),
            ("plakkaten", "12205", "12205_0026", "32f4425e99db", "right", "mid-sentence"),
            ("plakkaten", "12226", "12226_0072", "b7035c6002b9", "right", "mid-sentence"),
            ("plakkaten", "12232", "12232_0571", "9ed8bba05b88", "wrong", "fragment 'susvens' (unclear)"),
            ("plakkaten", "12235", "12235_0067", "32e0e426d904", "wrong", "German table junk (unclear)")]
    for series, inv, scan, tag, v, note in diag:
        f.write(f"diag-{series}\t{inv}\tNL-HaNA_1.01.02_{scan}\t...{tag}\t{v}\t{note}\n")
PAGE = {  # scan -> (verdict, note)
 "3769_0416": ("better", ""), "3769_0600": ("better", "page number '12 7' glued onto the last unit"),
 "3772_0344": ("better", ""), "3772_0369": ("better", ""), "3773_0200": ("same", "no suppressed cut on the page"),
 "3773_0353": ("better", ""), "3774_0281": ("better", "French memorial: 'En prenant connoissance' possibly a paragraph start, now merged"),
 "3774_0514": ("better", ""), "3795_0179": ("better", ""),
 "3795_0360": ("better", "signature 'Geteeckent, / C. Voet van Winsen.' merged into one unit"),
 "3816_0355": ("better", "'WAAR op gedelibereert' clause glued to the junk line '4080'"),
 "3816_0460": ("better", ""), "3823_0032": ("better", "marginal summary page; two hyphen continuations merged"),
 "3823_0196": ("better", ""), "3828_0115": ("better", ""), "3828_0195": ("better", ""),
 "3844_0260": ("better", ""), "3844_0323": ("better", ""), "3849_0108": ("better", ""),
 "3849_0257": ("better", "'En hier op gedelibereert zynde' decision merged into the page-top tail; a 3-line chunk stays cut")}
with open(f"{S}/pages.tsv", "w") as f:
    f.write("inv\tscan_id\tunits_15wr-a\tunits_15x-a\tverdict\tnote\n")
    for r in csv.DictReader(open("reports/data/phase-12-hand-print-box-height/review/reads_15x.tsv"), delimiter="\t"):
        inv, scan = r["inv"], r["scan_id"]
        a, b = Vol("15wr-a", inv), Vol("15x-a", inv)
        v, note = PAGE[scan.split("1.01.02_")[1]]
        f.write(f"{inv}\t{scan}\t{len(a.page[scan])}\t{len(b.page[scan])}\t{v}\t{note}\n")
