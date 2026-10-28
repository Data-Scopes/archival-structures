"""Record hand-read line labels for one entry side (labels are typed by the reader).

Usage: append_labels.py READ_ID LABELS [NOTES]
LABELS: one char per listed line (S/C/H/?), spaces ignored, e.g. "SCCSH".
NOTES: optional "k=note;k=note" (e.g. "5=merged;9=OCR garbled").
Appends rows to entry_gold.tsv; refuses if the read_id is already present or the length differs.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from show_side import load_row, side_lines, listing  # noqa: E402

OUT = Path(__file__).parent / "entry_gold.tsv"


def main():
    rid, labels = sys.argv[1], sys.argv[2].replace(" ", "")
    notes = {}
    if len(sys.argv) > 3 and sys.argv[3]:
        for part in sys.argv[3].split(";"):
            k, v = part.split("=", 1)
            notes[int(k)] = v
    row = load_row("entries", rid)
    lines = listing(side_lines(row))
    if len(labels) != len(lines):
        raise SystemExit(f"{len(labels)} labels for {len(lines)} lines")
    if set(labels) - set("SCH?"):
        raise SystemExit("bad label char")
    if OUT.exists() and any(l.split("\t")[0] == rid for l in OUT.read_text().splitlines()[1:]):
        raise SystemExit(f"read_id {rid} already recorded")
    new = not OUT.exists()
    with open(OUT, "a") as f:
        if new:
            f.write("read_id\tscan_id\tpage\tline_id\tk\tlabel\tnote\n")
        for k, (l, lab) in enumerate(zip(lines, labels), 1):
            f.write(f"{rid}\t{row['scan_id']}\t{row['page']}\t{l['line_id']}\t{k}\t{lab}\t{notes.get(k, '')}\n")
    print("ok", rid, len(lines), {c: labels.count(c) for c in "SCH?"})


if __name__ == "__main__":
    main()
