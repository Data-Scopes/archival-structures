"""Step 15e criterion 5: re-run 456 with the notebook's parameters (both page sides, verbatim).

Usage: .venv/bin/python reports/data/phase-06-text-layer-v2-sg/step-15e/run_notebook_params.py OUT_ROOT

OUT_ROOT/<preset name>/ gets the same tables as the CLI run. The scorer is fitted with the
notebook parameters too (the candidate rule decides which lines are training text). The 15d
lines table is used for the fit, so only one parse of the PageXML is needed.
"""
import sys
from pathlib import Path

from archival_structures.text_layer.line_split import SplitParams, extract_inventory_v2_split
from archival_structures.text_layer.presets import SG_PRINT_V2

SRC = ("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml/"
       "NL-HaNA_1.10.94_456.zip")
LINES = ("data/text_layer_v2_dev/15d-a/print/NL-HaNA_1.10.94_text_layer_v2/lines/"
         "456.lines.tsv.gz")


def main():
    out = Path(sys.argv[1]) / SG_PRINT_V2.name
    notebook = SplitParams(min_merged_width=1100, default_line_width=900,
                           default_line_num_chars=45, text_left_boundary=450,
                           text_right_boundary=2350, column_sep_width=20,
                           split_region_tags=SG_PRINT_V2.split_region_tags)
    res = extract_inventory_v2_split("456", SRC, out, SG_PRINT_V2, lines_path=LINES,
                                     params={"left": notebook, "right": notebook})
    print(res)


if __name__ == "__main__":
    main()
