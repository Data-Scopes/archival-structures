"""Step 15e criterion 6: merge-candidate rates over 15d's lines of all 16 inventories.

Usage: .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15e/rates.py \
           data/text_layer_v2_dev/15d-a OUT.tsv

For each inventory: the derived parameters per page side (from lines with >= 20 characters of
the tags "index", "resolution", "untagged", "Text"; the same rule for hand and print, so the
rates are comparable), the candidate rate among those participating lines and among all lines
with >= 20 characters, and the candidate counts per region tag (side parameters of the
participating lines applied to every line).
"""
import json
import sys
from collections import Counter
from pathlib import Path

from archival_structures.text_layer.line_split import (
    derive_split_params, is_candidate_row, read_line_rows)

TAGS = ("index", "resolution", "untagged", "Text")
GROUPS = ("hand_tar", "hand_local", "print")


def main():
    root = Path(sys.argv[1])
    out = Path(sys.argv[2])
    lines_out = []
    header = ["group", "inv", "lines", "lines_ge20", "participating_ge20", "left_w", "left_chars",
              "left_minmerged", "right_w", "right_chars", "right_minmerged", "cand_part",
              "rate_part_pct", "left_span", "left_col_share_pct", "left_w_over_span", "cand_all", "rate_all_pct", "cand_by_tag", "tags_ge20"]
    lines_out.append("\t".join(header))
    for group in GROUPS:
        layer = next((root / group).iterdir())
        for path in sorted((layer / "lines").glob("*.lines.tsv.gz")):
            inv = path.name.split(".")[0]
            rows, tag_by_region = read_line_rows(path)
            ge20 = [r for r in rows if len(r["text_raw"]) >= 20]
            part = [r for r in ge20 if tag_by_region.get(r["region_id"]) in TAGS]
            params = {s: derive_split_params(part, s, TAGS) for s in ("left", "right")}
            if all(p is None for p in params.values()):
                params = {s: derive_split_params(ge20, s, TAGS) for s in ("left", "right")}
            cand_part = cand_all = 0
            by_tag, tags = Counter(), Counter()
            for r in ge20:
                tag = tag_by_region.get(r["region_id"], "?")
                tags[tag] += 1
                p = params.get(r["page_side"])
                if p is not None and is_candidate_row(r, p):
                    cand_all += 1
                    by_tag[tag] += 1
                    if tag in TAGS:
                        cand_part += 1

            def colinfo(side):
                p = params[side]
                if p is None:
                    return "", "", ""
                rs = [r for r in part if r["page_side"] == side]
                span = p.text_right_boundary - p.text_left_boundary
                col = [r for r in rs if 0.35 * span <= r["w"] <= 0.6 * span]
                return span, round(100 * len(col) / len(rs), 1), round(p.default_line_width / span, 3)

            def f(p, name):
                return getattr(p, name) if p else ""
            lines_out.append("\t".join(str(v) for v in [
                group, inv, len(rows), len(ge20), len(part),
                f(params["left"], "default_line_width"), f(params["left"], "default_line_num_chars"),
                f(params["left"], "min_merged_width"), f(params["right"], "default_line_width"),
                f(params["right"], "default_line_num_chars"), f(params["right"], "min_merged_width"),
                cand_part, round(100 * cand_part / len(part), 2) if part else "",
                *colinfo("left"),
                cand_all, round(100 * cand_all / len(ge20), 2) if ge20 else "",
                json.dumps(dict(by_tag), sort_keys=True), json.dumps(dict(tags), sort_keys=True)]))
            print(lines_out[-1].split("\t")[:15], flush=True)
    out.write_text("\n".join(lines_out) + "\n")
    out.chmod(0o664)


if __name__ == "__main__":
    main()
