"""Page-number segments by sub-layer, band and shape, with seeded samples (phase 09; step 15k).

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/pn_offchain.py \
        --layer data/text_layer_v2_dev/15ir-a [--sample 20 --seed 41 --out sample.tsv] [--json F]

Counts every segment whose `type_method` starts with `rule:pn_line` or is `rule:year_offchain`, by
(sub-layer, type_method, line_type, band, shape). Band: `top` when the segment's y is above half the
page height, else `bottom` (the pn rule fires only in the top_n / bottom_n band lines). Shape:
`year` when the text fullmatches `\\W*1[67]\\d\\d\\W*`, else `num`.
The sample: per stratum (hand-top, hand-bottom, print-top, print-bottom) of the **off-chain,
non-gap, non-year** segments, `--sample` rows drawn with `random.Random(seed + k)` (k = 0..3 in that
order); then every non-gap year-shaped one (stratum `year`); then `--sample` gap rows
(`random.Random(seed + 4)`, stratum `gap`). Context columns: the confirmed (`rule:pn_line`) numbers of this page and
of the previous and next page in the inventory, and the other band lines of the page.
"""
import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import inventories, layer_root, read  # noqa: E402

YEAR = re.compile(r"\W*1[67]\d\d\W*")
STRATA = [("hand", "top"), ("hand", "bottom"), ("print", "top"), ("print", "bottom")]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True)
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--seed", type=int, default=41)
    ap.add_argument("--out", default="")
    ap.add_argument("--json", default="")
    a = ap.parse_args(argv)
    counts = Counter()
    pool = defaultdict(list)
    years, gap_pool = [], []
    for sub in ("hand", "print"):
        root = layer_root(a.layer, sub)
        for inv in inventories(sub):
            S = read(root / "types" / "segments" / f"{inv}.tsv.gz",
                     ["scan_id", "line_id", "char_start", "char_end", "y", "page", "line_type", "type_method"])
            L = read(root / "lines" / f"{inv}.lines.tsv.gz", ["scan_id", "line_id", "text"])
            text = dict(zip(zip(L.scan_id, L.line_id), L.text))
            Sc = read(root / "scans" / f"{inv}.scans.tsv.gz", ["scan_id", "page_h"])
            page_h = dict(zip(Sc.scan_id, Sc.page_h.astype(int)))
            pages = list(dict.fromkeys(zip(S.scan_id, S.page)))
            confirmed = defaultdict(list)
            conf_num = {}
            band_lines = defaultdict(list)
            rows = []
            for r in S.itertuples():
                t = text[(r.scan_id, r.line_id)][int(r.char_start):int(r.char_end)].strip()
                m = r.type_method
                if m == "rule:pn_line":
                    confirmed[(r.scan_id, r.page)].append(t)
                if m in ("rule:pn_line", "rule:pn_suffix"):
                    nums = re.findall(r"\d{1,4}", t)
                    if nums:
                        conf_num[(r.scan_id, r.page)] = int(nums[0] if m == "rule:pn_line" else nums[-1])
                if m.startswith("rule:pn_line") or m == "rule:year_offchain":
                    band = "top" if int(r.y) < page_h[r.scan_id] / 2 else "bottom"
                    shape = "year" if YEAR.fullmatch(t) else "num"
                    counts[(sub, m, r.line_type, band, shape)] += 1
                    band_lines[(r.scan_id, r.page)].append(t)
                    rows.append((r, t, band, shape))
            idx = {p: i for i, p in enumerate(pages)}
            prev_num, next_num, last = [None] * len(pages), [None] * len(pages), None
            for i, pg in enumerate(pages):
                prev_num[i] = last
                last = conf_num.get(pg, last)
            last = None
            for i in range(len(pages) - 1, -1, -1):
                next_num[i] = last
                last = conf_num.get(pages[i], last)
            for r, t, band, shape in rows:
                if r.type_method == "rule:pn_line":
                    continue
                i = idx[(r.scan_id, r.page)]
                n = int(re.search(r"\d{1,4}", t).group())
                gap = int(prev_num[i] is not None and next_num[i] is not None and prev_num[i] < n < next_num[i])
                counts[(sub, "gap" if gap else "nogap", band, shape)] += 1
                ctx = (inv, r.scan_id, r.page, band, r.type_method, r.line_type, t,
                       " ".join(confirmed.get((r.scan_id, r.page), [])),
                       " ".join(confirmed.get(pages[i - 1], [])) if i > 0 else "",
                       " ".join(confirmed.get(pages[i + 1], [])) if i + 1 < len(pages) else "",
                       " | ".join(x for x in band_lines[(r.scan_id, r.page)] if x != t)[:80], gap)
                (gap_pool if gap else years if shape == "year" else pool[(sub, band)]).append((sub,) + ctx)
    for k, v in sorted(counts.items()):
        print(k, v)
    if a.json:
        Path(a.json).write_text(json.dumps({"|".join(k): v for k, v in sorted(counts.items())}, indent=1))
    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write("stratum\tsub\tinv\tscan_id\tpage\tband\ttype_method\tline_type\ttext\tpage_confirmed"
                     "\tprev_confirmed\tnext_confirmed\tother_band_numbers\tgap\tverdict\n")
            for k, (sub, band) in enumerate(STRATA):
                items = list(pool[(sub, band)])
                random.Random(a.seed + k).shuffle(items)
                for row in items[:a.sample]:
                    fh.write(f"{sub}-{band}\t" + "\t".join(map(str, row)) + "\t\n")
            for row in years:
                fh.write("year\t" + "\t".join(map(str, row)) + "\t\n")
            random.Random(a.seed + 4).shuffle(gap_pool)
            for row in gap_pool[:a.sample]:
                fh.write("gap\t" + "\t".join(map(str, row)) + "\t\n")


if __name__ == "__main__":
    main()
