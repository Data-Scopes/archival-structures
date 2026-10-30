"""Step 21d silver check (reported, no bar): per located link, do the entry's words (>= 6 letters) sit on the target
sides, against the folio N+3 control? Phase 14's definition (reports/data/phase-14-index-links/step-20e/silver.py,
words part), run on v1 (21d-v1) and v2 (21d-a) with each version's own folio map. Also: for v2, the opening side
(verso of the target scan) against the target sides (front + back).
Run from the AS root: .venv/bin/python -I reports/data/phase-15-index-links-v2/step-21d/silver.py
Reads the dev v5 layer (lines, layout/lines, join/units), 21d-v1 and 21d-a. Writes silver.tsv next to this script."""
import re
from collections import defaultdict
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
V1 = Path("data/index_links_dev/21d-v1/NL-HaNA_1.01.02_index_links_v1")
V2 = Path("data/index_links_dev/21d-a/NL-HaNA_1.01.02_index_links_v2")
OUT = Path(__file__).parent
HIGH = "3438 3444 3455 3459 3493 3507 3522 3539 3547 3572 3587 3623 3630".split()
WORD = re.compile(r"[^\W\d_]{6,}")
KEYS = "links links_ctrl words tgt ctrl link_hit link_hit_ctrl open_words open_only_words links_open_only links_with_words".split()


def sides_of(fmap, n, opening=False):
    r = fmap.get(n)
    if r is None:
        return None
    s = [(r["front_scan_id"], r["front_page"])]
    if r["back_scan_id"]:
        s.append((r["back_scan_id"], r["back_page"]))
    return s


def one(base, version, inv, side_words, texts_v1):
    fmap = {int(r["number"]): r for r in read_tsv_gz(base / "folio_map" / f"{inv}.tsv.gz")}
    if version == "v2":
        etext = {(r["scan_id"], r["entry_id"]): r["text"] for r in read_tsv_gz(base / "entries" / f"{inv}.tsv.gz")}
    c = defaultdict(int)
    keyset = set()
    for r in read_tsv_gz(base / "links" / f"{inv}.tsv.gz"):
        if r["method"] == "none":
            continue
        n = int(r["number"])
        ts, cs = sides_of(fmap, n), sides_of(fmap, n + 3)
        key = (r["scan_id"], r["unit_id"] if version == "v1" else r["entry_id"], r["loc_idx"], n)
        text = texts_v1[(r["scan_id"], r["unit_id"])] if version == "v1" else etext[(r["scan_id"], r["entry_id"])]
        words = set(WORD.findall(text.lower()))
        if ts is None or not words:
            continue
        tw = set().union(*(side_words.get(s, set()) for s in ts))
        c["links"] += 1
        if cs is None:
            continue
        cw = set().union(*(side_words.get(s, set()) for s in cs))
        c["links_ctrl"] += 1
        c["words"] += len(words)
        c["tgt"] += len(words & tw)
        c["ctrl"] += len(words & cw)
        c["link_hit"] += bool(words & tw)
        c["link_hit_ctrl"] += bool(words & cw)
        if version == "v2" and r["opening_scan_id"]:
            ow = side_words.get((r["opening_scan_id"], r["opening_page"]), set())
            c["open_words"] += len(words & ow)
            c["open_only_words"] += len((words & ow) - tw)
            c["links_open_only"] += bool(words & ow) and not (words & tw)
        keyset.add(key)
    return c, keyset


tot = {(v, s): defaultdict(int) for v in ("v1", "v2") for s in ("all", "high")}
common = {(s): defaultdict(int) for s in ("all", "high")}
rows = []
vols = [l.split("\t")[0] for l in (V2 / "volumes.tsv").read_text().splitlines()[1:]]
for inv in vols:
    page_of = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(L / "layout" / "lines" / f"{inv}.tsv.gz")}
    side_words = defaultdict(set)
    for r in read_tsv_gz(L / "lines" / f"{inv}.lines.tsv.gz"):
        pg = page_of.get((r["scan_id"], r["line_id"]))
        if pg:
            side_words[(r["scan_id"], pg)].update(WORD.findall(r["text"].lower()))
    texts = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(L / "join" / "units" / f"{inv}.tsv.gz")}
    res = {}
    for v, base in (("v1", V1), ("v2", V2)):
        res[v] = one(base, v, inv, side_words, texts)
        strata = ["all"] + (["high"] if inv in HIGH else [])
        for s in strata:
            for k, x in res[v][0].items():
                tot[(v, s)][k] += x
        sh = lambda c, a, b: f"{c[a] / c[b]:.4f}" if c[b] else ""
        c = res[v][0]
        rows.append([inv, v, "high" if inv in HIGH else "", c["links"], c["links_ctrl"], c["words"], c["tgt"], c["ctrl"],
                     sh(c, "tgt", "words"), sh(c, "ctrl", "words"), c["link_hit"], c["link_hit_ctrl"],
                     c["links_open_only"], c["open_only_words"]])
hdr = "inv version stratum links links_ctrl words tgt_hits ctrl_hits tgt_share ctrl_share links_with_tgt_hit links_with_ctrl_hit links_open_only open_only_words".split()
lines = ["\t".join(hdr)] + ["\t".join(map(str, r)) for r in rows]
out = []
for s in ("all", "high"):
    for v in ("v1", "v2"):
        c = tot[(v, s)]
        row = ["TOTAL_" + s, v, s, c["links"], c["links_ctrl"], c["words"], c["tgt"], c["ctrl"], f"{c['tgt'] / c['words']:.4f}",
               f"{c['ctrl'] / c['words']:.4f}", c["link_hit"], c["link_hit_ctrl"], c["links_open_only"], c["open_only_words"]]
        lines.append("\t".join(map(str, row)))
        out.append(f"{s:4s} {v}: links {c['links']} (with control {c['links_ctrl']}); words target {c['tgt'] / c['words']:.4f} "
                   f"({c['tgt']}/{c['words']}) control {c['ctrl'] / c['words']:.4f}; links with >=1 target hit "
                   f"{c['link_hit'] / c['links_ctrl']:.4f}, control {c['link_hit_ctrl'] / c['links_ctrl']:.4f}"
                   + (f"; opening-only links {c['links_open_only']} = {c['links_open_only'] / c['links_ctrl']:.4f} of links, "
                      f"opening-only words {c['open_only_words']} = {c['open_only_words'] / c['words']:.4f} of words" if v == "v2" else ""))
(OUT / "silver.tsv").write_text("\n".join(lines) + "\n")
print("\n".join(out))
