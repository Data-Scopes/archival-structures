"""Step 15h error-analysis samples (seed 18). Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15h/ea.py
Writes ea_sample.txt (read it; the verdicts go in ea_verdicts.tsv) and ea_counts.json."""
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common15h import HERE, LAYERS, root  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402
from archival_structures.text_layer.units import cues_fired, UnitParams  # noqa: E402

rng = random.Random(18)
out = []
counts = {}


def short(t, n=70):
    t = " ".join(t.split())
    return t if len(t) <= n else t[:n - 1] + "~"


def load(inv):
    r = root(inv)
    lines = {(x["scan_id"], x["line_id"]): x for x in read_tsv_gz(r / "lines" / f"{inv}.lines.tsv.gz")}
    segs = list(read_tsv_gz(r / "types" / "segments" / f"{inv}.tsv.gz"))
    usegs = list(read_tsv_gz(r / "units" / "segments" / f"{inv}.tsv.gz"))
    units = {u["unit_id"]: u for u in read_tsv_gz(r / "units" / "units" / f"{inv}.tsv.gz")}
    dmap = list(read_tsv_gz(r / "units" / "doc_id_map" / f"{inv}.tsv.gz"))
    seg_unit = {(s["scan_id"], s["line_id"], s["seg_idx"]): s["unit_id"] for s in usegs}
    return r, lines, segs, units, dmap, seg_unit


def seg_text(lines, s):
    return lines[(s["scan_id"], s["line_id"])]["text"][int(s["char_start"]):int(s["char_end"])]


def region_view(inv, old_id, data, label):
    r, lines, segs, units, dmap, seg_unit = data
    out.append(f"--- {label} {old_id[-48:]}")
    short_id = {}
    rows = [s for s in segs if lines[(s["scan_id"], s["line_id"])]["region_id"] == old_id]
    for s in rows[:16]:
        uid = seg_unit[(s["scan_id"], s["line_id"], s["seg_idx"])]
        u = short_id.setdefault(uid, f"U{len(short_id)}{'*' if uid == old_id else ''}")
        out.append(f"   {u} {s['line_type'][:9]:9} {short(seg_text(lines, s), 72)}")
    if len(rows) > 16:
        out.append(f"   ... {len(rows) - 16} more segments")
    for uid, k in short_id.items():
        un = units[uid]
        out.append(f"   {k}: {un['unit_type']} {un['unit_method']} n_seg={un['n_segments']}")


# 1 hand trimmed
hand_trimmed = []
for sub in ("hand_tar", "hand_local"):
    for inv in LAYERS[sub][1]:
        for row in read_tsv_gz(root(inv) / "units" / "doc_id_map" / f"{inv}.tsv.gz"):
            if row["old_status"] == "trimmed" and row["link"] == "same_id":
                hand_trimmed.append((inv, row["old_doc_id"]))
counts["hand_trimmed_total"] = len(hand_trimmed)
out.append("=== 1 HAND TRIMMED (30)")
cache = {}
for inv, old in rng.sample(hand_trimmed, 30):
    cache.setdefault(inv, load(inv))
    region_view(inv, old, cache[inv], inv)

# 2 print paragraphs on test
pairs = list(read_tsv_gz(HERE / "pairs_test.tsv.gz"))
fp = [p for p in pairs if p["pred"] == "1" and p["ref"] == "0"]
fn = [p for p in pairs if p["pred"] == "0" and p["ref"] == "1"]
counts["test_fp"], counts["test_fn"] = len(fp), len(fn)
pc = {inv: load(inv) for inv in ("455", "456")}


def pair_view(p, kind):
    inv = p["scan_id"].split("_")[2]
    r, lines, segs, units, dmap, seg_unit = pc[inv]
    ordered = [s for s in segs if s["scan_id"] == p["scan_id"]]
    idx = next(i for i, s in enumerate(ordered) if s["line_id"] == p["line_id"] and s["seg_idx"] == p["seg_idx"])
    # previous main segment in same layout region
    cur = ordered[idx]
    prev = None
    for s in reversed(ordered[:idx]):
        if s["layout_region_id"] == cur["layout_region_id"] and s["line_type"] == "main_text":
            prev = s
            break
    out.append(f"--- {kind} {p['scan_id'][-8:]} indent={p['indent']} short={p['short']} gap={p['gap']} hdr={p['header']}")
    out.append(f"   prev: {short(seg_text(lines, prev), 90) if prev else ''}")
    out.append(f"   cur : {short(seg_text(lines, cur), 90)}")


out.append("=== 2a PRINT FALSE POSITIVES (20, test)")
for p in rng.sample(fp, 20):
    pair_view(p, "FP")
out.append("=== 2b PRINT FALSE NEGATIVES (20, test)")
for p in rng.sample(fn, 20):
    pair_view(p, "FN")
per_inv = {}
for inv in ("455", "456"):
    sel = [p for p in pairs if p["scan_id"].split("_")[2] == inv]
    tp = sum(p["pred"] == "1" and p["ref"] == "1" for p in sel)
    f_p = sum(p["pred"] == "1" and p["ref"] == "0" for p in sel)
    f_n = sum(p["pred"] == "0" and p["ref"] == "1" for p in sel)
    pr, rc = tp / (tp + f_p), tp / (tp + f_n)
    per_inv[inv] = {"tp": tp, "fp": f_p, "fn": f_n, "p": round(pr, 3), "r": round(rc, 3), "f1": round(2 * pr * rc / (pr + rc), 3)}
counts["test_per_inventory"] = per_inv

# print one-line short main units (residue)
resid = {}
for inv in ("455", "456"):
    r, lines, segs, units, dmap, seg_unit = pc[inv]
    segtxt = defaultdict(int)
    n = 0
    for u in units.values():
        if u["unit_type"] == "main_text" and u["n_segments"] == "1":
            pass
    usegs = list(read_tsv_gz(r / "units" / "segments" / f"{inv}.tsv.gz"))
    by_unit = defaultdict(list)
    for s in usegs:
        by_unit[s["unit_id"]].append(s)
    seg_by_key = {(s["scan_id"], s["line_id"], s["seg_idx"]): s for s in segs}
    short_main = 0
    one_line_main = 0
    for uid, ss in by_unit.items():
        if units[uid]["unit_type"] != "main_text" or len(ss) != 1:
            continue
        one_line_main += 1
        sg = seg_by_key[(ss[0]["scan_id"], ss[0]["line_id"], ss[0]["seg_idx"])]
        if int(sg["char_end"]) - int(sg["char_start"]) <= 6:
            short_main += 1
    resid[inv] = {"one_segment_main_units": one_line_main, "main_units_le6_chars": short_main}
counts["print_short_main_units"] = resid

# 3 ids: changed legacy regions (all inventories)
changed = []
for inv in [i for _, (_, invs) in LAYERS.items() for i in invs]:
    for row in read_tsv_gz(root(inv) / "units" / "doc_id_map" / f"{inv}.tsv.gz"):
        if row["old_status"] == "changed" and row["old_doc_id"] not in {c[1] for c in changed[-0:]}:
            changed.append((inv, row["old_doc_id"]))
seen = []
for c in changed:
    if c not in seen:
        seen.append(c)
counts["changed_regions_total"] = len(seen)
counts["changed_by_inv"] = dict(Counter(i for i, _ in seen))
out.append("=== 3 CHANGED REGIONS (20)")
for inv, old in rng.sample(seen, 20):
    cache.setdefault(inv, load(inv)) if inv not in pc else None
    data = pc.get(inv) or cache[inv]
    region_view(inv, old, data, inv)
    r = root(inv)
    maprows = [x for x in data[4] if x["old_doc_id"] == old]
    out.append("   map: " + "; ".join(f"{x['link']}->{x['unit_id'][-30:]} ({x['n_segments_shared']} seg)" for x in maprows[:4]))

# 4 crosswalk unmatched
out.append("=== 4 CROSSWALK UNMATCHED")
for inv in ("455", "456"):
    r, lines, segs, units, dmap, seg_unit = pc[inv]
    un = [x for x in read_tsv_gz(r / "units" / "republic_lines" / f"{inv}.tsv.gz") if x["match"] == "unmatched"]
    counts[f"unmatched_{inv}"] = len(un)
    by_scan = defaultdict(list)
    for l in lines.values():
        by_scan[l["scan_id"]].append(l)
    for x in un[:23]:
        scan = x["scan_id"]
        box = [int(v) for v in x["republic_line_id"].split("-line-")[1].split("-")]
        near = sorted(by_scan.get(scan, []), key=lambda l: abs(int(l["x"]) - box[0]) + abs(int(l["y"]) - box[1]))[:1]
        nl = near[0] if near else None
        out.append(f"--- {x['republic_line_id'][-40:]} box={box}; nearest ours: "
                   + (f"({nl['x']},{nl['y']},{nl['w']},{nl['h']}) blank={not nl['text'].strip()} '{short(nl['text'], 40)}' split={bool(nl['split_from'])}" if nl else "none"))
(HERE / "ea_sample.txt").write_text("\n".join(out))
(HERE / "ea_counts.json").write_text(json.dumps(counts, indent=1))
print(json.dumps(counts, indent=1))
