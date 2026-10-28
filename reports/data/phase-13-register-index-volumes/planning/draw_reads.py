"""Phase-13 planning (Opus, 2026-10-28): side features of the local hand volumes, the GT fit/test split and the
two read lists, all fixed before any phase-13 rule exists.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/planning/draw_reads.py

Reads (only): data/text_layer_v2_dev/{15y-b,15z-b}/1.01.02/NL-HaNA_1.01.02_text_layer_v4/{mode,lines,layout/lines},
ground_truth/NL-HaNA_1.01.02/inventory_metadata-NL-HaNA_1.01.02.json.
Writes, next to this file:
  side_features.tsv   one row per (inv, scan_id, page) of every local hand volume: n_lines, n_text8, n_text20,
                      col_share (P16 measure per page, '' if < 15 lines of >= 20 chars), end_num, date_start, gt_label
  gt_split.tsv        the 42 local hand volumes in the inventory GT: fit / test (seed 20261113, stratified by
                      whether the GT has an index_page section)
  reads_sides.tsv     60 sides outside the GT for index / not-index reads (seed 20261114): S1 20 from index volumes,
                      S2 20 from scans <= 0100 of half-year registers >= 3349, S3 10 hard (col_share >= 0.25) + 10 any
                      from the other hand volumes
  reads_entries.tsv   30 index-like sides for entry-start gold (seed 20261115): dev 10, test 20, volumes disjoint
                      between dev and test; 1 reserve per (split, group), used in order only for a side read
                      `not index` or `unreadable input`
Feature definitions are the ones step 19c implements (planning brief): col_share = share of lines with >= 20 chars
whose x >= lo + 0.4 * (hi - lo), lo = the 5th-percentile x (index len//20 of the sorted x), hi = max(x + w), over
the page's lines with >= 20 chars, measured iff >= 15 such lines; end_num / date_start = share of the page's lines
with >= 8 stripped chars that match END_NUM / DATE_START.
"""
import json
import random
import re
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import _TYPE_MONTHS

OUT = Path(__file__).resolve().parent
DEV = Path("data/text_layer_v2_dev")
L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
GT = Path("ground_truth/NL-HaNA_1.01.02/inventory_metadata-NL-HaNA_1.01.02.json")
END_NUM = re.compile(r"\d+\s*[.,:;]?\s*$")
DATE_START = re.compile(r"\W{0,2}\d{1,2}\s*[.,:]?\s*" + _TYPE_MONTHS, re.I)

INDEX_VOLS = ["3351", "3354", "3408", "3463", "3478", "3492", "3498", "3520", "3523", "3525", "3546", "3579",
              "3433", "3436", "3439", "3442", "3445", "3466", "3469", "3472", "3475", "3489"]   # tweede serie index
OTHER_INDEX_VOLS = ["3712", "3726", "12297"]      # repertoria G-N, Tafel der respecten, commissieboeken index
LATE_REGISTERS = ["3349", "3350", "3352", "3353", "3368", "3394", "3401", "3421", "3428", "3438", "3444", "3455",
                  "3456", "3459", "3471", "3487", "3493", "3507", "3522", "3539", "3547", "3572", "3587", "3590",
                  "3611", "3623", "3630", "3654"]  # tweede serie half-year registers, not in the GT
GT_OTHER_INDEX = ["3107", "3127", "4617", "4623", "4792"]   # GT volumes that are whole index books


def hand_roots():
    out = {}
    for tag in ("15y-b", "15z-b"):
        for m in sorted((DEV / tag / L4 / "mode").glob("*.json")):
            if json.loads(m.read_text())["mode"] == "hand":
                out[m.stem] = tag      # 15z-b wins for the 13 pinned 15z volumes
    return out


def gt_labels():
    lab = {}
    for x in json.loads(GT.read_text()):
        pages = {}
        for s in x.get("sections") or []:
            for p in range(s["start"], s["end"] + 1):
                pages[p] = s["page_type"]
        lab[str(x["inventory_num"])] = pages
    return lab


def page_number(scan_id, page):
    """REPUBLIC page numbers: verso (left) = 2 * scan - 2, recto (right) = 2 * scan - 1 (checked on 3254 at
    planning; step 19c re-checks it)."""
    s = int(scan_id.rsplit("_", 1)[1])
    return 2 * s - 2 if page == "verso" else 2 * s - 1


def side_rows(inv, tag, lab):
    root = DEV / tag / L4
    page_of = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz")}
    by = defaultdict(list)
    for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
        by[(r["scan_id"], page_of[(r["scan_id"], r["line_id"])])].append(r)
    rows = []
    for (scan, page), ls in sorted(by.items()):
        t8 = [l["text"].strip() for l in ls if len(l["text"].strip()) >= 8]
        s20 = [l for l in ls if len(l["text"]) >= 20]
        cs = ""
        if len(s20) >= 15:
            xs = sorted(int(l["x"]) for l in s20)
            lo, hi = xs[len(xs) // 20], max(int(l["x"]) + int(l["w"]) for l in s20)
            cs = round(sum(int(l["x"]) >= lo + 0.4 * (hi - lo) for l in s20) / len(s20), 4)
        end = round(sum(bool(END_NUM.search(x)) for x in t8) / len(t8), 4) if t8 else 0.0
        date = round(sum(bool(DATE_START.match(x)) for x in t8) / len(t8), 4) if t8 else 0.0
        g = lab.get(inv, {}).get(page_number(scan, page), "") if page in ("verso", "recto") else ""
        rows.append(dict(inv=inv, layer=tag, scan_id=scan, page=page, n_lines=sum(bool(l["text"].strip()) for l in ls),
                         n_text8=len(t8), n_text20=len(s20), col_share=cs, end_num=end, date_start=date, gt_label=g))
    return rows


def write(path, rows, cols):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(str(r[c]) for c in cols) + "\n")


def main():
    roots, lab = hand_roots(), gt_labels()
    feats = [r for inv, tag in sorted(roots.items(), key=lambda kv: int(kv[0])) for r in side_rows(inv, tag, lab)]
    fcols = ["inv", "layer", "scan_id", "page", "n_lines", "n_text8", "n_text20", "col_share", "end_num",
             "date_start", "gt_label"]
    write(OUT / "side_features.tsv", feats, fcols)

    # GT fit/test split
    gt_invs = sorted((i for i in roots if i in lab), key=int)
    with_index = [i for i in gt_invs if "index_page" in lab[i].values()]
    without = [i for i in gt_invs if i not in with_index]
    rng = random.Random(20261113)
    split = {}
    for group in (with_index, without):
        g = group[:]
        rng.shuffle(g)
        for k, inv in enumerate(g):
            split[inv] = "fit" if k % 2 == 0 else "test"
    write(OUT / "gt_split.tsv", [dict(inv=i, split=split[i], has_index_section=int(i in with_index)) for i in gt_invs],
          ["inv", "split", "has_index_section"])

    # side reads (outside the GT)
    rng = random.Random(20261114)
    ok = [r for r in feats if r["n_text8"] >= 5 and r["inv"] not in lab]
    s1 = [r for r in ok if r["inv"] in INDEX_VOLS + OTHER_INDEX_VOLS]
    s2 = [r for r in ok if r["inv"] in LATE_REGISTERS and int(r["scan_id"][-4:]) <= 100]
    rest = [r for r in ok if r["inv"] not in INDEX_VOLS + OTHER_INDEX_VOLS + LATE_REGISTERS]
    s3h = [r for r in rest if r["col_share"] != "" and r["col_share"] >= 0.25]
    picks = [("S1", r) for r in rng.sample(s1, 20)] + [("S2", r) for r in rng.sample(s2, 20)]
    h = rng.sample(s3h, 10)
    picks += [("S3h", r) for r in h] + [("S3a", r) for r in rng.sample([r for r in rest if r not in h], 10)]
    rng.shuffle(picks)   # read order mixes the strata
    write(OUT / "reads_sides.tsv",
          [dict(read_id=k + 1, stratum=s, inv=r["inv"], layer=r["layer"], scan_id=r["scan_id"], page=r["page"],
                n_lines=r["n_lines"]) for k, (s, r) in enumerate(picks)],
          ["read_id", "stratum", "inv", "layer", "scan_id", "page", "n_lines"])

    # entry gold (index-like sides with >= 20 lines)
    def indexlike(r):
        if r["inv"] in lab:
            return r["gt_label"] == "index_page"
        return r["col_share"] != "" and r["col_share"] >= 0.25 and max(r["end_num"], r["date_start"]) >= 0.15 and (
            r["inv"] not in LATE_REGISTERS or int(r["scan_id"][-4:]) <= 100)
    groups = {
        "G1": [i for i in with_index if i not in GT_OTHER_INDEX],        # GT register index sections
        "G2": [i for i in LATE_REGISTERS if i in roots],                   # late register front sections
        "G3": [i for i in INDEX_VOLS if i in roots],                       # tweede serie index volumes
        "G4": [i for i in GT_OTHER_INDEX + OTHER_INDEX_VOLS if i in roots],
    }
    quota = {"dev": {"G1": 3, "G2": 3, "G3": 3, "G4": 1}, "test": {"G1": 5, "G2": 5, "G3": 6, "G4": 4}}
    rng = random.Random(20261115)
    cand = defaultdict(list)
    for r in feats:
        if r["n_lines"] >= 20 and indexlike(r):
            cand[r["inv"]].append(r)
    rows = []
    for gname, invs in groups.items():
        invs = [i for i in invs if cand[i]]
        rng.shuffle(invs)
        n_dev = max(1, len(invs) // 3)
        vol_split = {"dev": invs[:n_dev], "test": invs[n_dev:]}
        for sp in ("dev", "test"):
            need = quota[sp][gname] + 1          # + 1 reserve
            vols = vol_split[sp]
            chosen = []
            k = 0
            while len(chosen) < need and any(cand[v] for v in vols):
                v = vols[k % len(vols)]
                pool = [r for r in cand[v] if r not in chosen]
                if pool:
                    chosen.append(rng.choice(pool))
                k += 1
            for j, r in enumerate(chosen):
                rows.append(dict(split=sp, group=gname, role="reserve" if j == need - 1 else "read", inv=r["inv"],
                                 layer=r["layer"], scan_id=r["scan_id"], page=r["page"], n_lines=r["n_lines"],
                                 split_volumes=" ".join(sorted(vols, key=int))))
    for k, r in enumerate(rows):
        r["read_id"] = k + 1
    write(OUT / "reads_entries.tsv", rows,
          ["read_id", "split", "group", "role", "inv", "layer", "scan_id", "page", "n_lines", "split_volumes"])
    print(f"sides {len(feats)}; gt volumes {len(gt_invs)} (index {len(with_index)}); reads_sides 60; "
          f"reads_entries {len(rows)}")


if __name__ == "__main__":
    main()
