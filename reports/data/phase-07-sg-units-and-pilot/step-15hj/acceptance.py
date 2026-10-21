"""Step 15hj acceptance criteria 1-5. Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15hj/acceptance.py
Writes acceptance.json next to this file. Everything is recomputed from the layer files."""
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common15hj import ALL_INVS, HERE, PRINT, right_first, root, wbc_set
from archival_structures.text_layer.extract_v2 import legacy_region_text
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.join import join_region
from archival_structures.text_layer.join_v2 import join_unit, read_unit_texts


def main():
    res = {k: {} for k in ("crit1", "crit2", "crit3", "crit4", "crit5", "robust_vs_default")}
    for inv in ALL_INVS:
        r = root(inv)
        wbcs = wbc_set(inv)
        wl = sorted(wbcs)
        jt = {(u["scan_id"], u["unit_id"]): u for u in read_tsv_gz(r / "join" / "units" / f"{inv}.tsv.gz")}
        js = defaultdict(list)
        for s in read_tsv_gz(r / "join" / "segments" / f"{inv}.tsv.gz"):
            js[(s["scan_id"], s["unit_id"])].append(s)
        lines = {(x["scan_id"], x["line_id"]): x for x in read_tsv_gz(r / "lines" / f"{inv}.lines.tsv.gz")}
        dmap = {d["old_doc_id"]: d for d in read_tsv_gz(r / "units" / "doc_id_map" / f"{inv}.tsv.gz")}
        legacy = defaultdict(list)
        for x in lines.values():
            legacy[(x["scan_id"], x["region_id"])].append(x["text"])
        c1 = Counter(); c2 = Counter(); c3 = Counter(); c4 = Counter(); c5 = Counter()
        c3_fail = []
        for unit, segs in read_unit_texts(r, inv):
            key = (unit["scan_id"], unit["unit_id"])
            full = [s for s in segs if s["text"]]
            texts = [s["text"] for s in full]
            c1["units"] += 1
            c1["empty_segments"] += len(segs) - len(full)
            text, spans, bounds = join_unit(texts, wbcs)
            # crit 2
            c2["units"] += 1
            c2["diff"] += text != join_region(texts, wbcs)
            c2["diff_vs_layer"] += text != jt[key]["text"]
            # crit 1 (from the written segments table)
            rows = js[key]
            c1["segments"] += len(rows)
            if len(rows) != len(full):
                c1["violations"] += 1
                continue
            prev_end = None; prev_merged = None
            for i, (s, row) in enumerate(zip(full, rows)):
                cs, ce, lead, off = (int(row["char_start"]), int(row["char_end"]),
                                     int(row["line_offset"]) - s["line_start"], int(row["line_offset"]))
                seg = s["text"][lead:]
                piece = jt[key]["text"][cs:ce]
                ok = seg.startswith(piece) and (lead in (0, 1))
                removed = seg[len(piece):]
                merged = row["merged_with_next"] == "1"
                ok = ok and (bool(removed) == merged or (not merged and removed == "")) \
                    and set(removed) <= wbcs
                ok = ok and lines[(row["scan_id"], row["line_id"])]["text"][off:off + len(piece)] == piece
                if prev_end is not None:
                    gap = jt[key]["text"][prev_end:cs]
                    ok = ok and gap == ("" if prev_merged else " ")
                if merged and removed == "":
                    c1["merge_kept_break_char"] += 1
                c1["violations"] += not ok
                if merged:
                    c1["merged"] += 1
                    c1["removed_len_" + str(len(removed))] += 1
                    # digit-digit merge?
                    if ce < len(jt[key]["text"]) and piece[-1:].isdigit() and jt[key]["text"][ce:ce + 1].isdigit():
                        c4["digit_digit_merges"] += 1
                    # lower-case share
                    nxt = full[i + 1]
                    rf = right_first(nxt["text"], int(rows[i + 1]["line_offset"]) - nxt["line_start"], wbcs)
                    cls = "lower" if rf.islower() else "upper" if rf.isupper() else "digit" if rf.isdigit() else "other"
                    c5[cls] += 1
                    c5["char|" + row["break_char_end"] + "|" + cls] += 1
                prev_end = ce; prev_merged = merged
            # crit 3
            if unit["unit_type"] == "main_text" or True:
                d = dmap.get(unit["unit_id"])
                if (inv not in PRINT and d and d["old_status"] == "identical" and d["link"] == "same_id"):
                    leg = legacy.get((unit["scan_id"], unit["unit_id"]), [])
                    mine = join_unit(texts, ["-", "„"], guards=(), both_ends_fix=False)[0] if texts else ""
                    c3["tested"] += 1
                    want = legacy_region_text(leg)
                    if mine != want:
                        c3["fail"] += 1
                        if len(c3_fail) < 5:
                            c3_fail.append([unit["unit_id"], mine[:80], want[:80]])
        res["crit1"][inv] = dict(c1)
        res["crit2"][inv] = dict(c2)
        res["crit3"][inv] = {**dict(c3), "examples": c3_fail}
        st = json.loads((r / "join" / "stats" / f"{inv}.json").read_text())
        res["crit4"][inv] = {"wbc_set": "".join(wl), "kinds": st["boundary_kinds"], **dict(c4)}
        res["crit5"][inv] = dict(c5)
        # sanity-clean variant: the same sets without '.' and ':'
        clean = wbcs - {".", ":"}
        if clean != wbcs:
            cc = Counter()
            for unit, segs in read_unit_texts(r, inv):
                texts = [s["text"] for s in segs if s["text"]]
                text, spans, bounds = join_unit(texts, clean)
                for i, (k, ch) in enumerate(bounds):
                    if k == "merge":
                        nxt = texts[i + 1]
                        lead = spans[i + 1][2]
                        rf = right_first(nxt, lead, clean)
                        cc["merges"] += 1
                        cc["lower"] += rf.islower()
            res["crit5"][inv]["clean_set"] = "".join(sorted(clean))
            res["crit5"][inv]["clean_merges"] = cc["merges"]
            res["crit5"][inv]["clean_lower"] = cc["lower"]
        # robust vs default for each char in the wbc stats
        parts = (r / "wbc" / "parts" / f"{inv}.tsv").read_text(encoding="utf-8").split("\n")
        hdr = parts[0].split("\t")
        rows = [dict(zip(hdr, p.split("\t"))) for p in parts[1:] if p]
        res["robust_vs_default"][inv] = {
            x["char"]: [x["all_freq"], x["all_freq_robust"], x["end_rel_frac"], x["end_rel_frac_robust"],
                        x["n_end_attached"], x["join_vocab_frac"]]
            for x in rows if x["char"] in "„-=.:"}
    # decisions
    dec = {}
    for inv in ALL_INVS:
        r = root(inv)
        d = {}
        for name in ("default", "robust"):
            for row in read_tsv_gz_plain(r / "wbc" / f"wbc_per_inventory_{name}.tsv"):
                if row["inventory_number"] == inv:
                    d[name] = row["wbc_set"]
        dec[inv] = d
    res["decisions"] = dec
    sanity = {}
    for sub in ("hand_tar", "hand_local", "print"):
        from common15hj import LAYER, LAYERS
        rr = LAYER / sub / LAYERS[sub][0]
        for name in ("default", "robust"):
            c = json.loads((rr / "wbc" / f"corpus_default_{name}.json").read_text())
            sanity[f"{sub}|{name}"] = {"default_set": c["default_set"], "pooled_rule_set": c["pooled_rule_set"],
                                       "sanity_pass": c["sanity_pass"], "sanity_in": c["sanity_in"],
                                       "sanity_out": c["sanity_out"]}
    res["sanity"] = sanity
    res["adoption"] = {"decisions_equal_everywhere": all(v["default"] == v["robust"] for v in dec.values())}
    (HERE / "acceptance.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, sort_keys=True))
    # summary
    print("crit1 violations", sum(v.get("violations", 0) for v in res["crit1"].values()),
          "segments", sum(v["segments"] for v in res["crit1"].values()),
          "units", sum(v["units"] for v in res["crit1"].values()),
          "empty segs", sum(v["empty_segments"] for v in res["crit1"].values()),
          "merge_kept_break_char", sum(v.get("merge_kept_break_char", 0) for v in res["crit1"].values()))
    print("crit2 diff", sum(v["diff"] for v in res["crit2"].values()),
          "diff_vs_layer", sum(v["diff_vs_layer"] for v in res["crit2"].values()))
    print("crit3 tested", sum(v.get("tested", 0) for v in res["crit3"].values()),
          "fail", sum(v.get("fail", 0) for v in res["crit3"].values()))
    print("digit-digit merges", sum(v.get("digit_digit_merges", 0) for v in res["crit4"].values()))
    for inv in ALL_INVS:
        c = res["crit5"][inv]; n = sum(c.get(k, 0) for k in ("lower", "upper", "digit", "other"))
        extra = (f" clean[{c['clean_set']}] {c['clean_lower']}/{c['clean_merges']}" if "clean_set" in c else "")
        print(inv, res["crit4"][inv]["wbc_set"], "merges", n, "lower", c.get("lower", 0),
              round(c.get("lower", 0) / n, 4) if n else None, extra)
    print("adoption", res["adoption"], "sanity", {k: v["sanity_pass"] for k, v in sanity.items()})


def read_tsv_gz_plain(path):
    lines = Path(path).read_text(encoding="utf-8").split("\n")
    hdr = lines[0].split("\t")
    return [dict(zip(hdr, l.split("\t"))) for l in lines[1:] if l]


if __name__ == "__main__":
    main()
