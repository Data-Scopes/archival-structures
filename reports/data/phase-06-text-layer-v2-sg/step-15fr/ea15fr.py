"""Step 15fr error analysis (seed 18): new splits and new unsplit candidates, 1.19 vs 1.22.

Usage: .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15fr/ea15fr.py E_RUN F_RUN OUT
"""
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "step-15e"))
import ea

E, F, OUT = sys.argv[1:4]
txt = []
n = 0
for inv in ea.INVS:
    er, fr = ea.read_rows(E, inv), ea.read_rows(F, inv)
    e_from = {r["split_from"] for r in er if r["split_from"]}
    fp = ea.pairs(fr)
    new = sorted(k for k in fp if k[1] not in e_from)
    idx = ea.pages_index(fr)
    rng = random.Random(18)
    txt.append(f"### {inv}: {len(new)} originals split in 15fr and not in 15e")
    for k in rng.sample(new, min(20, len(new))):
        L, R = fp[k]["L"], fp[k]["R"]
        a, b = ea.neighbours(idx, L, R)
        n += 1
        txt.append(f"[{n}] {inv} {L['scan_id'][-9:]} {L['page_side']} flag={L['edit_flags']} wL={L['w']} wR={R['w']}\n"
                   f"  L    : {L['text_raw']}\n  R    : {R['text_raw']}\n"
                   f"  below L (col1): {b['text_raw'][:70] if b else None}\n  above R (col2): {a['text_raw'][:70] if a else None}")
    def unsplit(rows, run):
        model = json.load(open(Path(run) / "split_model" / f"{inv}.json"))
        out = {}
        for r in rows:
            p = model["params"].get(r["page_side"])
            if p and len(r["text_raw"]) >= p["default_line_num_chars"] and r["w"] >= p["min_merged_width"] \
                    and r["column"] and not r["split_from"]:
                out[r["line_id"]] = r
        return out
    ue, uf = unsplit(er, E), unsplit(fr, F)
    newu = [uf[i] for i in uf if i not in ue]
    txt.append(f"### {inv}: unsplit candidates 15e {len(ue)}, 15fr {len(uf)}, new in 15fr {len(newu)}")
    for r in newu[:20]:
        page = idx[(r["scan_id"], r["page_side"])]
        above = [q for q in page if q["y"] < r["y"] and q is not r]
        below = [q for q in page if q["y"] > r["y"] and q is not r]
        a = max(above, key=lambda q: q["y"]) if above else None
        bb = min(below, key=lambda q: q["y"]) if below else None
        n += 1
        txt.append(f"[{n}] {inv} {r['scan_id'][-9:]} {r['page_side']} x={r['x']} y={r['y']} w={r['w']} col={r['column']}\n"
                   f"  TEXT : {r['text_raw']}\n  above: {a['text_raw'][:60] if a else None}\n  below: {bb['text_raw'][:60] if bb else None}")
Path(OUT).write_text("\n".join(txt) + "\n")
