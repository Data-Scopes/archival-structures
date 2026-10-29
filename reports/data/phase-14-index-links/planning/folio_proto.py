"""Phase-14 planning prototype: folio numbers of the resolution sides of the 41 front-index volumes.

Run from the AS root: .venv/bin/python -I reports/data/phase-14-index-links/planning/folio_proto.py

Per volume (`front_index_volumes.tsv`), sides in `layout/pages` order after the front index (`front_last`):
anchors = `types/segments` rows with `line_type == "page_number"` on those sides whose segment text
(`lines.text[char_start:char_end]`) holds one 1-4 digit number; the longest strictly increasing chain
(`analysis.page_numbering.longest_increasing_chain`) over them in side order. Reports per volume: sides after
the index, rectos / versos among them, anchors, chain length, chain anchors on recto / verso, the median of
`scan_pos - number` over the chain (scan_pos = 1-based position of the scan in the volume), and the leave-one-out
check: each chain anchor's number predicted by linear interpolation of its chain neighbours (scan positions),
rounded, exact share. Writes `folio_proto.tsv`; prints totals.
"""

import re
import statistics
import sys
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.analysis.page_numbering import longest_increasing_chain  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

LAYER = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
HERE = Path(__file__).parent
NUM = re.compile(r"^\D{0,3}(\d{1,4})\D{0,3}$")


def main():
    rows = [l.rstrip("\n").split("\t") for l in open(HERE / "front_index_volumes.tsv")][1:]
    out, tot = [], dict(after=0, rect=0, anch=0, chain=0, loo_n=0, loo_ok=0)
    for r in rows:
        inv, front_last = r[0], int(r[4])
        pages = list(read_tsv_gz(LAYER / "layout" / "pages" / f"{inv}.tsv.gz"))
        scan_pos, pos = {}, 0
        for p in pages:
            if p["scan_id"] not in scan_pos:
                pos += 1
                scan_pos[p["scan_id"]] = pos
        after = pages[front_last + 1:]
        side_idx = {(p["scan_id"], p["page"]): i for i, p in enumerate(after)}
        text = {(l["scan_id"], l["line_id"]): l["text"] for l in read_tsv_gz(LAYER / "lines" / f"{inv}.lines.tsv.gz")}
        anchors = []
        for s in read_tsv_gz(LAYER / "types" / "segments" / f"{inv}.tsv.gz"):
            if s["line_type"] != "page_number":
                continue
            k = (s["scan_id"], s["page"])
            if k not in side_idx:
                continue
            t = text.get((s["scan_id"], s["line_id"]), "")[int(s["char_start"]):int(s["char_end"])]
            m = NUM.match(t.strip())
            if m:
                anchors.append((side_idx[k], int(m.group(1)), s["page"], scan_pos[s["scan_id"]]))
        anchors.sort()
        # one anchor per side: the first
        seen, a1 = set(), []
        for a in anchors:
            if a[0] not in seen:
                seen.add(a[0])
                a1.append(a)
        chain = [a1[i] for i in longest_increasing_chain([a[1] for a in a1])] if a1 else []
        n_rect = sum(1 for p in after if p["page"] == "recto")
        c_rect = sum(1 for a in chain if a[2] == "recto")
        offs = [a[3] - a[1] for a in chain]
        loo_n = loo_ok = 0
        for i in range(1, len(chain) - 1):
            (_, n0, _, s0), (_, n1, _, s1), (_, n2, _, s2) = chain[i - 1], chain[i], chain[i + 1]
            if s2 == s0:
                continue
            pred = round(n0 + (n2 - n0) * (s1 - s0) / (s2 - s0))
            loo_n += 1
            loo_ok += pred == n1
        out.append((inv, len(after), n_rect, len(a1), len(chain), c_rect, len(chain) - c_rect,
                    statistics.median(offs) if offs else "", loo_n, round(loo_ok / loo_n, 4) if loo_n else ""))
        tot["after"] += len(after); tot["rect"] += n_rect; tot["anch"] += len(a1); tot["chain"] += len(chain)
        tot["loo_n"] += loo_n; tot["loo_ok"] += loo_ok
    with open(HERE / "folio_proto.tsv", "w") as fh:
        fh.write("inv\tn_sides_after\tn_recto_after\tn_anchors\tn_chain\tchain_recto\tchain_verso\tmedian_offset\tloo_n\tloo_exact\n")
        for o in out:
            fh.write("\t".join(map(str, o)) + "\n")
    print(tot, "chain/recto", round(tot["chain"] / tot["rect"], 4), "loo exact", round(tot["loo_ok"] / tot["loo_n"], 4))


if __name__ == "__main__":
    main()
