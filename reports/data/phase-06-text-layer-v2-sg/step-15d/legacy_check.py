"""Step 15d acceptance 1-3: legacy regression, uniqueness, coverage.

Usage: .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15d/legacy_check.py <run dir>
<run dir> holds hand_tar/ hand_local/ print/ (each with <preset name>/ inside). Read only.
"""
import csv
import gzip
import sys
import tarfile
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.extract_v2 import legacy_region_text
from archival_structures.text_layer.io import read_tsv_gz

SHARED = Path("/Users/Shared/claude_projects/data_scopes/shared-data")
LEGACY = SHARED / "NL-HaNA_1.01.02/paragraphs/source/paragraphs-NL-HaNA_1.01.02_{inv}.tsv.gz"
STATS = Path("data/scan_stats-1.01.02/scan_stats-inv_{inv}.tsv.gz")
RUNS = [("hand_tar", "NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
        ("hand_local", "NL-HaNA_1.01.02_text_layer_v2",
         ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
        ("print", "NL-HaNA_1.10.94_text_layer_v2", ["455", "456"])]
csv.field_size_limit(10 ** 9)


def read_legacy(inv):
    with gzip.open(str(LEGACY).format(inv=inv), "rt", encoding="utf-8", newline="\n") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE))
    return rows


def main(run_dir):
    run_dir = Path(run_dir)
    all_ok = True
    print("inv\ttsv_rows\tdoc_set_equal\tmatched\ttext_equal\tdup_regions\tdup_lines\tscans\tstatuses")
    for sub, layer, invs in RUNS:
        root = run_dir / sub / layer
        for inv in invs:
            regions = list(read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz"))
            lines = list(read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"))
            scans = list(read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz"))
            by_region = defaultdict(list)
            for ln in lines:
                by_region[ln["region_id"]].append(ln["text"])
            dup_r = sum(c - 1 for c in Counter(r["region_id"] for r in regions).values() if c > 1)
            dup_l = sum(c - 1 for c in Counter((l["scan_id"], l["line_id"]) for l in lines).values() if c > 1)
            statuses = dict(sorted(Counter(s["status"] for s in scans).items()))
            if sub == "print":
                print(f"{inv}\t-\t-\t-\t-\t{dup_r}\t{dup_l}\t{len(scans)}\t{statuses}")
                continue
            ours = {rid: legacy_region_text(t) for rid, t in by_region.items() if any(t)}
            tsv = read_legacy(inv)
            tsv_ids = Counter(r["doc_id"] for r in tsv)
            set_equal = set(tsv_ids) == set(ours)
            matched = sum(1 for r in tsv if r["doc_id"] in ours)
            text_equal = sum(1 for r in tsv if r["doc_id"] in ours and ours[r["doc_id"]] == r["text"])
            all_ok &= set_equal and text_equal == len(tsv) and matched == len(tsv)
            print(f"{inv}\t{len(tsv)}\t{set_equal}\t{matched}\t{text_equal}\t{dup_r}\t{dup_l}"
                  f"\t{len(scans)}\t{statuses}")
            for r in tsv:
                if r["doc_id"] in ours and ours[r["doc_id"]] != r["text"]:
                    print("  DIFF", r["doc_id"], repr(r["text"][:80]), repr(ours[r["doc_id"]][:80]))
                    break
            only_t = set(tsv_ids) - set(ours)
            only_o = set(ours) - set(tsv_ids)
            if only_t or only_o:
                print(f"  only_in_tsv={len(only_t)} only_in_ours={len(only_o)} e.g. {sorted(only_t)[:2]} {sorted(only_o)[:2]}")
            dup_tsv = sum(c - 1 for c in tsv_ids.values() if c > 1)
            if dup_tsv:
                print(f"  duplicate doc_ids inside the TSV: {dup_tsv}")
    print("LEGACY_OK", all_ok)

    print("\ncoverage: scan rows vs XML members, vs scan_stats")
    for sub, layer, invs in RUNS:
        root = run_dir / sub / layer
        for inv in invs:
            scans = list(read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz"))
            ours = {s["scan_id"] for s in scans}
            dropped = {s["scan_id"] for s in scans if s["status"] == "partial_shot_dropped"}
            n_scans = len(scans)
            n_xml = count_members(sub, inv)
            note = ""
            sp = Path(str(STATS).format(inv=inv))
            if sub == 'print':
                note = 'scan_stats-1.01.02 is another archive (1.10.94 not covered)'
            elif sp.exists():
                with gzip.open(sp, "rt", encoding="utf-8") as fh:
                    st = {r["doc_id"].removesuffix(".jpg") for r in csv.DictReader(fh, delimiter="\t")}
                a, b = (ours - dropped) - st, st - (ours - dropped)
                note = f"scan_stats={len(st)} ours_minus_dropped={len(ours - dropped)} only_ours={len(a)} only_stats={len(b)} {sorted(a)[:2]} {sorted(b)[:2]}"
            else:
                note = "no scan_stats"
            print(f"{inv}\tscan_rows={n_scans}\txml_members={n_xml}\t{'OK' if n_scans == n_xml else 'MISMATCH'}\t{note}")


def count_members(sub, inv):
    if sub == "hand_tar":
        p = SHARED / f"NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_{inv}-pagexml.tar.gz"
        with tarfile.open(p, "r:gz") as tf:
            return sum(1 for m in tf if m.isfile() and m.name.endswith(".xml")
                       and not m.name.rsplit("/", 1)[-1].startswith("._") and "__MACOSX" not in m.name)
    if sub == "hand_local":
        d = Path(f"data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_{inv}")
        return sum(1 for p in d.rglob("*.xml") if not p.name.startswith("."))
    p = SHARED / f"NL-HaNA_1.10.94/pagexml/NL-HaNA_1.10.94_{inv}.zip"
    with zipfile.ZipFile(p) as zf:
        return sum(1 for n in zf.namelist() if n.endswith(".xml") and "__MACOSX" not in n
                   and not n.rsplit("/", 1)[-1].startswith("."))


if __name__ == "__main__":
    main(sys.argv[1])
