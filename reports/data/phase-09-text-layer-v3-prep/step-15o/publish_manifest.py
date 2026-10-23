"""Step 15o: add the `publication` key to the two published text_layer_v3 manifests.

    .venv/bin/python publish_manifest.py [--dry-run]

Reads (all local): reports/.../step-15o/audit_published.json, the v2 manifests in shared-data (to check the
archive sha256s), data/text_layer_v3_full_inputs/{hand,print}/ (symlinks, resolved). Writes only the two
manifest.json files of the published dirs (key `publication`, other keys unchanged). Stops on a hash mismatch.
"""
import argparse
import datetime
import hashlib
import json
import sys
from pathlib import Path

AS = Path(__file__).resolve().parents[4]
SD = Path("/Users/Shared/claude_projects/data_scopes/shared-data")
LOTA_PARA = Path("/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94")
INPUTS = AS / "data/text_layer_v3_full_inputs"
AUDIT = Path(__file__).parent / "audit_published.json"
HAND = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
PRINT = ["455", "456"]
SIDES = {"hand": ("NL-HaNA_1.01.02", HAND), "print": ("NL-HaNA_1.10.94", PRINT)}
KNOWN_GAPS = [
    "stages.*.argv in manifest.json, `scans.member` and `extract_stats/*.json` `source` name dev paths "
    "(data/text_layer_v3_full_inputs/...); the tables are not rewritten",
    "the stage `version` field says 0.13.0 (stale editable-install metadata); the code is v0.14.0 at git_head "
    "(dirty only in archival_structures/text_layer/build.py, a --republic-dir help string)",
    "merge share with a lower-case right side is below 0.95 for 3351 (0.9415), 3823 (0.9414), 455 (0.9477), "
    "456 (0.932); reported, not blocking",
    "glued words (missing whitespace in print) are not repaired (UA-003); whitespace repairs = 0",
    "print REPUBLIC paragraphs come from LotA data/paragraphs/NL-HaNA_1.10.94/, not from a shared-data dataset",
    "coverage is partial: pilot inventories only (14 hand, 2 print); a full SG run will be a new version, not "
    "added into this one (the run-level word-break corpus default would change)",
]
RANK = ["producer", "as_version", "git_head", "git_dirty_files", "built_by", "layer", "format_version", "coverage",
        "predecessor", "inventories", "date", "audit", "counts", "rows116", "known_gaps", "schema", "built_from"]


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def v2_hashes():
    out = {}
    for d in ("NL-HaNA_1.01.02", "NL-HaNA_1.10.94"):
        m = json.load(open(SD / d / "text_layer_v2/manifest.json"))["publication"]
        out.update({b["inventory"]: b["sha256"] for b in m["built_from"] if "sha256" in b})
    return out


def built_from(side, invs, ref):
    rows = []
    for inv in invs:
        arch = "NL-HaNA_1.01.02" if side == "hand" else "NL-HaNA_1.10.94"
        cands = list((INPUTS / side).glob(f"{arch}_{inv}*"))
        assert len(cands) == 1, (inv, cands)
        p = cands[0].resolve()
        if p.is_dir():
            rows.append({"inventory": inv, "kind": "directory","path": str(p), "n_files": sum(1 for x in p.rglob("*") if x.is_file())})
        else:
            kind = "tar.gz" if p.name.endswith(".tar.gz") else "zip"
            h = sha256(p)
            if h != ref[inv]:
                sys.exit(f"sha256 mismatch for {inv}: {h} != {ref[inv]}")
            rows.append({"inventory": inv, "kind": kind, "path": str(p), "sha256": h})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    audit = json.load(open(AUDIT))
    ref = v2_hashes()
    today = datetime.date.today().isoformat()
    pubs = {}
    for side, (arch, invs) in SIDES.items():
        fails = [i["invariant"] for i in audit["invariants"] if not i["pass"]]
        pub = {
            "producer": "archival-structures", "as_version": "0.14.0",
            "git_head": "fa5cc63b2655df5d1d267b4af6f6a26d7556c683",
            "git_dirty_files": ["archival_structures/text_layer/build.py"],
            "built_by": "scripts/run_text_layer_v3_full.py (step 15n), dev layer 15n-a",
            "layer": "text_layer_v3", "format_version": 2, "coverage": "partial", "predecessor": "text_layer_v2",
            "inventories": invs, "date": today,
            "audit": {"n_pass": audit["n_pass"], "n_fail": audit["n_fail"], "failing_invariants": fails,
                      "source": "archival-structures reports/data/phase-09-text-layer-v3-prep/step-15o/audit_published.json"},
            "counts": {i: audit["counts"][i] for i in invs},
            "rows116": {"predicted_share_free": 0.526, "upper_bound": 0.560, "target": 0.80,
                        "source": "AS reports/phase-09-text-layer-v3-prep.md, Step 15m"},
            "known_gaps": KNOWN_GAPS,
            "schema": "docs/text_layer.md (AS) §§ layout, types, units, join; changes since v2: § Text layer v3 (phase 09)",
            "built_from": built_from(side, invs, ref),
        }
        if side == "print":
            paras = {}
            for i in invs:
                p = (LOTA_PARA / f"paragraphs-{i}.tsv.gz").resolve()
                paras[i] = {"path": str(p), "sha256": sha256(p)}
            pub["republic_paragraphs"] = paras
        pubs[side] = pub
        mp = SD / arch / "text_layer_v3/manifest.json"
        m = json.load(open(mp))
        m.pop("publication", None)
        m["publication"] = pub
        if args.dry_run:
            print(json.dumps(m["publication"], indent=2, ensure_ascii=False)[:2500])
        else:
            with open(mp, "w") as f:
                json.dump(m, f, indent=2, ensure_ascii=False)
                f.write("\n")


if __name__ == "__main__":
    main()
