"""Part B: add the `publication` key to the two published manifests (shared-data text_layer_v2/)."""
import json
import os
import sys

SH = "/Users/Shared/claude_projects/data_scopes/shared-data"
AUD = "reports/data/phase-07-sg-units-and-pilot/step-15i2/audit.json"
HEAD = "73e15179404f4118fcb758a50a9db94a256550e8"
HAND_TAR = {"3349": "46ee0a68445e437fa85cc96bfc9ba384dcf575ad611426d1867abd1605ca7b04",
            "3350": "eee1cc91b1b41df580648f92a1d56b7834e292d682fe3e5d69be1f2b1768e527",
            "3351": "9f700c6f7e11b58fa65c195e6e8afd5559169092088dca86489213c82fcac797",
            "3352": "2d97790a381400f728de43f13736044122d9dcc9a2ba5e1623b7a5506ba4adb1",
            "3353": "b5e8a0d0e25a656472e91e341ee72adf81eff110c72b17d2bb245bbfce761169",
            "3354": "a8fe7c3757cb2a51f30431b0269e7488cc04fa4f1fee92ece126c16ed2dc2460"}
PRINT_ZIP = {"455": "3f483416061fef5d6bb559c663b878ed720950752fae737c875d067930898584",
             "456": "e0ca7d9799376b05476cf1e4033af73975451dbcc348b50115ca58a6db3d39c6"}
LOCAL = {"3771": 580, "3772": 596, "3773": 609, "3821": 441, "3822": 464, "3823": 443, "11182": 756, "11281": 1140}
A = "/Users/marijnkoolen/Code/Huygens/archival-structures"
DEV_NOTE = ("stages.*.argv in manifest.json and the member/source fields in the tables name "
            "data/text_layer_v2_dev/15i-src/... (AS dev paths, symlinks to the sources below); the tables are not rewritten")
COMMON_GAPS = [
    DEV_NOTE,
    "as_version is the value the build recorded (0.9.0, the installed version at build time; pyproject.toml says 0.10.0). "
    "The build ran on git_head plus uncommitted changes (stages.extract.archival_structures.git_dirty_files)",
    "glued words (missing whitespace in print) are not repaired (UA-003); whitespace repairs = 0",
    "split segments carry char-proportional boxes; print units never cross columns or pages",
    "merge-share (lower-case right side) < 0.95 for 3351, 3823, 11182, 11281, 455, 456 (reported, not blocking); "
    "17,128 merges (5.4%) in hand have an upper-case or digit right side and read wrong in 15 of 20 sampled",
    "join/ of 3349, 3350, 3353 (820 merges fewer), 3351 (3 fewer) changed with the `=` pin (UA-011 b); "
    "word-break set for hand: `-=„`, `-:=„` for 11281, `-„` for 3349, 3350, 3351, 3353, 3354",
    "coverage is partial: a pilot of 14 hand and 2 print inventories, not the full SG run (that will be text_layer_v3)",
]


def main():
    aud = json.load(open(AUD))
    fails = [i["invariant"] for i in aud["invariants"] if not i["pass"]]
    summary = {"n_pass": aud["n_pass"], "n_fail": aud["n_fail"], "failing_invariants": fails,
               "failure_cause": "all are merge-share (lower-case right side) < 0.95, reported not blocking",
               "word_break_stat": aud["word_break_stat"], "corpus_default": aud["corpus_default"],
               "source": "archival-structures reports/data/phase-07-sg-units-and-pilot/step-15i2/audit.json"}
    cfg = {"NL-HaNA_1.01.02": (list(HAND_TAR) + list(LOCAL)),
           "NL-HaNA_1.10.94": list(PRINT_ZIP)}
    for arch, invs in cfg.items():
        p = f"{SH}/{arch}/text_layer_v2/manifest.json"
        assert not os.path.islink(p) and not os.path.islink(os.path.dirname(p))
        m = json.load(open(p))
        if arch == "NL-HaNA_1.01.02":
            built = [{"inventory": i, "kind": "tar.gz",
                      "path": f"{SH}/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_{i}-pagexml.tar.gz", "sha256": h}
                     for i, h in HAND_TAR.items()]
            built += [{"inventory": i, "kind": "directory",
                       "path": f"{A}/data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_{i}",
                       "n_files": n, "sha256": None} for i, n in LOCAL.items()]
        else:
            built = [{"inventory": i, "kind": "zip", "path": f"{SH}/NL-HaNA_1.10.94/pagexml/NL-HaNA_1.10.94_{i}.zip",
                      "sha256": h} for i, h in PRINT_ZIP.items()]
        m["publication"] = {
            "producer": "archival-structures", "as_version": "0.9.0", "git_head": HEAD,
            "layer": "text_layer_v2", "format_version": 2, "coverage": "partial", "inventories": invs,
            "date": "2026-10-21", "audit": summary,
            "counts": {i: aud["counts"][i] for i in invs},
            "known_gaps": COMMON_GAPS,
            "schema": "docs/text_layer.md (AS) §§ layout, types, units, join",
            "built_from": built}
        json.dump(m, open(p, "w"), indent=1, ensure_ascii=False)
        print("wrote", p, len(invs))


if __name__ == "__main__":
    main()
