"""Step 15 acceptance checks. Usage: .venv/bin/python -I check.py <out_dir> [ref] [ab] [scanid]

Every output goes into <out_dir> (a scratchpad). Only AS functions are used, except the A/B
check, which loads a copy of NLP's voc_text_layer.py from <out_dir>/nlp_voc_text_layer.py.
Protected: NLP's output/voc_text_layer/voc_text_layer_v1 (sha256 of every file) and
data/voc_text_layer_v1 (size+mtime of wbc/ files, manifest.json, status.tsv).
"""
import filecmp, hashlib, importlib.util, json, shutil, sys, time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd

from archival_structures.text_layer.extract import extract_inventory, scan_id_matches
from archival_structures.text_layer.io import inventory_sort_key, read_tsv_gz, write_json_atomic, write_plain_tsv
from archival_structures.text_layer.join import join_inventory
from archival_structures.text_layer.word_breaks import (
    WORD_BREAK_STATS_COLUMNS, decide_word_breaks, format_stat, read_word_break_sets,
    write_word_break_stats_part)

NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
REF = NLP / "output/voc_text_layer/voc_text_layer_v1"
V1 = NLP / "data/voc_text_layer_v1"
PREFIX = "NL-HaNA_1.04.02"
INV22 = "1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split()
INV20 = "1105 4365 4387 7597 7613 8031 8402 8809 8889 9037 9121 9223 9541 9626A 9720 10115 10186 10359 10428 11013B".split()
OUT = Path(sys.argv[1])
NLP_COPY = OUT / "nlp_voc_text_layer.py"


def protected_state():
    state = {}
    for p in sorted(REF.rglob("*")):
        if p.is_file():
            state[str(p.relative_to(REF))] = hashlib.sha256(p.read_bytes()).hexdigest()
    for p in sorted((V1 / "wbc").rglob("*")) + [V1 / "manifest.json", V1 / "status.tsv"]:
        if p.is_file():
            st = p.stat()
            state["V1:" + str(p.relative_to(V1))] = [st.st_size, st.st_mtime_ns]
    return state


def load_nlp():
    spec = importlib.util.spec_from_file_location("nlp_vtl_copy", NLP_COPY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def strip_source(path):
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    d.pop("source", None)
    return d


def cmp_layer(inv, a, b):
    """list of (kind, ok) for the 5 tsv files + 2 JSONs of one inventory, a vs b layer roots."""
    res = []
    for rel in (f"lines/{inv}.lines.tsv.gz", f"regions_meta/{inv}.tsv.gz", f"regions/{inv}.tsv.gz",
                f"join_events/{inv}.tsv.gz", f"wbc/parts/{inv}.tsv"):
        res.append((rel, filecmp.cmp(a / rel, b / rel, shallow=False)))
    for rel in (f"extract_stats/{inv}.json", f"join_stats/{inv}.json"):
        res.append((rel, strip_source(a / rel) == strip_source(b / rel)))
    return res


def as_stage1(args):
    inv, out = args
    zipf = NLP / "pagexml" / f"{inv}.zip"
    t = time.time()
    extract_inventory(inv, zipf, out, scan_id_prefix=PREFIX)
    write_word_break_stats_part(inv, out)
    return inv, time.time() - t


def as_join(args):
    inv, out, wbcs = args
    t = time.time()
    join_inventory(inv, out, wbcs, write_events=True)
    return inv, time.time() - t


def run_ref():
    out = OUT / "ref"
    t0 = time.time()
    order = sorted(INV22, key=inventory_sort_key)
    with ProcessPoolExecutor(8) as ex:
        list(ex.map(as_stage1, [(i, out) for i in order]))
    t1 = time.time()
    frames = [pd.read_csv(out / "wbc/parts" / f"{i}.tsv", sep="\t", quoting=3,
                          dtype={"inventory_number": str, "char": str},
                          keep_default_na=False, na_values=[]) for i in order]
    stats = pd.concat(frames, ignore_index=True)
    nl = stats.drop_duplicates("inventory_number").set_index("inventory_number")["n_lines"].to_dict()
    inventory_lines = {i: int(nl.get(i, 0)) for i in order}
    per, corpus = decide_word_breaks(stats, inventory_lines)
    corpus["allow_partial"] = False
    corpus["n_inventories_missing_extract"] = 0
    write_json_atomic(out / "wbc/corpus_default.json", corpus)
    cols = ["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default", "char_status"]
    write_plain_tsv(out / "wbc/wbc_per_inventory.tsv", cols, [[r[c] for c in cols] for r in per])
    write_plain_tsv(out / "wbc/wbc_stats.tsv", WORD_BREAK_STATS_COLUMNS,
                    [[format_stat(v) for v in row]
                     for row in stats[WORD_BREAK_STATS_COLUMNS].itertuples(index=False, name=None)])
    sets, default = read_word_break_sets(out)
    with ProcessPoolExecutor(8) as ex:
        list(ex.map(as_join, [(i, out, sets.get(i, default)) for i in order]))
    t2 = time.time()
    ok = n = 0
    for f in ("corpus_default.json", "wbc_per_inventory.tsv", "wbc_stats.tsv"):
        same = filecmp.cmp(out / "wbc" / f, REF / "wbc" / f, shallow=False)
        ok += same; n += 1
        if not same: print("DIFF wbc", f)
    for inv in order:
        for rel, same in cmp_layer(inv, out, REF):
            ok += same; n += 1
            if not same: print("DIFF", rel)
    print(f"1. REF reproduction: {ok}/{n} identical; stage1 {t1-t0:.0f}s, wbc+join {t2-t1:.0f}s")


def ab_worker(inv):
    nlp = load_nlp()
    sets, default = read_word_break_sets(V1)
    wbcs = sets.get(inv, default)
    zipf = NLP / "pagexml" / f"{inv}.zip"
    a, b = OUT / "ab_as", OUT / "ab_nlp"
    t = time.time()
    extract_inventory(inv, zipf, a, scan_id_prefix=PREFIX)
    write_word_break_stats_part(inv, a)
    join_inventory(inv, a, wbcs, write_events=True)
    ta = time.time() - t
    t = time.time()
    nlp.extract_inventory(inv, zipf, b)
    nlp.wbc_stats_part(inv, b)
    nlp.join_inventory(inv, b, wbcs, write_events=True)
    tb = time.time() - t
    return inv, cmp_layer(inv, a, b), ta, tb


def run_ab():
    shutil.copy(NLP / "src/startpage/voc_text_layer.py", NLP_COPY)
    t0 = time.time()
    with ProcessPoolExecutor(8) as ex:
        results = list(ex.map(ab_worker, INV20))
    ok = n = 0
    for inv, res, ta, tb in results:
        for rel, same in res:
            ok += same; n += 1
            if not same: print("DIFF", rel)
    ta = sum(r[2] for r in results); tb = sum(r[3] for r in results)
    print(f"2. A/B: {ok}/{n} equal (expect 140); cpu AS {ta:.0f}s vs NLP {tb:.0f}s; wall {time.time()-t0:.0f}s")


def scanid_worker(inv):
    ids = {r["scan_id"] for r in read_tsv_gz(V1 / "regions_meta" / f"{inv}.tsv.gz")}
    ids |= set(json.loads((V1 / "extract_stats" / f"{inv}.json").read_text())["scans_without_text_regions"])
    bad = [i for i in ids if not scan_id_matches(i, inv, PREFIX)]
    return inv, len(ids), bad[:5], len(bad)


def run_scanid():
    t0 = time.time()
    invs = sorted(p.name[:-len(".tsv.gz")] for p in (V1 / "regions_meta").glob("*.tsv.gz"))
    with ProcessPoolExecutor(8) as ex:
        res = list(ex.map(scanid_worker, invs, chunksize=20))
    total = sum(r[1] for r in res)
    bad = [r for r in res if r[3]]
    print(f"3. scan-id check: {total} ids in {len(invs)} inventories; mismatching ids: "
          f"{sum(r[3] for r in bad)} in {len(bad)} inventories; {time.time()-t0:.0f}s")
    for r in bad[:10]:
        print("   ", r)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    phases = sys.argv[2:] or ["ref", "ab", "scanid"]
    before = protected_state()
    write_json_atomic(OUT / "protected_before.json", before)
    print("protected files hashed:", len(before))
    for ph in phases:
        {"ref": run_ref, "ab": run_ab, "scanid": run_scanid}[ph]()
    after = protected_state()
    print("4. protected state unchanged:", before == after)
