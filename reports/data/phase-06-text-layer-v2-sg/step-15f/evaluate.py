"""Step 15f evaluation of the glued-word segmenter on synthetic glued tokens.

Usage: .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15f/evaluate.py \
           print|hand dev|test LAYERS_DIR OUT_DIR
(run from the AS root; LAYERS_DIR holds <inv>.lines.tsv.gz). `dev` runs the whole grid on dev
and writes OUT_DIR/<archive>_dev.json with the chosen parameters; `test` reads those, evaluates
dev and test once with them and writes OUT_DIR/<archive>_test.json. The vocabulary is built from
train scans only (sha1(scan_id) % 10 in 2..9; 0 = test, 1 = dev).
"""
import hashlib
import itertools
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.text_layer import io as tio  # noqa: E402
from archival_structures.text_layer.word_split import (  # noqa: E402
    WordSplitParams, build_vocabulary, is_candidate, maybe_segment, token_core, _is_word_core)

GRID = list(itertools.product((8, 10, 12), (3, 5, 10, 20), (0, 2, 4, 6)))  # mtl, mpc, penalty


def split_of(scan_id):
    m = int(hashlib.sha1(scan_id.encode()).hexdigest(), 16) % 10
    return "test" if m == 0 else "dev" if m == 1 else "train"


def load(layers):
    by = {"train": [], "dev": [], "test": []}
    for p in sorted(Path(layers).glob("*.lines.tsv.gz")):
        for r in tio.read_tsv_gz(p):
            if r["text_raw"]:
                by[split_of(r["scan_id"])].append(r["text_raw"])
    return by


def glue_pool(lines, min_len):
    """(glued core, tuple of true cut positions) for adjacent pairs/triples."""
    pool = []
    for text in lines:
        toks = [token_core(t) for t in text.split(" ")]
        n = len(toks)
        for i in range(n):
            for k in (2, 3):
                if i + k > n:
                    continue
                seg = toks[i:i + k]
                if not all(_is_word_core(c) for _, c, _ in seg):
                    continue
                if any(seg[m][2] or seg[m + 1][0] for m in range(k - 1)):
                    continue
                cores = [c for _, c, _ in seg]
                if sum(map(len, cores)) < min_len:
                    continue
                cuts, pos = [], 0
                for c in cores[:-1]:
                    pos += len(c)
                    cuts.append(pos)
                pool.append(("".join(cores), tuple(cuts)))
    return pool


def sample(pool, n=5000):
    rng = random.Random(17)
    return pool if len(pool) <= n else rng.sample(pool, n)


def cuts_of(parts):
    out, pos = [], 0
    for p in parts[:-1]:
        pos += len(p)
        out.append(pos)
    return tuple(out)


def metrics(items, vocab, params):
    total = sum(vocab.values())
    exact = nos = tp = pred = true = 0
    for glued, truth in items:
        parts = maybe_segment(glued, vocab, total, params)
        c = cuts_of(parts) if parts else ()
        if not parts:
            nos += 1
        exact += c == truth
        tp += len(set(c) & set(truth))
        pred += len(c)
        true += len(truth)
    n = len(items)
    return {"n": n, "exact": exact / n if n else 0, "boundary_p": tp / pred if pred else 0,
            "boundary_r": tp / true if true else 0, "no_split": nos / n if n else 0}


def real_share(lines, vocab, params):
    total = sum(vocab.values())
    cand = split = 0
    for text in lines:
        for t in text.split(" "):
            _, core, _ = token_core(t)
            if is_candidate(core, vocab, params):
                cand += 1
                split += maybe_segment(core, vocab, total, params) is not None
    return {"candidates": cand, "split": split, "share": split / cand if cand else 0}


def main():
    archive, mode, layers, out = sys.argv[1:5]
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    by = load(layers)
    vocab = build_vocabulary(by["train"])
    info = {"archive": archive, "lines": {k: len(v) for k, v in by.items()},
            "vocab_types": len(vocab), "vocab_tokens": sum(vocab.values())}
    samples = {}

    def items(split, mtl):
        key = (split, mtl)
        if key not in samples:
            samples[key] = sample(glue_pool(by[split], mtl))
        return samples[key]

    def evaluate(split, mtl, mpc, pen):
        params = WordSplitParams(min_token_len=mtl, min_part_count=mpc, split_penalty=pen)
        m = metrics(items(split, mtl), vocab, params)
        m["real"] = real_share(by[split], vocab, params)
        return m

    if mode == "dev":
        grid = []
        for mtl, mpc, pen in GRID:
            m = evaluate("dev", mtl, mpc, pen)
            grid.append({"min_token_len": mtl, "min_part_count": mpc, "split_penalty": pen, **m})
            print(archive, mtl, mpc, pen, round(m["exact"], 4), round(m["boundary_p"], 4),
                  round(m["boundary_r"], 4), round(m["real"]["share"], 4), flush=True)
        best = max(g["exact"] for g in grid)
        near = [g for g in grid if g["exact"] >= best - 0.02]
        chosen = max(near, key=lambda g: (g["min_part_count"], g["split_penalty"], g["min_token_len"]))
        info.update(grid=grid, best_exact=best, chosen={k: chosen[k] for k in
                    ("min_token_len", "min_part_count", "split_penalty")})
        (out / f"{archive}_dev.json").write_text(json.dumps(info, indent=1))
        print("CHOSEN", info["chosen"])
    else:
        ch = json.loads((out / f"{archive}_dev.json").read_text())["chosen"]
        args = (ch["min_token_len"], ch["min_part_count"], ch["split_penalty"])
        info.update(chosen=ch, dev=evaluate("dev", *args), test=evaluate("test", *args))
        (out / f"{archive}_test.json").write_text(json.dumps(info, indent=1))
        print(json.dumps({k: info[k] for k in ("chosen", "dev", "test")}, indent=1))


main()
