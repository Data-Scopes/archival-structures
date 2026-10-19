"""Step 15f error analysis. Usage (from the AS root):
  .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15f/ea.py LAYER_ROOT OUT_TXT
Checks text conservation on every changed line; samples 50 real repairs (seed 17) with line
context and 30 long (>= 18 letter) tokens that remain unsplit in `text` (seed 17); prints the
repair-length and most frequent-repair tables."""
import collections
import random
import sys
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402
from archival_structures.text_layer.word_split import token_core  # noqa: E402

root, outp = Path(sys.argv[1]), Path(sys.argv[2])
rows = []
for p in sorted((root / "lines").glob("*.lines.tsv.gz")):
    for r in read_tsv_gz(p):
        r["inv"] = p.name.split(".")[0]
        rows.append(r)
changed = [r for r in rows if r["text"] != r["text_raw"]]
bad = [r for r in changed if r["text"].replace(" ", "") != r["text_raw"].replace(" ", "")]
edits = {}
for p in sorted((root / "word_split" / "edits").glob("*.tsv.gz")):
    edits[p.name.split(".")[0]] = list(read_tsv_gz(p))
mism = 0
nins = {}
for r in changed:
    nins[r["line_id"]] = r["text"].count(" ") - r["text_raw"].count(" ")
for inv, es in edits.items():
    pl = collections.Counter()
    for e in es:
        pl[e["line_id"]] += e["replacement"].count(" ")
    for lid, k in pl.items():
        mism += nins[lid] != k
print("changed lines", len(changed), "conservation violations", len(bad),
      "inserted-space/edit mismatches", mism)
all_edits = [(inv, e) for inv, es in edits.items() for e in es]
by_id = {r["line_id"]: r for r in rows}
rng = random.Random(17)
pick = all_edits if len(all_edits) <= 50 else rng.sample(all_edits, 50)
lines = [f"== {len(all_edits)} repairs, sample {len(pick)} =="]
for i, (inv, e) in enumerate(pick, 1):
    r = by_id[e["line_id"]]
    lines.append(f"{i:2d} [{e['original']} -> {e['replacement']}]  || {r['text']}")
freq = collections.Counter((e["original"].lower(), e["replacement"].lower()) for _, e in all_edits)
lines.append("== most frequent distinct repairs ==")
for (o, rep), c in freq.most_common(25):
    lines.append(f"{c:5d} {o} -> {rep}")
lines.append(f"distinct repairs {len(freq)}")
longs = []
for r in rows:
    for t in r["text"].split(" "):
        if len(token_core(t)[1]) >= 18:
            longs.append((t, r["text"]))
lines.append(f"== {len(longs)} long tokens (>=18 core chars) remain; sample 30 ==")
for i, (t, ctx) in enumerate(rng.sample(longs, min(30, len(longs))), 1):
    lines.append(f"{i:2d} {t}  || {ctx}")
lc = collections.Counter(t.lower() for t, _ in longs)
lines.append("== most frequent remaining long tokens ==")
for t, c in lc.most_common(20):
    lines.append(f"{c:5d} {t}")
outp.write_text("\n".join(lines) + "\n")
print("written", outp)

# Why do the sampled long tokens stay unsplit? (print/hand production vocabulary, preset params)
import json
from archival_structures.text_layer.io import read_tsv_gz as _r
from archival_structures.text_layer.word_split import WordSplitParams
vocab = {r["word"]: int(r["count"]) for r in _r(root / "word_split" / "vocab.tsv.gz")}
prm = WordSplitParams(**{k: v for k, v in json.loads((root / "word_split" / "params.json").read_text()).items()
                         if k in WordSplitParams.__dataclass_fields__})


def min_parts(low):
    n, INF = len(low), 99
    best = [INF] * (n + 1)
    best[0] = 0
    for j in range(1, n + 1):
        for i in range(0, j - prm.min_part_len + 1):
            if vocab.get(low[i:j], 0) >= prm.min_part_count and best[i] + 1 < best[j]:
                best[j] = best[i] + 1
    return best[n]


reasons = collections.Counter()
for t, ctx in longs:
    _, core, _ = token_core(t)
    if not core.isalpha():
        reasons["not all letters (internal punctuation or hyphen)"] += 1
        continue
    mp = min_parts(core.lower())
    if mp >= 99:
        reasons["no cover: a part is below min_part_count"] += 1
    elif mp == 1:
        reasons["attested word"] += 1
    elif mp > prm.max_parts:
        reasons["needs more than max_parts"] += 1
    else:
        reasons["cover exists, score does not beat unsplit"] += 1
print("reasons over all remaining long tokens:", dict(reasons))
with open(outp, "a") as fh:
    fh.write("== reasons (all remaining long tokens) ==\n" + json.dumps(dict(reasons), indent=1) + "\n")
