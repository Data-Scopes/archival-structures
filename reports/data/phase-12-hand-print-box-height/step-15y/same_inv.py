"""Step 15y: are the files of one inventory in two layer roots equal?

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15y/same_inv.py INV ROOT_A ROOT_B

ROOT_* are layer roots (``.../NL-HaNA_1.01.02_text_layer_v4``). Every file whose name starts with ``<INV>.`` is
compared: same set of relative paths, equal decompressed bytes (JSON without ``seconds`` / ``elapsed*`` keys and without
``source``, the input path, which differs between a direct and a symlinked input dir).
Exit 0 if equal, 1 otherwise."""
import gzip
import json
import sys
from pathlib import Path


def files(root, inv):
    out = {}
    for p in sorted(Path(root).rglob(f"{inv}.*")):
        if not p.is_file():
            continue
        raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
        if p.name.endswith(".json"):
            doc = json.loads(raw)
            if isinstance(doc, dict):
                doc = {k: v for k, v in doc.items() if k not in ("seconds", "source") and not str(k).startswith("elapsed")}
            raw = json.dumps(doc, sort_keys=True).encode()
        out[str(p.relative_to(root))] = raw
    return out


def main(argv):
    inv, a, b = argv
    fa, fb = files(a, inv), files(b, inv)
    only_a, only_b = sorted(fa.keys() - fb.keys()), sorted(fb.keys() - fa.keys())
    diff = sorted(k for k in fa.keys() & fb.keys() if fa[k] != fb[k])
    print(f"{inv}: {len(fa)} files in A, {len(fb)} in B, {len(fa.keys() & fb.keys()) - len(diff)} equal")
    for label, items in (("only in A", only_a), ("only in B", only_b), ("differ", diff)):
        for k in items:
            print(f"  {label}: {k}")
    return 0 if not (only_a or only_b or diff) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
