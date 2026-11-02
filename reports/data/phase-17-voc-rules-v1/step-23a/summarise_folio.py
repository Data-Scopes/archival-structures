"""Step 23a: folio_verdicts.json (exported from folio_page.html) -> gold/folio_gold.tsv + gold/folio_gold.done.
Run from the AS root, only after the user has exported (UA-061):
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/summarise_folio.py
Options (for tests): --verdicts F  --sample F  --gold-dir DIR.
folio_gold.tsv: item_id inv scan_id scan_num side_layout status number left_number beneath_number note, one row per
folio_sample.tsv item. status in number/none/unreadable/missing (missing = no verdict). Appends the folio_gold.tsv
hash to SHA256SUMS (replacing an earlier folio_gold line) and writes folio_gold.done (UTC time + hash). Refuses to
overwrite an existing folio_gold.done (gold is frozen)."""
import argparse, csv, datetime, hashlib, json, os
from collections import Counter
from pathlib import Path

D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
ap = argparse.ArgumentParser()
ap.add_argument("--verdicts", default=str(D / "folio_verdicts.json"))
ap.add_argument("--sample", default=str(D / "folio_sample.tsv"))
ap.add_argument("--gold-dir", default=str(D / "gold"))
a = ap.parse_args()
G = Path(a.gold_dir)
if (G / "folio_gold.done").exists():
    raise SystemExit("folio_gold.done exists: gold is frozen, not overwriting")
ver = {x["id"]: x for x in json.load(open(a.verdicts))["items"]}
sample = list(csv.DictReader(open(a.sample), delimiter="\t"))
assert set(ver) <= {s["item_id"] for s in sample}, "verdict ids not in the sample"


def clean(x):
    return str(x or "").replace("\t", " ").replace("\n", " ").strip()


G.mkdir(parents=True, exist_ok=True)
out = G / "folio_gold.tsv"
cnt = Counter()
with open(out, "w") as f:
    f.write("item_id\tinv\tscan_id\tscan_num\tside_layout\tstatus\tnumber\tleft_number\tbeneath_number\tnote\n")
    for s in sample:
        v = ver.get(s["item_id"], {})
        st = clean(v.get("status"))
        if st not in ("number", "none", "unreadable"):
            st = "missing"
        num = clean(v.get("number")) if st == "number" else ""
        if st == "number" and not num:
            st = "missing"
        cnt[st] += 1
        f.write("\t".join([s["item_id"], s["inv"], s["scan_id"], s["scan_num"], s["side_layout"], st, num,
                           clean(v.get("left_number")), clean(v.get("beneath_number")), clean(v.get("note"))]) + "\n")
h = hashlib.sha256(out.read_bytes()).hexdigest()
sums = G / "SHA256SUMS"
old = [l for l in (sums.read_text().splitlines() if sums.exists() else []) if not l.endswith("folio_gold.tsv")]
sums.write_text("\n".join(old + [f"{h}  folio_gold.tsv"]) + "\n")
(G / "folio_gold.done").write_text(f"{datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')}\n{h}  folio_gold.tsv\n")
for p in (out, sums, G / "folio_gold.done"):
    if not os.stat(p).st_mode & 0o020:  # only the owner may chmod; files another account owns are left as they are
        try:
            os.chmod(p, os.stat(p).st_mode | 0o020)
        except PermissionError:
            print("could not make group-writable (not the owner):", p)
print(dict(cnt), h[:12])
