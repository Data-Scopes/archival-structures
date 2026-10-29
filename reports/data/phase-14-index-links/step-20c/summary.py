"""Counts for the step-20c report, from entry_gold.tsv (run: .venv/bin/python -I summary.py from the AS root)."""
import csv, collections
rows = list(csv.DictReader(open("reports/data/phase-14-index-links/step-20c/entry_gold.tsv"), delimiter="\t"))
print("rows:", len(rows), "unique read_id:", len({r["read_id"] for r in rows}))
print("reserves used as replacement:", sum(1 for r in rows if r["used_for"] not in ("", "extra")), "| extra test reserves:", sum(1 for r in rows if r["used_for"] == "extra"))
def nloc(r): return len([x for x in r["locators"].split(";") if x])
def has_t(r): return r["target_scan_id"] not in ("none", "")
def ok_t(r): return has_t(r) and r["folio_evidence"] != "unclear"
for sp in ("dev", "test", "test:read", "test:extra", "all"):
    s = [r for r in rows if sp == "all" or r["split"] == sp.split(":")[0]]
    if sp == "test:read": s = [r for r in s if r["used_for"] != "extra"]
    if sp == "test:extra": s = [r for r in s if r["used_for"] == "extra"]
    print(f"\n== {sp}: {len(s)} rows")
    print(" verdict:", dict(collections.Counter(r["entry_verdict"] for r in s)))
    print(" locators 0/1/>=2:", sum(nloc(r) == 0 for r in s), sum(nloc(r) == 1 for r in s), sum(nloc(r) >= 2 for r in s),
          "| total locators", sum(nloc(r) for r in s))
    t = [r for r in s if has_t(r)]
    print(" rows with a target:", len(t), "| and folio_evidence != unclear:", sum(ok_t(r) for r in s))
    print(" folio_evidence:", dict(collections.Counter(r["folio_evidence"] for r in t)))
    print(" mean scans_opened (rows with target): %.1f" % (sum(int(r["scans_opened"]) for r in t) / max(1, len(t))))
    c = collections.Counter(r["content"] for r in t)
    print(" content:", dict(c))
    good = sum(c[k] for k in ("recto", "verso", "both"))
    print(" index accuracy (recto+verso+both)/rows with target: %d/%d = %.2f" % (good, len(t), good / max(1, len(t))))
    t2 = [r for r in t if r["folio_evidence"] != "unclear"]
    c2 = collections.Counter(r["content"] for r in t2)
    g2 = sum(c2[k] for k in ("recto", "verso", "both"))
    print(" ... excluding unclear numbering: %d/%d = %.2f" % (g2, len(t2), g2 / max(1, len(t2))))
print("\n== per volume (verdicts; targets; content)")
by = collections.defaultdict(list)
for r in rows: by[(r["split"], r["inv"])].append(r)
for (sp, inv), s in by.items():
    v = collections.Counter(r["entry_verdict"] for r in s)
    c = collections.Counter(r["content"] for r in s if has_t(r))
    print(f" {sp} {inv}: verdicts {dict(v)}; targets {sum(has_t(r) for r in s)}; content {dict(c)}; locators {sum(nloc(r) for r in s)}")
