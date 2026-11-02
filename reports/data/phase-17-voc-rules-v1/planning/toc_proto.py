"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): one-column TOC detector grid on the 22 GLOBALISE volumes (F17-P.5). Caches side texts in $P17_CACHE_DIR/toc_vols.pkl.
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: P17_CACHE_DIR=<scratch> .venv/bin/python reports/data/phase-17-voc-rules-v1/planning/toc_proto.py
"""
import sys, re, itertools, pickle, os
sys.path.insert(0, "reports/data/phase-16-voc-generalisation/step-22c")
sys.argv = ["x"]
import eval_toc as E
from collections import defaultdict
gold = defaultdict(dict)
for r in E.read_tsv_gz_plain(E.GOLD):
    gold[r["inv"]][r["scan_id"]] = max(gold[r["inv"]].get(r["scan_id"], 0), int(r["toc"]))
P = os.path.join(os.environ["P17_CACHE_DIR"], "toc_vols.pkl")
if os.path.exists(P):
    vols = pickle.load(open(P, "rb"))
else:
    vols = {}
    for inv in E.ALL:
        v = E.Vol(inv)
        vols[inv] = (v.keys, [[l["text"] for l in s] for s in v.sides], v.scans)
    pickle.dump(vols, open(P, "wb"))
NUM = r"(?:fo\.?|fol\.?|f\.|pag\.?)?\s*\d{1,4}"
RANGE = NUM + r"\s*(?:verso\s*)?(?:a|à|tot|en|—|–|-|/|\.\s*\.\s*\.?)\s*\d{1,4}"
FOLIO_LINE = re.compile(r"^\W{0,3}(?:" + RANGE + r"|" + NUM + r")\W{0,3}(?:verso\W*)?$", re.I)
LEAD = re.compile(r"^\W{0,3}" + RANGE + r"\b", re.I)
TAIL = re.compile(RANGE + r"\W{0,3}(?:verso\W*)?$", re.I)
DATE = re.compile(r"^\W{0,2}(?:den\s+)?\d{1,2}\s*[.,:'\"]*\s*" + E._TYPE_MONTHS, re.I)
AMOUNT = re.compile(r"ƒ|\d+\s*[:.]\s*\d{1,2}\s*[:.]\s*\d{1,2}\b|transport|per\s+saldo|debet|credit", re.I)
TITLE = re.compile(r"regist|regyst|inhoud|indice|index", re.I)
def feats(lines):
    ls = [l.strip() for l in lines if len(l.strip()) >= 3]
    n = len(ls)
    if not n:
        return dict(n=0, fol=0, date=0, amt=0, title=False)
    fol = sum(bool(FOLIO_LINE.match(l) or LEAD.match(l) or TAIL.search(l)) for l in ls) / n
    date = sum(bool(DATE.match(l)) for l in ls) / n
    amt = sum(bool(AMOUNT.search(l)) for l in ls) / n
    title = any(TITLE.search(l) for l in ls[:5])
    return dict(n=n, fol=fol, date=date, amt=amt, title=title)
F = {inv: [feats(s) for s in v[1]] for inv, v in vols.items()}
def predict(inv, W, t, a, g, use_title):
    keys, _, scans = vols[inv]
    order = {s: i for i, s in enumerate(scans)}
    fl = []
    for k, f in zip(keys, F[inv]):
        inwin = order[k[0]] < W
        hit = inwin and f["amt"] < a and ((f["n"] >= 3 and f["fol"] + f["date"] >= t) or (use_title and f["title"]))
        fl.append(hit)
    idx = [i for i, x in enumerate(fl) if x]
    for i, j in zip(idx, idx[1:]):
        if j - i - 1 <= g:
            for k in range(i + 1, j):
                fl[k] = True
    return {keys[i][0] for i, x in enumerate(fl) if x}
def score(cfg, invs):
    tp = fp = fn = 0
    for inv in invs:
        pred = predict(inv, *cfg)
        for s, y in gold[inv].items():
            q = s in pred
            tp += y and q; fp += (not y) and q; fn += y and not q
    p = tp / (tp + fp) if tp + fp else 0; r = tp / (tp + fn) if tp + fn else 0
    return tp, fp, fn, round(p, 3), round(r, 3), round(2 * p * r / (p + r), 3) if p + r else 0
invs = E.ALL
res = []
for W, t, a, g, ut in itertools.product([1000000, 80, 60], [0.2, 0.3, 0.4, 0.5], [0.15, 0.3, 1.01], [0, 2, 4], [False, True]):
    res.append(((W, t, a, g, ut), score((W, t, a, g, ut), invs)))
res.sort(key=lambda x: -x[1][5])
for c, s in res[:12]:
    print(c, s)
print("no window, no guard, t .3 g2", score((1000000, .3, 1.01, 2, False), invs))
print("no window, guard .15", score((1000000, .3, .15, 2, False), invs))
print("window 60 no guard", score((60, .3, 1.01, 2, False), invs))
best = res[0][0]
for inv in invs:
    print(inv, score(best, [inv])[:3], end="; ")
