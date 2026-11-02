"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): locator L9 (L8 + connectors, trailing ranges, date/year guards) and the monotone filter on the GLOBALISE TOCs, map = 22b SEL (F17-P.7).
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/l9_proto.py
"""
import sys, re, pickle, os, dataclasses
sys.path.insert(0, "reports/data/phase-16-voc-generalisation/step-22d"); sys.path.insert(0, ".")
import eval_links as E
from archival_structures.analysis.index_links import extract_locators, VOC_LOCATOR_PARAMS, SG_LOCATOR_PARAMS
from archival_structures.analysis.page_numbering import longest_increasing_chain
from archival_structures.text_layer.presets import _TYPE_MONTHS
L8 = VOC_LOCATOR_PARAMS["L8"]
PRE = r"(?:(?:fo|fol|folio|pag|pag:s|p)\.?\s*)?"
CON = r"(?:a|à|tot|en|—|–|-|/|(?:\.\s*){2,3})"
NUM = PRE + r"(\d{1,4})(?:\s*(?:verso|v)\.?)?"
RANGE = NUM + r"\s*" + CON + r"\s*" + PRE + r"\d{1,4}(?:\s*(?:verso|v)\.?)?"
STAND = re.compile(r"(?:" + RANGE + r"|" + NUM + r")", re.I)
LEAD = re.compile(r"\W{0,3}" + RANGE + r"\b", re.I)
TRAIL = re.compile(r"(?:^|[\s„\-.])" + RANGE + r"\W*$", re.I)
TRAILN = re.compile(r"(?:[-.]\s*){3,}\W{0,3}" + NUM + r"\W*$", re.I)
DATEN = re.compile(r"(?:dato|den|tot|primo|ulto?)\s*$", re.I)
MONTH_AFTER = re.compile(r"^\s*[.,:'\"]*\s*" + _TYPE_MONTHS, re.I)
YEAR_BEFORE = re.compile(r"(?:a[„o\.]*o|anno|ao)\W*$|" + _TYPE_MONTHS + r"[a-z]*\.?\s*$", re.I)
STRIP = " \t„\"'.:,;-—–"
def l9(text, rng, guards=True):
    core = text.strip(STRIP)
    for pat, fn in ((STAND, "full"), (LEAD, "match"), (TRAIL, "search"), (TRAILN, "search")):
        m = pat.fullmatch(core) if fn == "full" else (pat.match(text) if fn == "match" else pat.search(text))
        if m:
            nums = [int(next(g for g in m.groups() if g))]
            break
    else:
        nums = []
        for loc in extract_locators(text, dataclasses.replace(L8, variant="L7"), folio_range=None):
            before, after = text[:loc.char_start], text[loc.char_end:]
            if guards and (DATEN.search(before) or MONTH_AFTER.match(after) or YEAR_BEFORE.search(before)):
                continue
            nums.append(loc.number)
    if rng:
        nums = [n for n in nums if rng[0] - 6 <= n <= rng[1] + 6]
    return nums
def run(variant, mono, invs, gold):
    hit = n = 0; cov = tot = 0
    for inv in invs:
        v = E.Vol(inv, gold)
        fmap = v.fmap(E.SEL_MAP, E.SEL_TAGS)
        rng = (int(fmap.chain[0, 1]), int(fmap.chain[-1, 1])) if len(fmap.chain) else None
        locs = []
        for sid, page, lid, text in v.entries("lines"):
            if variant == "L8":
                ns = [l.number for l in extract_locators(text, L8, folio_range=rng)]
            else:
                ns = l9(text, rng, guards=(variant == "L9g"))
            locs += [(sid, x) for x in ns]
        if mono and locs:
            keep = longest_increasing_chain([x + i * 1e-6 for i, (_, x) in enumerate(locs)])
            locs = [locs[i] for i in keep]
        rows = []
        for sid, x in locs:
            t = fmap.locate(x)
            if t is None:
                n += 1; continue
            p = v.pos[t.front[0]]; leaf = v.scan_ids[p:p + 2]
            hit += any(s in v.gold_starts for s in leaf); n += 1
            rows.append({"_leaf": leaf})
        c, T = v.coverage(rows); cov += c; tot += T
    return hit, n, round(hit / n, 3), cov, tot, round(cov / tot, 3)
gold = E.load_gold()
invs = [i for i in E.DEV + E.TEST if i != "8023"] + ["2775"]
for var in ("L8", "L9", "L9g"):
    for mono in (False, True):
        print(var, "mono" if mono else "", run(var, mono, invs, gold), flush=True)
