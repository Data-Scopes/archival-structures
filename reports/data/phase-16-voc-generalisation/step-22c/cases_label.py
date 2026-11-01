"""Step 22c: category labels for the error analysis. Categories are assigned by rules written AFTER reading the
sampled cases (cases dump of eval_toc.py cases); the sampled cases were read by hand, the rules extend the labels
to every FN/FP so the counts cover the whole population. -> cases_read.tsv, categories.json"""
import json, random, re, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval_toc as E

gold = E.load_gold(); sel = E.load_sel()
vols = {i: E.Vol(i) for i in E.ALL if i != E.SPECIAL}
NUMLINE = re.compile(r"^\W*\d{1,4}\W*$")


def info(v, cfg, sid):
    fe = v.feats(cfg); fl = v.flags(cfg)
    idx = [i for i, k in enumerate(v.keys) if k[0] == sid]
    if not idx:
        return None
    i = idx[0]
    lines = [l["text"] for l in v.sides[i]]
    return fe[i], lines, [int(fl[j]) for j in idx]


def cat_fn(fe, lines):
    if fe is None:
        return "layout lost the side (no lines)"
    first = " ".join(lines[:5]).lower()
    short = sum(bool(NUMLINE.match(x)) for x in lines) / max(1, len(lines))
    if fe["n_lines"] <= 5:
        return "very short page (<=5 lines)"
    if re.search(r"register (der|van)", first) and fe["n_text8"] <= 40 and fe["end_num"] < 0.15:
        return "title/heading page of the register"
    if short >= 0.1 or re.search(r"\b(pag|fol|folo|fols)\b", first):
        return "folio numbers in own short lines or column absent"
    return "one-column TOC, numbers/dates not at line ends or starts"


def cat_fp(fe, lines):
    first = " ".join(lines[:6]).lower()
    if re.search(r"transport|ƒ|\bf ?\d+[:.]\d", first) or fe["end_num"] >= 0.3 and re.search(r"\d+\s*[:.]\s*\d+", first):
        return "account/ledger page (Transport, guilder amounts)"
    if re.search(r"\b(van|vant)\b.*\b(onder )?dato|\bonder dato\b", first):
        return "dated heading of an account (Van <place> onder dato ...)"
    return "other (letter text, roster, number column)"


def run():
    rng = random.Random(20261031)
    rows, pop = [], {}
    specs = [("dev", E.DEV, sel, "FN", 30), ("dev", E.DEV, sel, "FP", 30), ("dev", E.DEV, E.SG, "FN", 15),
             ("test", E.TEST, sel, "FN", 30), ("test", E.TEST, sel, "FP", 30)]
    for split, invs, cfg, kind, lim in specs:
        items = []
        for inv in invs:
            pred = vols[inv].scan_pred(cfg)
            for sid, toc, _ in gold[inv]:
                q = sid in pred
                if (kind == "FN" and toc and not q) or (kind == "FP" and q and not toc):
                    items.append((inv, sid))
        pop_cnt = Counter()
        for inv, sid in items:
            r = info(vols[inv], cfg, sid)
            fe, lines = (r[0], r[1]) if r else (None, [])
            pop_cnt[cat_fn(fe, lines) if kind == "FN" else cat_fp(fe, lines)] += 1
        pop[f"{split}_{E.cell_name(cfg)}_{kind}"] = {"n": len(items), "categories(all, rule-based)": dict(pop_cnt)}
        samp = sorted(rng.sample(items, lim)) if len(items) > lim else items
        for inv, sid in samp:
            r = info(vols[inv], cfg, sid)
            fe, lines = (r[0], r[1]) if r else (None, [])
            c = cat_fn(fe, lines) if kind == "FN" else cat_fp(fe, lines)
            note = ("; ".join(f"{k}={fe[k] if not isinstance(fe[k], float) else round(fe[k], 2)}" for k in ("n_lines", "col_share", "end_num", "date_start")) + " | " + " / ".join(x[:40] for x in lines[:5])) if fe else "no lines"
            rows.append([inv, sid, split, E.cell_name(cfg), 1 if kind == "FN" else 0, 0 if kind == "FN" else 1, c, note.replace("\t", " ")])
    E.write_tsv(E.HERE / "cases_read.tsv", ["inv", "scan_id", "split", "cell", "gold", "pred", "category", "note"], rows)
    (E.HERE / "categories.json").write_text(json.dumps(pop, indent=1)); (E.HERE / "categories.json").chmod(0o664)
    print(json.dumps(pop, indent=1))


run()
