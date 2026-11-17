"""listlike: telling running text from list- and table-like elements, line by line (stdlib only).

Ported from shared-tools `listlike.py` (republic-segmentation 11c3, 2026-11-01) in AS phase 18 step 24a; the
shared-tools copy stays frozen; defaults are unchanged. The phase-18 variants (`LISTLIKE_CFG_C1`, `RULE_R1`,
`RULE_R2`, `LISTLIKE_CELLS`) are named constants after `DEFAULT_RULE`; none is a default.

Original docstring follows::


    Input: a sequence of line records (dicts) in reading order:
        text   str   the line text
        x0, x1 float left and right edge of the line (any unit, one unit per group)
        group  any   hashable key of the column (or page column); a window never crosses groups
      optional:
        y, h   float top/baseline and height (not used by the defaults; kept for callers)
        gx0, gw float the left edge and width of the line's column (per line); when absent they are min x0 and (max x1 - min x0) of the group
        edge   float left-edge position used for left-edge repetition (default x0); with `edge` the tolerance is cfg["edge_tol"],
                      without it cfg["edge_tol_frac"] * group width
        flags  dict name -> bool or float, per-line values the caller supplies (e.g. attendance, name density)

    window_features(lines, window=3, cfg=None) -> one dict per line (input order). The window of a line is the line, up to `window` lines above and
    below in the same group (fewer at the edges). Keys (NaN where undefined):
        cap_self, cap_share        first letter/digit is an upper-case letter (digits: not capital); window share
        prev_closed, closed_share  previous line of the group ends in a sentence close (cfg["closed"]); window share of lines that end closed
        re_self, re_mean, re_std   right edge (x1 - gx0) / gw clipped to [0, 1.5]; the line's, window mean and std
        short_share                window share of lines with right edge < cfg["short_re"]
        edge_rep                   share of the other window lines whose left edge lies within the tolerance of this line's
        num_self, num_share        line opens as a numbering (cfg["numbering"]); window share
        amt_share, numamt_share    window share of lines ending in a number/amount (cfg["amount_end"]); share that open as numbering or end as amount
        capw_self, capw_win        capitalised words per word; the line's and the window mean
        words                      words in the line
        tiny_self, tiny_win        share of letter tokens of cfg["tiny_len"] letters or fewer (HTR garbage proxy); window mean
        <flag>_self, <flag>_win    per caller flag: the line's value, and the window mean (a boolean flag gives a share)
        <flag>_any                 1.0 when the flag is set on any window line
    list_score(features, rule) -> int, the number of conditions of `rule` that hold; rule = {"conditions": [(key, op, threshold), ...], "min": k}.
    list_flag(features, rule) -> bool, score >= rule["min"]. No fitted model: thresholds are passed in. DEFAULT_RULE is an example, not tuned.

    Patterns are in cfg (DEFAULT_CFG): early-modern Dutch and French. Pass your own cfg dict (keys replace the defaults) for another language or period.
    Self-test: python -m archival_structures.analysis.listlike --selftest   (offline)
"""
import math
import re
import sys

NAN = float("nan")
DEFAULT_CFG = dict(
    # "1.", "12)", "IV.", "iv)", "Item", "Idem", "Art."/"Article", a bullet or dash
    numbering=r"^\W{0,2}(?:\d{1,3}|[IVXLC]{1,6}|[ivxlc]{1,6})\s?[.)]|^\W{0,2}(?:Item|Idem|Art(?:icle|\.))\b|^\s*[•·*\-–—]\s",
    # a digit or an amount word at the line end: gl., guldens, st., stuyvers, stuivers, florin sign, pound sign, ponden, schell., blancken, crs.; French: livres, sols, deniers, ecus
    amount_end=r"(?:\d|\bgl\.?|\bguldens?|\bst\.?|\bstuyvers?|\bstuivers?|ƒ|£|\bponden?|\bschell\.?|\bblancken|\bcrs?\.?|\blivres?|\bsols?|\bdeniers?|\bécus?|\becus?)[\s.,:;]*$",
    closed=r"[.;:][\"'»”’)\]\s]*$",
    letters=r"[^\W\d_]+",
    tiny_len=2, short_re=0.75, edge_tol=0.25, edge_tol_frac=0.02, re_clip=1.5,
)
DEFAULT_RULE = {"conditions": [("cap_share", ">=", 0.8), ("numamt_share", ">=", 0.3), ("short_share", ">=", 0.5)], "min": 2}
_OPS = {">=": lambda a, b: a >= b, ">": lambda a, b: a > b, "<=": lambda a, b: a <= b, "<": lambda a, b: a < b}

# Phase-18 candidates (step 24d grid); none is a default.
_AMOUNT_WORDS = (r"\bgl\.?|\bguldens?|\bst\.?|\bstuyvers?|\bstuivers?|ƒ|£|\bponden?|\bschell\.?|\bblancken|\bcrs?\.?"
                 r"|\blivres?|\bsols?|\bdeniers?|\bécus?|\becus?")
AMOUNT_END_LEADERS = (r"(?:" + _AMOUNT_WORDS + r")[\s.,:;]*$"
                      r"|(?:\.{2,}|-{2,}|„|" + _AMOUNT_WORDS + r"|\bfol?\.?|\bpag\.?|\bp\.)\s*[\d„\-—=]+[\s.,:;„\-—]*$")
LISTLIKE_CFG_C1 = {"amount_end": AMOUNT_END_LEADERS}   # a line-final number counts only after an amount word, a leader or a locator
RULE_R1 = {"conditions": DEFAULT_RULE["conditions"] + [("closed_share", "<=", 0.4)], "min": 2}
RULE_R2 = {"conditions": DEFAULT_RULE["conditions"] + [("closed_share", "<=", 0.4)], "min": 3}
LISTLIKE_CELLS = {"C0R0": (None, DEFAULT_RULE), "C0R1": (None, RULE_R1), "C0R2": (None, RULE_R2),
                  "C1R0": (LISTLIKE_CFG_C1, DEFAULT_RULE), "C1R1": (LISTLIKE_CFG_C1, RULE_R1),
                  "C1R2": (LISTLIKE_CFG_C1, RULE_R2)}


def group_shares(flags, keys):
    """{key: share of True} for parallel lists `flags` (bools) and `keys` (hashables), e.g. per side or per region."""
    if len(flags) != len(keys):
        raise ValueError("flags and keys differ in length")
    n, k = {}, {}
    for f, key in zip(flags, keys):
        n[key] = n.get(key, 0) + 1
        k[key] = k.get(key, 0) + (1 if f else 0)
    return {key: k[key] / n[key] for key in n}



def _first_cap(t):
    for ch in t:
        if ch.isalnum():
            return ch.isalpha() and ch.isupper()
    return False


def _fin(v):
    return v == v and v not in (math.inf, -math.inf)


def _mean(vals):
    v = [x for x in vals if _fin(x)]
    return sum(v) / len(v) if v else NAN


def _std(vals):
    v = [x for x in vals if _fin(x)]
    if not v:
        return NAN
    m = sum(v) / len(v)
    return math.sqrt(sum((x - m) ** 2 for x in v) / len(v))


def window_features(lines, window=3, cfg=None):
    c = dict(DEFAULT_CFG); c.update(cfg or {})
    rx = {k: re.compile(c[k], re.I if k in ("numbering", "amount_end") else 0) for k in ("numbering", "amount_end", "closed", "letters")}
    n = len(lines)
    groups = {}
    for i, r in enumerate(lines):
        groups.setdefault(r["group"], []).append(i)
    out = [None] * n
    for g, idx in groups.items():
        recs = [lines[i] for i in idx]
        ax0 = min(r["x0"] for r in recs); aw = max(r["x1"] for r in recs) - ax0
        GX = [r["gx0"] if r.get("gx0") is not None else ax0 for r in recs]                      # per-record extent when the caller gives one
        GW = [(r["gw"] if r.get("gw") is not None else aw) for r in recs]
        GW = [v if _fin(v) and v > 0 else NAN for v in GW]
        m = len(recs)
        T = [(r.get("text") or "") for r in recs]
        cap = [1.0 if _first_cap(t) else 0.0 for t in T]
        closed = [1.0 if rx["closed"].search(t.strip()) else 0.0 for t in T]
        re_ = [min(max((r["x1"] - GX[j]) / GW[j], 0.0), c["re_clip"]) if _fin(GW[j]) else NAN for j, r in enumerate(recs)]
        short = [(1.0 if v < c["short_re"] else 0.0) if _fin(v) else NAN for v in re_]
        num = [1.0 if rx["numbering"].search(t) else 0.0 for t in T]
        amt = [1.0 if rx["amount_end"].search(t.strip()) else 0.0 for t in T]
        numamt = [max(a, b) for a, b in zip(num, amt)]
        W = [t.split() for t in T]
        nw = [float(len(w)) for w in W]
        capw = [sum(1 for x in w if x[:1].isupper()) / max(len(w), 1) for w in W]
        tk = [rx["letters"].findall(t) for t in T]
        tiny = [sum(1 for x in k if len(x) <= c["tiny_len"]) / max(len(k), 1) for k in tk]
        has_edge = "edge" in recs[0]
        edge = [r["edge"] if has_edge else r["x0"] for r in recs]
        tol = c["edge_tol"] if has_edge else c["edge_tol_frac"] * (aw if _fin(aw) else 0.0)
        names = sorted({k for r in recs for k in (r.get("flags") or {})})
        fl = {k: [float((r.get("flags") or {}).get(k, 0.0)) for r in recs] for k in names}
        for j in range(m):
            lo, hi = max(0, j - window), min(m, j + window + 1)
            sl = lambda a: a[lo:hi]
            others = [k for k in range(lo, hi) if k != j and _fin(edge[k])]
            d = dict(cap_self=cap[j], cap_share=_mean(sl(cap)), prev_closed=closed[j - 1] if j > 0 else NAN, closed_share=_mean(sl(closed)),
                     re_self=re_[j], re_mean=_mean(sl(re_)), re_std=_std(sl(re_)), short_share=_mean(sl(short)),
                     edge_rep=(sum(1 for k in others if abs(edge[k] - edge[j]) <= tol) / len(others)) if (others and _fin(edge[j])) else NAN,
                     num_self=num[j], num_share=_mean(sl(num)), amt_share=_mean(sl(amt)), numamt_share=_mean(sl(numamt)),
                     capw_self=capw[j], capw_win=_mean(sl(capw)), words=nw[j], tiny_self=tiny[j], tiny_win=_mean(sl(tiny)))
            for k in names:
                d[k + "_self"] = fl[k][j]; d[k + "_win"] = _mean(sl(fl[k])); d[k + "_any"] = 1.0 if any(v > 0 for v in sl(fl[k])) else 0.0
            out[idx[j]] = d
    return out


def list_score(features, rule=None):
    rule = rule or DEFAULT_RULE
    if isinstance(features, (list, tuple)):
        return [list_score(f, rule) for f in features]
    s = 0
    for key, op, th in rule["conditions"]:
        v = features.get(key, NAN)
        if _fin(v) and _OPS[op](v, th):
            s += 1
    return s


def list_flag(features, rule=None):
    rule = rule or DEFAULT_RULE
    s = list_score(features, rule)
    return [x >= rule["min"] for x in s] if isinstance(s, list) else s >= rule["min"]


def _doc(texts, group="c", width=100.0, ragged=True):
    return [dict(text=t, x0=0.0, x1=(width if not ragged else width - (7 * i) % 40), group=group) for i, t in enumerate(texts)]


def selftest():
    prose = ["Op heden is by haer Hoog Mogende vergaderd geweest de", "heeren Gedeputeerden, die hebben gehoort het rapport van", "den Raedt, ende daerop gedelibereert zynde, is goetgevonden",
             "dat men de voorsz. Commissie sal verlenen, mits dat de", "selve binnen drie maenden sal worden uitgevoert ende", "daervan rapport sal gedaen worden aen haer Ho: Mo:", "ende dat de Griffier daervan sal ontfangen eene copie"]
    nums = ["1. Voor het leveren van hout", "2. Voor het maken van een brugge", "3. Voor het verven van de poort", "4. Voor het repareren van de sluys", "5. Voor het graven van een gracht", "6. Voor het schoonmaken van de haven", "7. Voor het betalen van den wacht"]
    table = ["Aen Jan Pietersz. voor hout gl. 120", "Aen Cornelis Jansz. voor steen gl. 45", "Aen Willem de Vries voor kalck gl. 33", "Aen Hendrick Claesz. voor yser gl. 210", "Aen Dirck Gerritsz. voor loon gl. 18", "Aen Pieter Adriaensz. voor teer gl. 9", "Aen Claes Jacobsz. voor tou gl. 64"]
    names = ["Jan Pietersz. van Beuningen", "Cornelis de Witt", "Willem van Oranje", "Hendrick Casimir", "Dirck van Hoorn", "Pieter de Groot", "Claes van Rijn"]
    docs = {"prose": _doc(prose), "numbered list": _doc(nums, ragged=True), "table of amounts": _doc(table), "name list": _doc(names, width=60.0)}
    for k, d in docs.items():
        for i, r in enumerate(d):
            r["flags"] = {"name": 1.0 if k == "name list" else 0.0}
    rule = {"conditions": [("cap_share", ">=", 0.8), ("numamt_share", ">=", 0.3), ("short_share", ">=", 0.5), ("name_win", ">=", 0.5)], "min": 2}
    res = {}
    for k, d in docs.items():
        F = window_features(d)
        assert len(F) == len(d)
        res[k] = sum(list_score(F, rule)) / len(F)
        print(f"  {k:18s} mean list score {res[k]:.2f}  cap_share {_mean([f['cap_share'] for f in F]):.2f}  numamt {_mean([f['numamt_share'] for f in F]):.2f}")
    assert res["prose"] == min(res.values()) and res["prose"] < min(v for k, v in res.items() if k != "prose"), res
    P = window_features(docs["prose"])
    assert P[0]["prev_closed"] != P[0]["prev_closed"]              # NaN for the first line of a group
    assert P[3]["cap_self"] == 0.0 and P[0]["cap_self"] == 1.0
    assert window_features(docs["numbered list"])[3]["num_share"] == 1.0
    assert window_features(docs["table of amounts"])[3]["amt_share"] == 1.0
    # windows never cross groups; input order kept
    mix = [dict(text="Een", x0=0, x1=10, group="a"), dict(text="Twee", x0=0, x1=10, group="b"), dict(text="Drie", x0=0, x1=10, group="a")]
    F = window_features(mix)
    assert F[0]["cap_share"] == 1.0 and F[1]["prev_closed"] != F[1]["prev_closed"] and F[2]["prev_closed"] == 0.0
    # own patterns
    F = window_features([dict(text="Article 1 foo", x0=0, x1=10, group=0)], cfg=dict(numbering=r"^Article \d"))
    assert F[0]["num_self"] == 1.0
    print("listlike selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        print(__doc__)
