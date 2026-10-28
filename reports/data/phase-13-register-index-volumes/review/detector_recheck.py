import sys, random
sys.path.insert(0, "reports/data/phase-13-register-index-volumes/step-19c")
import importlib.util as _u; _s = _u.spec_from_file_location("sel19c", "reports/data/phase-13-register-index-volumes/step-19c/select.py"); sel_mod = _u.module_from_spec(_s); _s.loader.exec_module(sel_mod)
import pandas as pd
fit = sel_mod.load("fit"); test = sel_mod.load("test")
print("fit invs", fit.inv.nunique(), "test invs", test.inv.nunique())
for k in range(1, 13):
    P, R, tp, fp, fn = sel_mod.score(sel_mod.predict(fit, sel_mod.variant_cfg(k)))
    print(f"fit V{k:02d} P {P:.4f} R {R:.4f} {tp}/{fp}/{fn}")
d = sel_mod.predict(test, sel_mod.variant_cfg(5))
P, R, tp, fp, fn = sel_mod.score(d)
print(f"test V05 P {P:.4f} R {R:.4f} {tp}/{fp}/{fn}")
# per volume and volume bootstrap
d = d[d.gt_label != ""].copy()
d["w"] = d.n_lines.astype(int); d["pos"] = d.gt_label.isin(["index_page", "respect_page"])
g = d.groupby("inv").apply(lambda v: pd.Series({"tp": (v.w*(v.pos & v.pred)).sum(), "fp": (v.w*(~v.pos & v.pred)).sum(),
                                               "fn": (v.w*(v.pos & ~v.pred)).sum()}), include_groups=False)
print(g[(g.tp+g.fp+g.fn) > 0].to_string())
rng = random.Random(1); invs = list(g.index); Ps = []; Rs = []
for _ in range(2000):
    s = g.loc[[rng.choice(invs) for _ in invs]].sum()
    Ps.append(s.tp/max(s.tp+s.fp, 1)); Rs.append(s.tp/max(s.tp+s.fn, 1))
Ps.sort(); Rs.sort()
print("bootstrap over volumes 90% CI P", round(Ps[100],4), round(Ps[1900],4), "R", round(Rs[100],4), round(Rs[1900],4))
# sensitivity of test to the thresholds the planner fixed (post-hoc diagnostic)
for c1 in (0.2, 0.25, 0.3):
    for c2 in (0.1, 0.15, 0.2):
        cfg = {**sel_mod.variant_cfg(5), "col_share_min": c1, "shape_min": c2}
        P1, R1, *_ = sel_mod.score(sel_mod.predict(test, cfg)); P2, R2, *_ = sel_mod.score(sel_mod.predict(fit, cfg))
        print(f"c1 {c1} c2 {c2}: fit P {P2:.4f} R {R2:.4f} | test P {P1:.4f} R {R1:.4f}")
