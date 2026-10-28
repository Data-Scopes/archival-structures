"""Step 15y (P27): throughput model of the v4 build, fitted on the full run's status.tsv.

Run from the AS root:  .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15y/throughput.py

Reads (read only): data/text_layer_v4_full/<archive>/NL-HaNA_<archive>_text_layer_v4/{status.tsv,mode/<inv>.json,split_model/}
(the fit) and data/text_layer_v2_dev/15x-a/ (the out-of-sample check). Writes throughput.json, throughput.tsv,
residuals.tsv, validation.json next to this script. Per (inv, stage) the last `done` row counts; MB = input_xml_bytes / 1e6.
1.01.02: OLS seconds = intercept_s + s_per_mb * MB per stage over all volumes (both modes pooled), and per mode.
1.10.94 (2 volumes): intercept 0, s_per_mb = sum(seconds) / sum(MB).
Validation: the measured span of a stage = max(finished_utc) - min(finished_utc - seconds) over its last rows; the
prediction uses the worker count granted to that pass (full run: extract 7, wbc and join 4; 1.10.94 2; 15x-a: 4 and 2).
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from archival_structures.text_layer.presets import predict_seconds, predict_wall

HERE = Path(__file__).parent
STAGES = ["extract", "layout", "types", "units", "wbc", "join"]
POOL_STAGES = ("extract", "wbc", "join")
SERIAL_STAGES = ("layout", "types", "units")
FULL = Path("data/text_layer_v4_full")
DEV = Path("data/text_layer_v2_dev/15x-a")
WORKERS_FULL = {"1.01.02": {"extract": 7, "wbc": 4, "join": 4}, "1.10.94": {"extract": 2, "wbc": 2, "join": 2}}
WORKERS_DEV = {"1.01.02": {"extract": 4, "wbc": 4, "join": 4}, "1.10.94": {"extract": 2, "wbc": 2, "join": 2}}


def layer(root, archive):
    return Path(root) / archive / f"NL-HaNA_{archive}_text_layer_v4"


def load(root, archive):
    path = layer(root, archive)
    d = pd.read_csv(path / "status.tsv", sep="\t", dtype={"inventory_number": str}, low_memory=False)
    d = d[d.status == "done"].drop_duplicates(["inventory_number", "stage"], keep="last").copy()
    d["mb"] = d.input_xml_bytes.astype(float) / 1e6
    d["t"] = pd.to_datetime(d.finished_utc)
    d["t0"] = d.t - pd.to_timedelta(d.seconds, unit="s")
    modes = {}
    for inv in d.inventory_number.unique():
        modes[inv] = json.loads((path / "mode" / f"{inv}.json").read_text())["mode"]
    d["mode"] = d.inventory_number.map(modes)
    d["split"] = d.inventory_number.map(lambda i: (path / "split_model" / f"{i}.json").exists())
    return d


def ols(x, y):
    if len(x) < 3:
        return None
    b, a = np.polyfit(x, y, 1)
    pred = a + b * x
    ss_res, ss_tot = ((y - pred) ** 2).sum(), ((y - y.mean()) ** 2).sum()
    return {"intercept_s": float(a), "s_per_mb": float(b), "r2": float(1 - ss_res / ss_tot) if ss_tot else None}


def sig4(v):
    return float(f"{v:.4g}")


def span_h(sub):
    return float((sub.t.max() - sub.t0.min()).total_seconds() / 3600)


def predict_span(sizes, stage, fit, workers):
    if stage in POOL_STAGES:
        return predict_wall(sizes, workers[stage], fit, stages=(stage,))
    return sum(predict_seconds(b, stage, fit) for b in sizes)


def main():
    fits, rows, resid = {}, [], []
    data = {a: load(FULL, a) for a in ("1.01.02", "1.10.94")}
    for archive, d in data.items():
        fits[archive] = {}
        for stage in STAGES:
            s = d[d.stage == stage]
            if archive == "1.01.02":
                f = ols(s.mb.values, s.seconds.values)
            else:
                f = {"intercept_s": 0.0, "s_per_mb": float(s.seconds.sum() / s.mb.sum()), "r2": None}
            fits[archive][stage] = {k: (sig4(v) if v is not None else None) for k, v in f.items()}
            rows.append({"archive": archive, "stage": stage, "mode": "all", "n": len(s), **fits[archive][stage],
                         "worker_h": s.seconds.sum() / 3600, "measured_span_h": span_h(s)})
            if archive == "1.01.02":
                for mode in ("hand", "print"):
                    m = s[s["mode"] == mode]
                    f = ols(m.mb.values, m.seconds.values)
                    rows.append({"archive": archive, "stage": stage, "mode": mode, "n": len(m),
                                 "intercept_s": f["intercept_s"], "s_per_mb": f["s_per_mb"], "r2": f["r2"],
                                 "worker_h": m.seconds.sum() / 3600, "measured_span_h": None})
                top = s.assign(pred=lambda x: fits[archive][stage]["intercept_s"] + fits[archive][stage]["s_per_mb"] * x.mb)
                top = top.assign(resid=top.seconds - top.pred).nlargest(5, "resid")
                ex = d[(d.stage == "extract")].set_index("inventory_number")
                for _, r in top.iterrows():
                    e = ex.loc[r.inventory_number]
                    resid.append({"stage": stage, "inv": r.inventory_number, "mb": round(r.mb, 1), "seconds": r.seconds,
                                  "pred": round(r.pred, 1), "resid": round(r.resid, 1), "mode": r["mode"],
                                  "split": r.split, "n_scans": int(e.n_scans), "n_errors": int(e.n_errors),
                                  "n_rows_extract": int(e.n_rows), "n_lines": int(e.n_lines)})
    # validation: spans, in sample (full run) and out of sample (15x-a)
    val = []
    for label, root, workers in (("in_sample", FULL, WORKERS_FULL), ("out_of_sample", DEV, WORKERS_DEV)):
        for archive in ("1.01.02", "1.10.94"):
            d = data[archive] if root == FULL else load(root, archive)
            ex = d[d.stage == "extract"]
            sizes = list(ex.mb.values * 1e6)
            for stage in STAGES:
                meas = span_h(d[d.stage == stage])
                pred = predict_span(sizes, stage, fits[archive], workers[archive]) / 3600
                val.append({"sample": label, "archive": archive, "stage": stage, "workers": workers[archive].get(stage, 1),
                            "measured_span_h": meas, "predicted_span_h": pred, "error_pct": 100 * (pred - meas) / meas})
            ser_m = sum(v["measured_span_h"] for v in val[-6:] if v["stage"] in SERIAL_STAGES)
            tot_m = sum(v["measured_span_h"] for v in val[-6:])
            ser_p = sum(v["predicted_span_h"] for v in val[-6:] if v["stage"] in SERIAL_STAGES)
            tot_p = sum(v["predicted_span_h"] for v in val[-6:])
            val.append({"sample": label, "archive": archive, "stage": "TOTAL", "workers": None, "measured_span_h": tot_m,
                        "predicted_span_h": tot_p, "error_pct": 100 * (tot_p - tot_m) / tot_m,
                        "serial_share_measured": ser_m / tot_m, "serial_share_predicted": ser_p / tot_p})
    for r in rows:
        v = [x for x in val if x["sample"] == "in_sample" and x["archive"] == r["archive"] and x["stage"] == r["stage"]]
        r["predicted_span_h"] = v[0]["predicted_span_h"] if v and r["mode"] == "all" else None
    # sums for the run script constants
    ex = data["1.01.02"][data["1.01.02"].stage == "extract"]
    consts = {"archive_xml_ratio": round(float(ex.input_xml_bytes.astype(float).sum() / ex.input_bytes.astype(float).sum()), 2),
              "xml_bytes": {a: float(d[d.stage == "extract"].input_xml_bytes.astype(float).sum()) for a, d in data.items()},
              "print_byte_share": float(ex[ex["mode"] == "print"].mb.sum() / ex.mb.sum())}
    (HERE / "throughput.json").write_text(json.dumps({"fits": fits, "constants": consts}, indent=1))
    pd.DataFrame(rows).to_csv(HERE / "throughput.tsv", sep="\t", index=False, float_format="%.5g")
    pd.DataFrame(resid).to_csv(HERE / "residuals.tsv", sep="\t", index=False)
    (HERE / "validation.json").write_text(json.dumps(val, indent=1))
    print(pd.DataFrame(rows).to_string())
    print(pd.DataFrame(val).to_string())
    print(consts)


if __name__ == "__main__":
    main()
