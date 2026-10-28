"""Step 15yr (step 6 of the brief): the corrected time model against what was measured. No bar.

Run from the AS root: .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15yr/validate.py

Measured: the full run's extract span 2.836 h (7 workers) and 15x-a's 0.372 h (4 workers); the UA-035 rebuild
(16 volumes, 3 workers, the 9,122-volume layer) 79 min. Predicted with the extract entries of THROUGHPUT_V4 as they
were before 15yr (OLD) and after (NEW); for the rebuild the whole-layer terms are added: OLD = two sizing scans
(a forced-extract call and a --stage all call, each about 538 s) + the wbc decision + the audit; NEW = the same
terms with the recorded sizes reused (no sizing).
"""
import importlib.util
import json
from pathlib import Path

from archival_structures.text_layer.presets import MODE_PINS_V4

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
spec = importlib.util.spec_from_file_location("run_v4_full", ROOT / "scripts" / "run_text_layer_v4_full.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
pf = importlib.util.spec_from_file_location("pf", HERE / "prepass_factor.py")
pfm = importlib.util.module_from_spec(pf)
pf.loader.exec_module(pfm)

OLD_EXTRACT = {"1.01.02": {"intercept_s": -1.47, "s_per_mb": 0.1408}, "1.10.94": {"intercept_s": 0.0, "s_per_mb": 0.404}}


def xml_mb(rows, only=None):
    return [int(r["input_xml_bytes"]) for i, r in rows.items() if only is None or i in only]


def extract_wall(archive, xml, workers, extract):
    fit = {**mod.THROUGHPUT_V4[archive], "extract": extract}
    return mod.predict_wall(xml, workers, fit, stages=("extract",))


def main():
    out = {}
    full = pfm.last_done_extract(pfm.FULL / "status.tsv")
    xml = xml_mb(full)
    for label, ex in (("old", OLD_EXTRACT["1.01.02"]), ("new", mod.THROUGHPUT_V4["1.01.02"]["extract"])):
        out[f"full_run_extract_h_{label}"] = extract_wall("1.01.02", xml, 7, ex) / 3600
    dev = pfm.last_done_extract(pfm.DEV / "status.tsv")
    dev_p = pfm.last_done_extract(pfm.DEV.parents[1] / "1.10.94/NL-HaNA_1.10.94_text_layer_v4/status.tsv")
    for label in ("old", "new"):
        t = 0
        for arch, rows in (("1.01.02", dev), ("1.10.94", dev_p)):
            ex = OLD_EXTRACT[arch] if label == "old" else mod.THROUGHPUT_V4[arch]["extract"]
            t += extract_wall(arch, xml_mb(rows), 4, ex)
        out[f"15x_a_extract_h_{label}"] = t / 3600
    pins = set(MODE_PINS_V4)
    sx = [int(r["input_xml_bytes"]) for i, r in full.items() if i in pins]
    n_total = len(full)
    for label, ex, sizing in (("old", OLD_EXTRACT["1.01.02"], 2 * 538.0), ("new", mod.THROUGHPUT_V4["1.01.02"]["extract"], 0.0)):
        fit = {**mod.THROUGHPUT_V4["1.01.02"], "extract": ex}
        serial = sum(mod.predict_seconds(b, st, fit) for st in mod.SERIAL_STAGES for b in sx)
        pool = sum(mod.predict_wall(sx, 3, fit, stages=(st,)) for st in mod.POOL_STAGES)
        whole = mod.WBC_DECISION_S + mod.AUDIT_S_PER_INVENTORY * (n_total + 2) + sizing
        out[f"rebuild_min_{label}"] = {"stages": (serial + pool) / 60, "whole_layer": whole / 60,
                                       "total": (serial + pool + whole) / 60}
    out["n_pins"] = len(pins)
    out["measured"] = {"full_run_extract_h": 2.836, "15x_a_extract_h": 0.372, "rebuild_min": 79}
    (HERE / "validate.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
