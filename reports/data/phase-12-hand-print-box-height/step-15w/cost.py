"""Step 15w: XML bytes, output bytes and worker-seconds per archive of 15w-a, from status.tsv and du; per stage; modes."""
import subprocess
from pathlib import Path

import pandas as pd

for archive in ("1.01.02", "1.10.94"):
    root = next(Path(f"data/text_layer_v2_dev/15w-a/{archive}").glob("NL-HaNA_*_text_layer_v4"))
    st = pd.read_csv(root / "status.tsv", sep="\t", dtype={"inventory_number": str})
    st = st[st.status == "done"]
    xml = st[st.stage == "extract"].input_xml_bytes.sum()
    arch = st[st.stage == "extract"].input_bytes.sum()
    out_kib = int(subprocess.run(["du", "-sk", str(root)], capture_output=True, text=True).stdout.split()[0])
    sec = st.groupby("stage").seconds.sum()
    big = st[st.stage == "extract"].sort_values("input_xml_bytes").iloc[-1]
    print(f"{archive}: {st[st.stage == 'extract'].shape[0]} invs, XML {xml:,} B ({xml / 2**30:.2f} GiB), archive bytes {arch:,} "
          f"(XML/archive {xml / arch:.2f}), output {out_kib:,} KiB -> {out_kib * 1024 / xml:.5f} per XML byte")
    print("   worker-seconds per stage:", {k: round(v, 1) for k, v in sec.items()}, "total", round(sec.sum(), 1),
          f"-> {sec.sum() / xml:.3e} per XML byte; largest {big.inventory_number} {big.input_xml_bytes / 2**20:.0f} MiB, "
          f"{big.seconds:.0f} s extract")

# per mode (1.01.02): worker-seconds and output per XML byte
import json
root = next(Path("data/text_layer_v2_dev/15w-a/1.01.02").glob("NL-HaNA_*_text_layer_v4"))
st = pd.read_csv(root / "status.tsv", sep="\t", dtype={"inventory_number": str})
st = st[st.status == "done"]
xml = st[st.stage == "extract"].set_index("inventory_number").input_xml_bytes
sec = st.groupby("inventory_number").seconds.sum()
mode = pd.Series({i: json.loads((root / "mode" / f"{i}.json").read_text())["mode"] for i in xml.index})
for m in ("hand", "print"):
    ids = mode[mode == m].index
    kib = sum(int(subprocess.run(["du", "-sk", str(p)], capture_output=True, text=True).stdout.split()[0])
              for i in ids for p in root.glob(f"*/*{i}.*") if p.is_file()) if False else None
    print(f"{m}: {len(ids)} invs, XML {xml[ids].sum() / 2**30:.2f} GiB, worker-s {sec[ids].sum():.0f} -> "
          f"{sec[ids].sum() / xml[ids].sum():.3e} per XML byte (per-inventory range {(sec[ids] / xml[ids]).min():.2e}-"
          f"{(sec[ids] / xml[ids]).max():.2e})")
