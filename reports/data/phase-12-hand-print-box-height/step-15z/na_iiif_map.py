"""Step 15z: map the 185 read scans to Nationaal Archief IIIF bases and check image sizes against the PageXML.
Fetches the 37 NA viewer pages (sequential, 1 s pause, cached in na_cache/<inv>.html), parses the embedded file list, writes
na_iiif_map.tsv (scan_id, iiif_base) for the read scans found, then fetches <base>/info.json (cached in na_cache/info/<scan>.json)
and compares width/height with imageWidth/imageHeight in the scan's PageXML (tar in shared-data). Writes na_dims_check.tsv
(scan_id, iiif_base, na_w, na_h, xml_w, xml_h, status) with status ok / scaled (same aspect, NA size is r = na_w/xml_w times the XML size) / size_differs / not_in_na_list. build_check_page.py gives crops to ok and scaled.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/na_iiif_map.py
"""
import json, re, subprocess, time, urllib.request
from pathlib import Path
import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height")
OUT = D / "step-15z"
CACHE = OUT / "na_cache"
TAR = "/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_{inv}-pagexml.tar.gz"
VIEW = "https://www.nationaalarchief.nl/onderzoeken/archief/1.01.02/invnr/{inv}/file/{scan}"
RX = re.compile(r'"label":"(NL-HaNA_[^"]+?)\.(?:jpg|tif)".*?"iiif":\{"url":"([^"]+?)/info\.json"')


def fetch(url, path):
    if path.exists():
        return path.read_text(encoding="utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    for k in range(4):
        try:
            time.sleep(1)
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            txt = urllib.request.urlopen(req, timeout=60).read().decode("utf-8")
            path.write_text(txt, encoding="utf-8"); path.chmod(0o664)
            return txt
        except Exception as e:
            print("retry", k, url, e)
            time.sleep(5 * 2 ** k)
    return None


reads = pd.read_csv(D / "planning/reads_15z.tsv", sep="\t", dtype={"inv": str})
CACHE.mkdir(exist_ok=True); (CACHE / "info").mkdir(exist_ok=True)
rows = []
reads["tok"] = reads.scan_id.str.split("_").str[2]  # inventory token as in the scan name, with suffix (7462A, 8828A)
for inv, g in reads.groupby("tok", sort=False):
    scans = g.sort_values("read_id").scan_id.tolist()
    html = fetch(VIEW.format(inv=inv, scan=scans[0]), CACHE / f"{inv}.html")
    mp = {}
    if html:
        html = html.replace("\\u0022", '"').replace("\\/", "/")
        mp = dict(RX.findall(html))
    print(inv, "list", len(mp))
    for s in scans:
        base = mp.get(s)
        na_w = na_h = None
        if base:
            t = fetch(base + "/info.json", CACHE / "info" / f"{s}.json")
            if t:
                j = json.loads(t); na_w, na_h = j.get("width"), j.get("height")
        out = subprocess.run(["tar", "-xzOf", TAR.format(inv=inv.rstrip("AB")), f"{inv.rstrip('AB')}/page/{s}.xml"], capture_output=True, text=True).stdout
        m = re.search(r'<Page [^>]*imageWidth="(\d+)" imageHeight="(\d+)"', out)
        xw, xh = (int(m[1]), int(m[2])) if m else (None, None)
        if not base: st = "not_in_na_list"
        elif (na_w, na_h) == (xw, xh) and xw: st = "ok"
        elif xw and na_w and abs(na_h - xh * na_w / xw) <= 1.5: st = "scaled"  # same aspect ratio, crop box is multiplied by na_w/xml_w
        else: st = "size_differs"
        rows.append(dict(scan_id=s, iiif_base=base or "", na_w=na_w, na_h=na_h, xml_w=xw, xml_h=xh, status=st))
df = pd.DataFrame(rows)
df[df.iiif_base != ""][["scan_id", "iiif_base"]].to_csv(OUT / "na_iiif_map.tsv", sep="\t", index=False)
df.to_csv(OUT / "na_dims_check.tsv", sep="\t", index=False)
for f in ("na_iiif_map.tsv", "na_dims_check.tsv"):
    (OUT / f).chmod(0o664)
print(df.status.value_counts().to_dict())
