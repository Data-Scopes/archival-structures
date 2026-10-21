"""Step 15i A6: crops of the hand and print line boxes for the user's row-level check.

    .venv/bin/python crops.py     # writes crops/*.jpg and crops/index.html next to this file
Rows: 39, 60 (unlabelled, from rows_extra.tsv) and 241, plus 30 labelled rows from random.Random(20).
Boxes are drawn on LotA's 1200 px thumbnails (scale = 1200 / page_w); IIIF links are built, never fetched.
"""
import csv
import html
import json
import random
from pathlib import Path

from PIL import Image, ImageDraw

HERE = Path(__file__).parent
LOTA = Path("/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/scan_info")
USER_ROWS = ["39", "60", "241"]
MAX_BYTES = 300_000
PAD = 70
COL = {"hand": (220, 30, 30), "print": (30, 60, 220)}


def read(p):
    return list(csv.DictReader(open(p, newline=""), delimiter="\t"))


def thumb_paths(side, scan):
    parts = scan.split("_")
    arch = "_".join(parts[:2])
    inv = parts[2]
    d = LOTA / arch / inv
    return d / f"{scan}.jpg", d / f"{scan}-info.json"


def iiif_base(info):
    j = json.loads(Path(info).read_text())
    return j.get("id") or j.get("@id"), j["width"], j["height"]


def draw_crop(side, scan, boxes, out):
    jpg, info = thumb_paths(side, scan)
    if not jpg.exists() or not info.exists():
        return None, None
    base, w, h = iiif_base(info)
    im = Image.open(jpg).convert("RGB")
    sc = im.width / w
    dr = ImageDraw.Draw(im)
    for b in boxes:
        x, y, bw, bh = (v * sc for v in b[1:5])
        dr.rectangle([x, y, x + bw, y + bh], outline=COL[side], width=3)
    xs = [b[1] * sc for b in boxes] + [(b[1] + b[3]) * sc for b in boxes]
    ys = [b[2] * sc for b in boxes] + [(b[2] + b[4]) * sc for b in boxes]
    x0, x1 = min(xs) - PAD, max(xs) + PAD
    y0, y1 = min(ys) - PAD, max(ys) + PAD
    if x1 - x0 < 700:
        c = (x0 + x1) / 2
        x0, x1 = c - 350, c + 350
    box = (max(0, int(x0)), max(0, int(y0)), min(im.width, int(x1)), min(im.height, int(y1)))
    crop = im.crop(box)
    q = 85
    while True:
        crop.save(out, "JPEG", quality=q)
        if out.stat().st_size <= MAX_BYTES or q <= 40:
            break
        q -= 10
    out.chmod(0o664)
    return base, (box, sc)


def main():
    rows = {r["row_no"]: r for r in read(HERE / "rows116.tsv")}
    rows.update({r["row_no"]: r for r in read(HERE / "rows_extra.tsv")})
    pool = sorted((r for r in rows if rows[r]["label"] and r not in USER_ROWS), key=int)
    picked = USER_ROWS + sorted(random.Random(20).sample(pool, 30), key=int)
    out = HERE / "crops"
    out.mkdir(exist_ok=True)
    out.chmod(0o775)
    sections, total, n_missing = [], 0, 0
    for no in picked:
        r = rows[no]
        parts = [f"<h2 id='r{no}'>Row {no}: {html.escape(r['label'] or '(unlabelled)')}"
                 f"{' / ' + html.escape(r['second_label']) if r['second_label'] else ''}</h2>"]
        parts.append(f"<p class='note'>{html.escape(r['notes'] or '(no note)')}</p>")
        parts.append("<table class='tok'>"
                     f"<tr><th>hand tokens</th><td>{html.escape(r['hand_tokens'])}</td><td>found ({r['hand_found']}): "
                     f"{html.escape(r['hand_match'])}</td></tr>"
                     f"<tr><th>print tokens</th><td>{html.escape(r['print_tokens'])}</td><td>found ({r['print_found']}): "
                     f"{html.escape(r['print_match'])}</td></tr></table>")
        parts.append(f"<p>flags: {html.escape(r['flags'])} &middot; predicted: <b>{html.escape(r['predicted'])}</b> "
                     f"{html.escape(r.get('reason', ''))}</p>")
        for side in ("hand", "print"):
            boxes = json.loads(r[f"{side}_boxes"])
            by_scan = {}
            for b in boxes:
                by_scan.setdefault(b[0], []).append(b)
            if not by_scan:
                parts.append(f"<p class='miss'>{side}: tokens not found, no boxes.</p>")
                continue
            for scan, bs in by_scan.items():
                fn = out / f"row{no}_{side}_{scan.split('_')[-1]}.jpg"
                base, _ = draw_crop(side, scan, bs, fn)
                if base is None:
                    n_missing += 1
                    parts.append(f"<p class='miss'>{side}: {html.escape(scan)}: thumbnail or info.json not staged.</p>")
                    continue
                total += fn.stat().st_size
                parts.append(f"<div class='side {side}'><h3>{side}: {html.escape(scan)}</h3>"
                             f"<img src='{fn.name}' alt='row {no} {side}'><ul>")
                for b in bs:
                    link = f"{base}/{b[1]},{b[2]},{b[3]},{b[4]}/full/0/default.jpg"
                    parts.append(f"<li>line {html.escape(b[5][:8])} ({b[6]}): {b[1]},{b[2]},{b[3]},{b[4]} "
                                 f"<a href='{html.escape(link)}'>full resolution</a></li>")
                parts.append("</ul></div>")
        sections.append("\n".join(parts))
    nav = " ".join(f"<a href='#r{n}'>{n}</a>" for n in picked)
    doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>15i crops</title>
<style>body{{font-family:sans-serif;max-width:1000px;margin:1em auto;padding:0 1em}}
.side.hand h3{{color:#dc1e1e}}.side.print h3{{color:#1e3cdc}}img{{max-width:100%;border:1px solid #888}}
.note{{background:#f6f3e4;padding:.4em}}.miss{{color:#a00}}th{{text-align:left;padding-right:1em}}
h2{{border-top:2px solid #444;margin-top:2em}}</style></head><body>
<h1>Step 15i: boxes of the token spans on the thumbnails</h1>
<p>Red = hand line segments, blue = print. Check that the boxes cover the tokens. Rows {', '.join(USER_ROWS)} are yours
(60 is unlabelled in the gold sheet); the other 30 are random labelled rows (Random(20)).
Boxes are segment boxes: for a segment cut out of a line they are char-proportional.</p>
<p>{nav}</p>
{chr(10).join(sections)}</body></html>"""
    (out / "index.html").write_text(doc)
    (out / "index.html").chmod(0o664)
    print(len(picked), "rows;", total // 1000, "KB of crops;", n_missing, "missing thumbnails")


if __name__ == "__main__":
    main()
