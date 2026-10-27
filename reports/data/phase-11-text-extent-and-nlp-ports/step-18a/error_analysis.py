"""Step 18a error analysis: which region types set the margin on pages whose class changes
between the all-lines and body variants, plus text of the largest-bot_gap pages.

Needs the outputs of recheck_report84.py in --out-dir. Single process.
Run like recheck_report84.py (NLP interpreter, AS on PYTHONPATH). Writes
ea_changed_types.tsv, ea_samples.tsv, ea_gap_pages.tsv into --out-dir.
"""
import argparse, os
import pandas as pd
from pagexml.parser import parse_pagexml_files_from_archive
from archival_structures.analysis.text_extent import line_region_type

ap = argparse.ArgumentParser()
ap.add_argument('--nlp-root', required=True)
ap.add_argument('--out-dir', required=True)
a = ap.parse_args()
O = a.out_dir
ca = pd.read_csv(f'{O}/class_all.tsv.gz', sep='\t')
cb = pd.read_csv(f'{O}/class_body.tsv.gz', sep='\t')
m = ca.merge(cb, on=['scanname', 'inventory_number'], how='left', suffixes=('_a', '_b'))
lab = pd.read_csv(f'{a.nlp_root}/output/early_end/page_table.csv', usecols=['scanname', 'end_last'])
m = m.merge(lab, on='scanname', how='left')
chg = m[m.text_extent_b.notna() & (m.text_extent_a != m.text_extent_b)]
s1 = chg[(chg.text_extent_a == 'early_end') & (chg.text_extent_b == 'full_text')].sample(5, random_state=18)
s2 = chg[(chg.text_extent_a == 'full_text') & (chg.text_extent_b == 'early_end')].sample(5, random_state=18)
samples = set(s1.scanname) | set(s2.scanname)
lab2 = m.dropna(subset=['end_last'])
gp_end = lab2[lab2.end_last == True].nlargest(10, 'bot_gap_a')
gp_non = lab2[lab2.end_last == False].nlargest(10, 'bot_gap_a')
gaps = {**{s: 'END' for s in gp_end.scanname}, **{s: 'nonEND' for s in gp_non.scanname}}
changed = set(chg.scanname)
want = changed | samples | set(gaps)
inv_want = sorted(set(m[m.scanname.isin(want)].inventory_number))
types, samp, gp = [], [], []


def info(page):
    ls = [ln for ln in page.get_lines() if ln.coords]
    ls_b = sorted(ls, key=lambda l: l.coords.bottom)
    ls_t = sorted(ls, key=lambda l: l.coords.top)
    txt = lambda l: (l.text or '')[:70]
    return ls, ls_t, ls_b, txt


for inv in inv_want:
    for page in parse_pagexml_files_from_archive(f'{a.nlp_root}/pagexml/{inv}.zip', ignore_errors=True, silent_mode=True):
        sn = os.path.splitext(page.id)[0] if page.id.lower().endswith(('.jpg', '.tif', '.xml')) else page.id
        if sn not in want:
            continue
        ls, ls_t, ls_b, txt = info(page)
        if not ls:
            continue
        row = m[m.scanname == sn].iloc[0]
        if sn in changed:
            types.append(dict(scanname=sn, change=f'{row.text_extent_a}->{row.text_extent_b}',
                              bot_type=line_region_type(ls_b[-1]), top_type=line_region_type(ls_t[0])))
        if sn in samples:
            samp.append(dict(scanname=sn, change=f'{row.text_extent_a}->{row.text_extent_b}', end_last=row.end_last,
                             bot_type=line_region_type(ls_b[-1]), bot_text=txt(ls_b[-1]),
                             top_type=line_region_type(ls_t[0]), top_text=txt(ls_t[0]),
                             n_lines=len(ls)))
        if sn in gaps:
            body = [l for l in ls_b if line_region_type(l) in {'paragraph', 'signature-mark', 'header'}]
            gp.append(dict(scanname=sn, group=gaps[sn], bot_gap=row.bot_gap_a, n_lines=len(ls),
                           first_text=txt(ls_t[0]), last_text=txt(ls_b[-1]), last_type=line_region_type(ls_b[-1]),
                           last_body_text=txt(body[-1]) if body else ''))
pd.DataFrame(types).to_csv(f'{O}/ea_changed_types.tsv', sep='\t', index=False)
pd.DataFrame(samp).to_csv(f'{O}/ea_samples.tsv', sep='\t', index=False)
pd.DataFrame(gp).to_csv(f'{O}/ea_gap_pages.tsv', sep='\t', index=False)
