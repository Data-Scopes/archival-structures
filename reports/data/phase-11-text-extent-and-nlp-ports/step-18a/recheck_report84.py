"""Re-check NLP report/84 (early_end -> END, all-lines and body variants) through the
AS text_extent options (`region_types`, `text_extent_table`), step 18a of phase 11.

Reads NLP's local PageXML zips and reference CSVs (read only), writes into --out-dir:
extents_all.tsv.gz, extents_body.tsv.gz, recheck.tsv, bins.tsv, run.log.

Command (zsh; run from the NLP root with NLP's interpreter, AS on PYTHONPATH):
  cd $NLP && python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run \\
    --project AS --label 18a --want 4 --min 1 --wait -- env PYTHONNOUSERSITE=1 \\
    PYTHONPATH=<AS root> venv/bin/python <this file> --nlp-root $NLP --out-dir <dir> --workers {workers}
Smoke test: add --limit 2 and a scratch --out-dir (the equality checks then fail by design).
"""
import argparse
import logging
import os
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

from archival_structures.analysis.text_extent import (
    BODY_REGION_TYPES_VOC, compute_page_text_extent, estimate_stable_margins,
    text_extent_table)

INVENTORIES = [1120, 1267, 1274, 1388, 1529, 1539, 1547, 1557, 2448, 2548, 2555, 2775,
               3142, 3891, 7923, 8023, 8121, 8237, 8276, 8284, 8697, 8834]
MARG = ['top', 'bot', 'left', 'right']
BINS = [-1, 0.02, 0.05, 0.07, 0.10, 0.15, 0.25, 0.40, 1]
log = logging.getLogger('recheck')


def _sn(pid):
    return os.path.splitext(pid)[0] if pid.lower().endswith((".jpg", ".tif", ".xml")) else pid


def process_inventory(args):
    nlp_root, inv = args
    from pagexml.parser import parse_pagexml_files_from_archive
    zpath = os.path.join(nlp_root, 'pagexml', f'{inv}.zip')
    rows_all, rows_body = [], []
    for page in parse_pagexml_files_from_archive(zpath, ignore_errors=True, silent_mode=True):
        a = compute_page_text_extent(page)
        sn = _sn(page.id)
        if a is None:  # NLP's page_extents.csv keeps such pages with n_lines == 0 (brief defect)
            rows_all.append(dict(inventory_number=inv, scanname=sn, n_lines=0))
            rows_body.append(dict(inventory_number=inv, scanname=sn, n_lines=0))
            continue
        rows_all.append(dict(inventory_number=inv, scanname=sn, n_lines=a['n_lines'],
                             **{m: a[m] for m in MARG}))
        b = compute_page_text_extent(page, region_types=BODY_REGION_TYPES_VOC)
        if b is None:
            rows_body.append(dict(inventory_number=inv, scanname=sn, n_lines=0))
        else:
            rows_body.append(dict(inventory_number=inv, scanname=sn, n_lines=b['n_lines'],
                                  **{m: b[m] for m in MARG}))
    return inv, rows_all, rows_body


def classify(ext):
    """Per inventory stable margins from pages with n_lines > 0, then text_extent_table."""
    parts = []
    for inv, g in ext[ext['n_lines'] > 0].groupby('inventory_number'):
        g = g.set_index('scanname')
        parts.append(text_extent_table(g[MARG], estimate_stable_margins(g[MARG])).assign(inventory_number=inv))
    return pd.concat(parts)


def pr(flag, end):
    tp = int((flag & end).sum())
    return int(flag.sum()), tp / flag.sum(), tp / end.sum()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--nlp-root', required=True)
    ap.add_argument('--out-dir', required=True)
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--limit', type=int, default=None)
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(a.out_dir, 'run.log'), level=logging.INFO,
                        format='%(asctime)s %(message)s')
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    ref = os.path.join(a.nlp_root, 'output', 'early_end')

    invs = INVENTORIES[:a.limit] if a.limit else INVENTORIES
    invs = sorted(invs, key=lambda i: -os.path.getsize(os.path.join(a.nlp_root, 'pagexml', f'{i}.zip')))
    log.info('inventories %s workers %d', invs, a.workers)
    all_rows, body_rows = [], []
    with Pool(max(1, a.workers)) as pool:
        for inv, ra, rb in pool.imap_unordered(process_inventory, [(a.nlp_root, i) for i in invs]):
            log.info('inventory %s: %d pages', inv, len(ra))
            all_rows += ra
            body_rows += rb
    ea = pd.DataFrame(all_rows).sort_values(['inventory_number', 'scanname']).reset_index(drop=True)
    eb = pd.DataFrame(body_rows).sort_values(['inventory_number', 'scanname']).reset_index(drop=True)
    ea.to_csv(os.path.join(a.out_dir, 'extents_all.tsv.gz'), sep='\t', index=False)
    eb.to_csv(os.path.join(a.out_dir, 'extents_body.tsv.gz'), sep='\t', index=False)

    res = []

    def rec(crit, exp, got, ok):
        res.append(dict(criterion=crit, expected=exp, measured=got, **{'pass': bool(ok)}))
        log.info('%s | expected %s | measured %s | %s', crit, exp, got, 'PASS' if ok else 'FAIL')

    # 1. extents equality
    na = pd.read_csv(os.path.join(ref, 'page_extents.csv'))
    nb = pd.read_csv(os.path.join(ref, 'page_extents_body.csv'))
    for mode, mine, theirs, ncol in (('all', ea, na, 'n_lines'), ('body', eb, nb, 'n_body_lines')):
        if a.limit:
            theirs = theirs[theirs['inventory_number'].isin(invs)]
        m = mine.merge(theirs, on='scanname', how='outer', suffixes=('', '_nlp'), indicator=True)
        left_only = int((m['_merge'] == 'left_only').sum())
        right_only = int((m['_merge'] == 'right_only').sum())
        both = m[m['_merge'] == 'both']
        rec(f'{mode}: rows AS/NLP', f'{len(theirs)}/{len(theirs)}', f'{len(mine)}/{len(theirs)}',
            len(mine) == len(theirs))
        rec(f'{mode}: scannames only in AS / only in NLP', '0/0', f'{left_only}/{right_only}',
            left_only == 0 and right_only == 0)
        dmax = {k: float((both[k] - both[k + '_nlp']).abs().max()) for k in MARG}
        # body pages without body lines: NaN on both sides, compare where defined
        rec(f'{mode}: max |d| margins', '<=1e-12', ' '.join(f'{k}={v:.3g}' for k, v in dmax.items()),
            all(not (v > 1e-12) for v in dmax.values()))
        nl = both['n_lines'].astype(int) == both[ncol].astype(int)
        rec(f'{mode}: n_lines equal', f'{len(both)}/{len(both)}', f'{int(nl.sum())}/{len(both)}', nl.all())

    # 2/3. classification
    lab = pd.read_csv(os.path.join(ref, 'page_table.csv'),
                      usecols=['inventory_number', 'scanname', 'end_last', 'n_lines', 'top', 'bot', 'text_extent'])
    lab = lab.rename(columns={'n_lines': 'n_lines_nlp'})
    lab['end_last'] = lab['end_last'].astype(bool)
    if a.limit:
        lab = lab[lab['inventory_number'].isin(invs)]
    tabs = {}
    for mode, ext in (('all', ea), ('body', eb)):
        t = classify(ext).reset_index()
        tabs[mode] = t
        m = lab.merge(t[['scanname', 'text_extent', 'top_gap', 'bot_gap']], on='scanname', how='left',
                      suffixes=('_nlp', ''))
        flag = (m['text_extent'] == 'early_end')
        n, p, r = pr(flag, m['end_last'])
        exp = {'all': (3112, 0.1397814910025707, 0.3154459753444525),
               'body': (2762, 0.17052860246198406, 0.34155184916606235)}[mode]
        ok = n == exp[0] and abs(p - exp[1]) < 1e-12 and abs(r - exp[2]) < 1e-12
        rec(f'{mode}: early_end->END n_flag, P, R', f'{exp[0]} {exp[1]:.16g} {exp[2]:.16g}',
            f'{n} {p:.16g} {r:.16g} (labelled rows {len(m)})', ok)
        if mode == 'all':
            c = m[m['text_extent'].notna()]
            eq = (c['text_extent'] == c['text_extent_nlp'])
            rec('all: class == page_table text_extent (pages with lines)', f'{len(c)}/{len(c)}',
                f'{int(eq.sum())}/{len(c)}', eq.all())
            c[~eq].to_csv(os.path.join(a.out_dir, 'class_mismatch.tsv'), sep='\t', index=False)
            # 4. bins
            c = c.assign(bin=pd.cut(c['bot_gap'], BINS))
            b = c.groupby('bin', observed=False).agg(n=('end_last', 'size'), END_rate=('end_last', 'mean')).reset_index()
            b['END_rate'] = b['END_rate'].round(3)
            b['bin'] = b['bin'].astype(str)
            nb_ref = pd.read_csv(os.path.join(ref, 'supp_end_rate_by_bot_bin.csv'), usecols=['bin', 'n', 'END_rate'])
            b = b.merge(nb_ref, left_on=b.index, right_on=nb_ref.index, suffixes=('', '_nlp')).drop(columns='key_0')
            b.to_csv(os.path.join(a.out_dir, 'bins.tsv'), sep='\t', index=False)
            okb = (b['n'] == b['n_nlp']).all() and np.allclose(b['END_rate'], b['END_rate_nlp'], atol=1e-12)
            rec('gap bins n and END_rate (8 bins)', '8/8',
                f"{int(((b['n'] == b['n_nlp']) & np.isclose(b['END_rate'], b['END_rate_nlp'], atol=1e-12)).sum())}/8", okb)
    pd.DataFrame(res).to_csv(os.path.join(a.out_dir, 'recheck.tsv'), sep='\t', index=False)
    # tables for error analysis
    tabs['all'].to_csv(os.path.join(a.out_dir, 'class_all.tsv.gz'), sep='\t', index=False)
    tabs['body'].to_csv(os.path.join(a.out_dir, 'class_body.tsv.gz'), sep='\t', index=False)
    log.info('done; all pass: %s', all(r['pass'] for r in res))


if __name__ == '__main__':
    main()
