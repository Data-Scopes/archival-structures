"""15q error analysis: (1) dev scans whose label differs from the volume's class (30, seed 16);
(2) part D: 15p `mixed` staged volumes, scan-label runs and 5 scans at label changes (seed 16)."""
import sys, random
sys.path.insert(0, sys.argv[0].rsplit('/', 1)[0])
import validate as V
import pandas as pd
rnd = random.Random(16)
ds = pd.read_csv(V.OUT / 'dev_scans.tsv.gz', sep='\t')
ds = ds[ds.labelled]
bad = ds[((ds.cls == 'hand') & (ds.label == 'print')) | ((ds.cls == 'print') & (ds.label == 'hand'))]
print('dev mismatching labelled scans:', len(bad), bad.groupby(['cls']).size().to_dict())
cache = {}
for _, r in bad.sample(30, random_state=16).sort_values(['inv', 'scan_order']).iterrows():
    f = cache.setdefault(r.inv, V.load_regions('1.01.02', str(r.inv)))
    rs = sorted((x for x in f if x['scan_id'] == r.scan_id and x['hrel'] is not None), key=lambda x: -x['n_chars'])
    print(f"-- {r.inv} {r.cls}-vol, label {r.label}, value {r.value:.3f}, chars {r.n_chars}, {r.scan_id[-5:]}")
    for x in rs[:2]:
        print(f"   hrel {x['hrel']:.2f} lines {x['n_lines']} chars {x['n_chars']}: {x['text_head'][:130]}")
print('\n=== PART D')
st = pd.read_csv(V.OUT / 'staged_volumes.tsv', sep='\t', dtype={'inv': str})
sc = pd.read_csv(V.OUT / 'staged_scans.tsv.gz', sep='\t', dtype={'inv': str})
for _, v in st.iterrows():
    s = sc[(sc.inv == v.inv)]
    s = s[s.label != 'unlabelled']
    seq = list(s.label)
    changes = sum(a != b for a, b in zip(seq, seq[1:]))
    # compress runs
    runs = []
    for l in seq:
        if runs and runs[-1][0] == l: runs[-1][1] += 1
        else: runs.append([l, 1])
    print(f"{v.inv}: 15p {v.label15p}, mode {v['mode']}, share {v.print_scan_share:.3f}, labelled {len(seq)}, changes {changes}, runs {len(runs)}"
          + (f" first runs {[tuple(r) for r in runs[:8]]}" if v.label15p == 'mixed' else ''))
    if v.label15p == 'mixed' and changes:
        f = V.load_regions('1.01.02', str(v.inv))
        idx = [i for i in range(1, len(seq)) if seq[i] != seq[i-1]]
        pick = rnd.sample(idx, min(5, len(idx)))
        ss = s.reset_index(drop=True)
        for i in sorted(pick):
            r = ss.iloc[i]; p = ss.iloc[i-1]
            print(f"   change {p.label}->{r.label} at {r.scan_id[-5:]} (value {r.value:.3f}, prev {p.value:.3f})")
            rs = sorted((x for x in f if x['scan_id'] == r.scan_id and x['hrel'] is not None), key=lambda x: -x['n_chars'])
            for x in rs[:1]:
                print(f"      hrel {x['hrel']:.2f} chars {x['n_chars']}: {x['text_head'][:120]}")
