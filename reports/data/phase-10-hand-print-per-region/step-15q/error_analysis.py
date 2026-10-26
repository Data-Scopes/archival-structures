"""15q error analysis helper: for chosen volumes print the histogram of scan values (0.05 bins) and
N scans (seed 16) nearest the volume median with region texts. Usage: error_analysis.py INV[,INV..] [N] [--near]"""
import sys, random, gzip, csv, json
import numpy as np
sys.path.insert(0, sys.argv[0].rsplit('/',1)[0])
import validate as V
invs = sys.argv[1].split(','); N = int(sys.argv[2]) if len(sys.argv) > 2 else 5
arch = '1.10.94' if invs[0] in ('455','456') else '1.01.02'
rnd = random.Random(16)
for inv in invs:
    arch = '1.10.94' if inv in ('455', '456') else '1.01.02'
    feats = V.load_regions(arch, inv)
    sc = [s for s in V.scan_values(feats) if s['labelled']]
    vals = np.array([s['value'] for s in sc])
    print(f"=== {inv}: {len(sc)} labelled scans, median value {np.median(vals):.3f}")
    h, e = np.histogram(vals, bins=np.arange(0.8, 2.6, 0.05))
    print(' '.join(f"{e[i]:.2f}:{h[i]}" for i in range(len(h)) if h[i]))
    med = np.median(vals)
    near = sorted(sc, key=lambda s: abs(s['value'] - med))[:25]
    pick = rnd.sample(near, min(N, len(near)))
    for s in pick:
        print(f"-- {s['scan_id']} value {s['value']:.3f} chars {s['n_chars']}")
        rs = [f for f in feats if f['scan_id'] == s['scan_id'] and f['hrel'] is not None]
        rs.sort(key=lambda f: -f['n_chars'])
        for f in rs[:3]:
            print(f"   hrel {f['hrel']:.2f} n_lines {f['n_lines']} chars {f['n_chars']}: {f['text_head'][:150]}")
