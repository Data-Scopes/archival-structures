import sys
from pathlib import Path
from archival_structures.stream_analysis.corruption import *
from archival_structures.stream_analysis.corruption.metrics import *
SP=Path(sys.argv[1])
def run(gt, layer, tag):
    kw = {} if layer=='section' else {'layer':layer}
    res = simulate_inventory(Path(gt), SP/'out17b'/tag, cases=('page_duplication',), rates=(0.1,0.3,0.5), seeds=(0,1,2), **kw)
    from archival_structures.annotation.ground_truth import read_ground_truth
    canon = extract_canonical_stream(read_ground_truth(Path(gt)), layer=layer)
    starts = true_start_indices(canon)
    ctp = true_same_document_pairs(canon)
    print('canon pair share', round(sum(ctp)/len(ctp),4))
    print(tag, len(canon), len({u.document_id for u in canon}))
    print('rate seed n_copies frac copies_of_starts F1old wd_old changed pairs_true_share(canon)')
    for r in res:
        cs = read_corrupted_stream(r.output_path)
        obs, prov = cs.observed, cs.provenance
        true = observed_start_labels(obs, prov, canon)
        old = [p.canonical_index in starts for p in prov]
        cs_ = sum(1 for p in prov if p.events and p.canonical_index in starts)
        f1 = score_start_classification(true, old).f1
        wd = window_diff(true, old)
        pairs = true_same_document_pairs(obs); psh=sum(pairs)/len(pairs)
        print(r.rate, r.seed, r.n_touched, round(r.n_touched/len(canon),3), cs_, round(f1,3), round(wd,4), sum(old)-sum(true), round(psh,4))
run(*sys.argv[2:5])
