import sys
from pathlib import Path
from archival_structures.annotation.ground_truth import read_ground_truth
from archival_structures.stream_analysis.corruption import *
gt, layer, tag = sys.argv[1:4]
canon = extract_canonical_stream(read_ground_truth(Path(gt)), layer=layer)
cs = read_corrupted_stream(Path(sys.argv[4])/tag/sys.argv[5]/f'page_duplication-rate0.3-seed0.yaml')
starts = true_start_indices(canon)
lab = observed_start_labels(cs.observed, cs.provenance, canon)
dups=[k for k,p in enumerate(cs.provenance) if p.events]
st=[k for k in dups if cs.provenance[k].canonical_index in starts][:5]
mid=[k for k in dups if k not in st][: 10-len(st)]
for k in st+mid:
    ci=cs.provenance[k].canonical_index
    print(k, ci, canon[ci].document_id, 'is_start' if ci in starts else 'mid', 'label',lab[k],'prev label',lab[k-1],'prev canon',cs.provenance[k-1].canonical_index, 'old', ci in starts)
