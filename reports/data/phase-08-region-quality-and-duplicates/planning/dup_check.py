import sys, time
import pandas as pd
L='/Users/Shared/claude_projects/data_scopes/language-of-the-archive'
sys.path.insert(0, L)
from archival_language.config import load_config
from archival_language.corpus.tsv_source import TSVParagraphSource
from archival_language.ead.adapters import load_adapter
from archival_language.corpus.integrity import duplicate_candidates
from archival_structures.model.inventory_number import InventoryNumber
cfg = load_config(); src = TSVParagraphSource('1.01.02', config=cfg); ad = load_adapter(cfg.archives['1.01.02'])
ref = pd.read_csv(f'{L}/output/integrity/1.01.02-duplicate-candidates.tsv', sep='\t', dtype={'inventory_number': str})
print('ref', ref.shape, 'invs with candidates', ref.inventory_number.nunique())
for inv in sys.argv[1:]:
    t=time.time(); df = duplicate_candidates(src, InventoryNumber(inv), adapter=ad)
    r = ref[ref.inventory_number == inv].reset_index(drop=True)
    same = len(df)==len(r) and (df.page_id_a.values==r.page_id_a.values).all() and (df.page_id_b.values==r.page_id_b.values).all() and (abs(df.jaccard_exact.values-r.jaccard_exact.values)<1e-12).all() and (df.jaccard_estimated.values==r.jaccard_estimated.values).all() and (df.adjacent.values==r.adjacent.values).all()
    print(inv, 'new', len(df), 'ref', len(r), 'same', same, 'excl', df.attrs['n_excluded_near_empty'], 'cmp', df.attrs['n_pages_compared'], f'{time.time()-t:.1f}s', df.dtypes.scan_number_a)
