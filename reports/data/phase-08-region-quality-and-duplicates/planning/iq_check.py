import sys, gzip, random, time
import pandas as pd
L='/Users/Shared/claude_projects/data_scopes/language-of-the-archive'
sys.path.insert(0, L)
from archival_language.config import load_config
from archival_language.corpus.tsv_source import TSVParagraphSource
from archival_language.corpus.quality import inventory_quality
from archival_structures.model.inventory_number import InventoryNumber
lex = frozenset(l.strip() for l in gzip.open(f'{L}/output/lexicon/fasttext-dim_384-window_10-min_count_100-case_lower-vocab.txt.gz','rt') if l.strip())
cfg = load_config(); src = TSVParagraphSource('1.01.02', config=cfg)
ref = pd.read_csv(f'{L}/output/quality/1.01.02-inventory-quality.tsv', sep='\t', dtype={'inventory_number': str})
print('ref', ref.shape, list(ref.columns)[:12])
invs = sorted(src.inventories(), key=str); rng = random.Random(int(sys.argv[1])); pick = rng.sample(invs, int(sys.argv[2]))
t=time.time(); new = inventory_quality(src, lexicon=lex, inventories=pick)
new['inventory_number']=new['inventory_number'].astype(str)
cols = ['n_regions','n_usable','n_fragment','n_noise','n_tokens_all','n_tokens_usable','n_tokens_usable_fragment','n_chars_usable']
m = new.merge(ref, on='inventory_number', suffixes=('_n','_r'))
bad = [(r.inventory_number, c) for _, r in m.iterrows() for c in cols if r[c+'_n'] != r[c+'_r']]
print('picked', [str(p) for p in pick], 'merged', len(m), 'mismatches', bad[:10], f'{time.time()-t:.0f}s', 'regions', int(new.n_regions.sum()))
