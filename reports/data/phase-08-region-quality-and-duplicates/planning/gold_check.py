import gzip, math, re, sys
from types import SimpleNamespace
import pandas as pd
L='/Users/Shared/claude_projects/data_scopes/language-of-the-archive'
sys.path.insert(0, L)
import archival_language.corpus.quality as q
lex = frozenset(l.strip() for l in gzip.open(f'{L}/output/lexicon/fasttext-dim_384-window_10-min_count_100-case_lower-vocab.txt.gz','rt') if l.strip())
print('lexicon', len(lex))
def run(stem, gold):
    ref = pd.read_csv(f'{L}/output/quality/1.01.02-{stem}.tsv', sep='\t', keep_default_na=False, quoting=3, usecols=['row_no','text','predicted_class','score','rules_fired'])
    g = pd.read_csv(f'{L}/gold/{gold}', sep='\t', keep_default_na=False, quoting=3, usecols=['row_no','text'])
    assert (g.text.values == ref.text.values).all()
    bad = 0
    for t, pc, sc, rf in zip(g.text, ref.predicted_class, ref.score, ref.rules_fired):
        v = q.classify_features(q.region_features(SimpleNamespace(text=t, bbox=None), lexicon=lex), region_id='x')
        if (v.quality_class, '|'.join(v.rules)) != (pc, rf) or abs(v.score - float(sc)) > 1e-9: bad += 1
    print(stem, 'mismatches', bad, 'of', len(g))
run('gold-scored-after', '02-region-quality-done.tsv'); run('gold-scored-supplement-after', '02c-short-region-quality-done.tsv')
q.THRESHOLDS['digit_vs_punct_fraction_for_numeric_exempt'] = math.inf
run('gold-scored-before', '02-region-quality-done.tsv'); run('gold-scored-supplement-before', '02c-short-region-quality-done.tsv')
# lexicon subset size for fixture
toks = set()
for gold in ['02-region-quality-done.tsv','02c-short-region-quality-done.tsv']:
    g = pd.read_csv(f'{L}/gold/{gold}', sep='\t', keep_default_na=False, quoting=3, usecols=['text'])
    for t in g.text:
        for tok in t.split():
            c = q._clean_token_for_lookup(tok)
            if c: toks.add(c)
print('gold cleaned tokens', len(toks), 'in lexicon', len(toks & lex))
