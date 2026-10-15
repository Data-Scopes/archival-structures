import pandas as pd, random
from collections import Counter
V1="/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1/wbc/"
per=pd.read_csv(V1+"wbc_per_inventory.tsv",sep="\t",quoting=3,dtype=str,keep_default_na=False,na_values=[],usecols=["inventory_number","n_lines","mode","wbc_set","differs_from_default","char_status"])
print(per.wbc_set.value_counts().head(10))
print(per["mode"].value_counts())
dev=per[per.differs_from_default=="True"]
print("deviating",len(dev), dev["mode"].value_counts().to_dict())
cat=Counter(); adds=Counter()
for s in dev.wbc_set:
    S=set(s); extra=S-{"¬","„"}
    drops=[c for c in "¬„" if c not in S]
    if extra and not drops: cat["adds"]+=1; adds.update(extra)
    elif drops and not extra: cat["drops "+"".join(drops)]+=1
    elif drops and extra: cat["drops+adds"]+=1
    else: cat["other"]+=1
print(cat); print(adds.most_common())
st=pd.read_csv(V1+"wbc_stats.tsv",sep="\t",quoting=3,dtype={"inventory_number":str,"char":str},keep_default_na=False,na_values=[],usecols=["inventory_number","char","n_lines","n_end_attached","join_vocab_frac","end_rel_frac"])
e=st[(st.n_end_attached>=30)&(st.n_lines>=2000)]
print("rows eligible",len(e))
j=e[(e.join_vocab_frac>=0.225)&(e.join_vocab_frac<=0.275)]
r=e[(e.end_rel_frac>=4.5)&(e.end_rel_frac<=5.5)]
print("near join",len(j),"near rel",len(r),"either",len(e[e.index.isin(j.index)|e.index.isin(r.index)]))
# flipping only matters when the other criterion passes
jp=j[j.end_rel_frac>=5]; rp=r[r.join_vocab_frac>=0.25]
print("near join & rel passes",len(jp),"near rel & join passes",len(rp))
print(jp.char.value_counts().head(8).to_dict(), rp.char.value_counts().head(8).to_dict())
rng=random.Random(14)
invs=sorted(dev.inventory_number); samp=rng.sample(invs,10)
print(samp)
for inv in samp:
    print("==",inv,per[per.inventory_number==inv][["n_lines","mode","wbc_set"]].values.tolist())
    g=st[(st.inventory_number==inv)&(st.n_end_attached>=30)]
    print(g.to_string(index=False))
