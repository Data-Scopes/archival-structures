"""Review analysis (read-only): start-score distribution at gold starts, held-out vs long-chunk."""
import sys
from pathlib import Path
import numpy as np, torch
torch.set_num_threads(8)
NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
sys.path[:0] = [str(NLP / "src"), str(NLP / "scripts")]
import train_semi_markov_duration as m
step31 = m.step31
from startpage.splits import load_or_build_split
from startpage.crf_sequence import START

fs = step31.build_full_page_sequences(); states, _, _ = step31.build_page_states(); fdf = step31.load_scalar_features()

def summarise(name, model, seqs, log_pmf):
    sc, gold, nsm, nviterbi = [], [], 0, 0
    for s in seqs:
        f = torch.tensor(s["features"]); x = m.start_score_from_emissions(model, f)
        lab = np.asarray(s["labels"]) == START; mask = np.asarray(s["mask"], bool)
        sm = np.array(m.decode_semi_markov(x, log_pmf, m.MAX_DURATION), bool)
        vit = np.array(model.viterbi(f)) == START
        sc.append(x[mask]); gold.append(lab[mask]); nsm += sm[mask].sum(); nviterbi += vit[mask].sum()
    sc, gold = np.concatenate(sc), np.concatenate(gold)
    print(f"{name}: pages {len(sc)} gold starts {gold.sum()} ({gold.mean():.3f}) | viterbi starts {nviterbi} | semi-Markov starts {nsm}")
    print(f"   start score at gold starts: median {np.median(sc[gold]):+.2f}, share>0 {np.mean(sc[gold]>0):.3f}")
    print(f"   start score at non-starts:  median {np.median(sc[~gold]):+.2f}, share>0 {np.mean(sc[~gold]>0):.3f}")
    print(f"   overall share of pages with score>0: {np.mean(sc>0):.3f}; mean score {sc.mean():+.2f}")
    print(f"   log_pmf d=1..5: {np.round(log_pmf[:5],2)}; d=50 {log_pmf[49]:.2f}; d=250 {log_pmf[249]:.2f}")

# long_chunk seed 0
split_df = load_or_build_split("long_chunk"); rs = step31.load_region_mil_scores()
tr = split_df.loc[(split_df["split"]=="train")&(split_df["role"]=="core"),"page_id"]
ss = step31.fit_scaler_train_only(fdf, set(tr))
lp = m.log_duration_pmf(**{k: v for k, v in m.fit_lognormal_duration(m.fit_duration_long_chunk(fs, states, split_df)).items()}, max_duration=m.MAX_DURATION)
lk = rs[rs["seed"]==0].set_index("page_id")["score_logit"].to_dict(); imp = step31.compute_train_median_logit(lk, tr.tolist())
trs = step31.long_chunk_sequences("train", split_df, lk, ss, states, imp); dvs = step31.long_chunk_sequences("dev", split_df, lk, ss, states, imp)
model, _ = step31.train_crf(trs, dvs, 2 + ss.shape[1], 0)
summarise("long_chunk dev seed 0", model, dvs, lp)

# held-out seed 42
iho = load_or_build_split("inventory_held_out", seed=0); knn = step31.load_knn_heldout_scores()
tri = sorted(set(iho.loc[iho["split"]=="train","inventory_number"])); tei = sorted(set(iho.loc[iho["split"]=="test","inventory_number"]))
trp = set(iho.loc[iho["split"]=="train","page_id"]); ssi = step31.fit_scaler_train_only(fdf, trp)
lpi = m.log_duration_pmf(**m.fit_lognormal_duration(m.fit_duration_inventory_held_out(fs, states, tri)), max_duration=m.MAX_DURATION)
lk = knn[knn["seed"]==42].set_index("page_id")["score_logit"].to_dict(); imp = step31.compute_train_median_logit(lk, trp)
trs = step31.inventory_held_out_sequences(tri, fs, lk, ssi, states, imp); tes = step31.inventory_held_out_sequences(tei, fs, lk, ssi, states, imp)
rng = np.random.RandomState(42); sh = rng.permutation(len(trs)); nv = max(1, len(trs)//5)
model, _ = step31.train_crf([trs[i] for i in sh[nv:]], [trs[i] for i in sh[:nv]], 2 + ssi.shape[1], 42)
summarise("held-out test seed 42", model, tes, lpi)
summarise("held-out TRAIN (fit+val) seed 42", model, trs, lpi)
