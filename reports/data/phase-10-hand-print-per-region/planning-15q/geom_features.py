import pandas as pd, numpy as np, glob, sys, os
B='data/text_layer_v2_dev/15m-a/'
layers={'hand':B+'hand/NL-HaNA_1.01.02_text_layer_v3/','print':B+'print/NL-HaNA_1.10.94_text_layer_v3/'}
def bl(s):
    try:
        p=np.array([[float(v) for v in q.split(',')] for q in s.split()])
        return p
    except Exception: return None
rows=[]
for side,L in layers.items():
    for f in sorted(glob.glob(L+'lines/*.lines.tsv.gz')):
        inv=os.path.basename(f).split('.')[0]
        d=pd.read_csv(f,sep='\t',usecols=['scan_id','region_id','line_idx','x','y','w','h','baseline','conf','text_raw','split_from'],dtype={'text_raw':str})
        d=d[d.text_raw.fillna('').str.len()>0]
        d['n']=d.text_raw.str.len()
        sw=pd.read_csv(L+f'scans/{inv}.scans.tsv.gz',sep='\t',usecols=['scan_id','page_w'])
        pw=sw.page_w.median()
        for rid,g in d.groupby('region_id',sort=False):
            if len(g)<3: continue
            g=g.sort_values('line_idx')
            ys=[];slopes=[];res=[]
            for b in g.baseline:
                p=bl(b) if isinstance(b,str) else None
                if p is None or len(p)<2: ys.append(np.nan);slopes.append(np.nan);res.append(np.nan);continue
                ys.append(p[:,1].mean())
                dx=p[:,0].max()-p[:,0].min()
                if dx<=0 or len(p)<3: slopes.append(np.nan);res.append(np.nan);continue
                c=np.polyfit(p[:,0],p[:,1],1);slopes.append(c[0]);res.append(np.sqrt(np.mean((np.polyval(c,p[:,0])-p[:,1])**2)))
            ys=np.array(ys);pitch=np.diff(np.sort(ys[~np.isnan(ys)]))
            pitch=pitch[pitch>0]
            if len(pitch)<2: continue
            P=np.median(pitch)
            right=(g.x+g.w).values[:-1];left=g.x.values
            mw=np.median(g.w)
            rows.append(dict(side=side,inv=inv,scan=g.scan_id.iloc[0],pw=pw,n=g.n.sum(),nl=len(g),pitch=P,pitch_rel=P/pw*1000,
              cpe=np.median(g.n/(g.w/P)),  # chars per pitch-length
              hrel=np.median(g.h)/P,
              right_sd=np.std(right)/mw, left_sd=np.std(left)/mw,
              pitch_cv=np.std(pitch)/P, res=np.nanmedian(res)/P, slope_sd=np.nanstd(slopes),
              conf=g.conf.median()))
r=pd.DataFrame(rows)
r.to_csv(sys.argv[1],sep='\t',index=False)
feats=['pitch_rel','cpe','hrel','right_sd','left_sd','pitch_cv','res','slope_sd','conf']
pd.set_option('display.width',250)
print(r.groupby(['side','inv'])[feats].median().round(3))
print(r.groupby(['side','inv']).size())
