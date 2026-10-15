import gzip
V1="/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1/lines/"
want={"1068":"¬","8575":"=","9840":"=","9183":"=","7724":"„","2914":"¬","3132":"„","3008":"„","3248":"=","10502":"="}
for inv,c in want.items():
    prev=None;found=0
    with gzip.open(V1+inv+".lines.tsv.gz","rt",encoding="utf-8") as h:
        h.readline()
        for line in h:
            p=line.rstrip("\n").split("\t")
            if prev and prev[1]==p[1] and prev[4].endswith(c) and len(prev[4])>3 and prev[4][-2].isalpha():
                found+=1
                if found==5: print(inv,c,repr(prev[4]),"|",repr(p[4])); break
            prev=p
