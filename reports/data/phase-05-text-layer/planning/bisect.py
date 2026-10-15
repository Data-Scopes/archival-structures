import re, sys, traceback
from pagexml.parser import parse_pagexml_file
x = open(sys.argv[1]).read()
def tryit(label, s):
    try:
        scan = parse_pagexml_file("f.xml", pagexml_data=s)
        regs = []
        def walk(rs, d=0):
            for r in rs:
                regs.append((d, r.id, r.coords is not None, len(r.lines or []), [l.text for l in (r.lines or [])]))
                walk(r.text_regions or [], d + 1)
        walk(scan.text_regions or [])
        print(label, "OK", scan.id, regs)
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)[-1]
        print(label, "FAIL", type(e).__name__, e, tb.filename.split("site-packages/")[-1], tb.lineno)
r3 = re.search(r'\s*<TextRegion id="r3">.*?</TextRegion>', x, re.S).group(0)
nested = re.search(r'\s*<TextRegion id="r1a".*?</TextRegion>', x, re.S).group(0)
tryit("full", x)
tryit("no r3", x.replace(r3, ""))
tryit("no nested", x.replace(nested, ""))
tryit("no r3 no nested", x.replace(r3, "").replace(nested, ""))
r3c = r3.replace('<TextRegion id="r3">', '<TextRegion id="r3"><Coords points="10,400 990,400 990,500 10,500"/>')
tryit("r3 with coords", x.replace(r3, r3c))
