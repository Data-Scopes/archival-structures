"""Re-check the two 'Known pagexml-tools limits' on the installed version.
Run: .venv/bin/python -I reports/data/phase-14-index-links/step-20a/known_limits_check.py"""
import importlib.metadata as m
from pagexml.parser import parse_pagexml_file
import pagexml.model.physical_document_model as pdm
print("pagexml-tools", m.version("pagexml-tools"))
f = "tests/fixtures/text_layer/pagexml/9999A/NL-HaNA_1.04.02_9999A_0003.xml"
page = parse_pagexml_file(f)
def walk(rs, d=0):
    for r in rs:
        yield r, d
        yield from walk(getattr(r, "text_regions", []) or [], d + 1)
print("9999A_0003: top-level regions", len(page.text_regions), "total incl. nested",
      sum(1 for _ in walk(page.text_regions)))
for r, d in walk(page.text_regions):
    print("  ", "  " * d, r.id, "lines", len(r.lines), "coords", r.coords is not None)
print("page.stats", page.stats)
import tempfile, os
src = open(f).read()
bare = src.replace('<TextRegion id="n1" custom="structure {type:paragraph;}">', '<TextRegion id="bare0"></TextRegion>\n    <TextRegion id="n1" custom="structure {type:paragraph;}">', 1)
with tempfile.TemporaryDirectory() as d:
    p = os.path.join(d, "bare.xml"); open(p, "w").write(bare)
    try:
        pg = parse_pagexml_file(p)
        print("region without Coords/lines: parsed, region ids", [r.id for r, _ in walk(pg.text_regions)])
    except Exception as e:
        print("region without Coords/lines: parse error", type(e).__name__, e)
