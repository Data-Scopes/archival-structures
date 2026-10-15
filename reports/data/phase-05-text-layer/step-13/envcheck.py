import importlib.metadata as m, sys
import pagexml
print("py", sys.executable)
for p in ("pagexml-tools","fuzzy-search","lxml","nltk","py7zr","pytest"):
    try: print(p, m.version(p))
    except Exception as e: print(p, "ERR")
print("file", pagexml.__file__, getattr(pagexml,"__version__",None))
try:
    import pagexml.helper.pagexml_helper; print("helper OK")
except Exception as e: print("helper FAIL", repr(e)[:150])
try:
    from pagexml.analysis.text_stats import LineCharAnalyser; print("LCA OK")
except Exception as e: print("LCA FAIL", repr(e)[:150])
