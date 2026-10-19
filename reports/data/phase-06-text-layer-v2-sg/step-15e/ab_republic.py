"""Step 15e, criterion 1: A/B of the AS port against republic-project's original splitter.

Usage: .venv/bin/python reports/data/phase-06-text-layer-v2-sg/step-15e/ab_republic.py \
           455|456 [out.json] [--limit N]

For every resolution page of REPUBLIC page JSON (LotA's raw_pages-<inv>.jsonl.gz): build one
shared scorer from the pages' own lines, then run the original and the port with the notebook
parameters on deep copies (hyphenated -> non-hyphenated, in try/except -> columns) and compare,
per page and per column, in order: (line.id, line.text, coords.points, baseline.points); and
the set of pages that raise. Read only; republic-project is put on sys.path, nothing is written
there. `-I` is NOT usable here (sys.path is set explicitly); run without it.
"""
import contextlib
import copy
import gzip
import io
import json
import os
import sys
import time

R = "/Users/marijnkoolen/Code/Huygens/republic-project"
RAW = ("/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/pages/raw/"
       "raw_pages-{inv}.jsonl.gz")
sys.dont_write_bytecode = True
sys.path.insert(0, R)

from republic.helper.pagexml_helper import json_to_pagexml_page  # noqa: E402
import republic.parser.pagexml.long_line_splitter as orig  # noqa: E402
import archival_structures.text_layer.line_split as port  # noqa: E402


def _get_all_text_regions_07(self):
    """pagexml-tools 0.7.x behaviour of PageXMLPage.get_all_text_regions. In >= 0.8.0 the
    method looks at page.text_regions only (empty for a page with columns), so the unmodified
    original's page_split_columns raises IndexError on every page. The A/B patches the
    original's environment back to 0.7.x behaviour; the port has its own fix 1."""
    text_regions = [tr for col in self.columns for tr in col.text_regions]
    text_regions.extend([tr for tr in self.extra])
    return text_regions


if "--no-env-patch" not in sys.argv:
    import pagexml.model.physical_document_model as _pdm
    _pdm.PageXMLPage.get_all_text_regions = _get_all_text_regions_07

PARAMS = dict(min_merged_width=1100, default_line_width=900, text_left_boundary=450,
              text_right_boundary=2350, default_line_num_chars=45)


def iter_pages(inv, limit=0):
    n = 0
    with gzip.open(RAW.format(inv=inv), "rt") as fh:
        for line in fh:
            page = json_to_pagexml_page(json.loads(line))
            if not page.has_type("resolution_page"):
                continue
            yield page
            n += 1
            if limit and n >= limit:
                return


def build_scorer(inv, limit):
    splitter = port.LineSplitter(**PARAMS)
    runs = []
    for page in iter_pages(inv, limit):
        for col in page.columns:
            for tr in col.text_regions:
                runs.append(tr.lines)
    docs = port.training_docs_from_lines(
        runs, is_merged=lambda ln: bool(splitter.is_merged_line(ln)), get_text=lambda ln: ln.text)
    finder = port.train_bigram_finder(docs)
    return port.make_pmi_scorer(finder, min_uni_freq=100), len(docs), finder.N


def pipeline(mod, page, scorer):
    splitter = mod.LineSplitter(**PARAMS)
    err = None
    with contextlib.redirect_stdout(io.StringIO()):
        try:
            hy = mod.page_split_merged_hyphenated_lines(splitter, page)
            try:
                sp = mod.page_split_merged_non_hyphenated_lines(splitter, hy, scorer)
            except Exception as exc:  # the notebook falls back to the unsplit page
                err = f"{type(exc).__name__}: {str(exc)[:100]}"
                sp = hy
            out = mod.page_split_columns(splitter, sp)
        except Exception as exc:
            return None, f"FATAL {type(exc).__name__}: {str(exc)[:100]}", None
    sig = []
    for col in out.columns:
        sig.append([(ln.id, ln.text, [tuple(p) for p in ln.coords.points],
                     [tuple(p) for p in ln.baseline.points] if ln.baseline else None)
                    for ln in col.get_lines()])
    extra = [[ln.id for ln in tr.lines] for tr in out.extra]
    return (sig, extra), err, None


def main():
    inv = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else None
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 0
    t0 = time.time()
    scorer, n_docs, n_tokens = build_scorer(inv, limit)
    print(f"inv {inv}: scorer from {n_docs} docs, {n_tokens} tokens ({time.time()-t0:.0f}s)")
    n = same = 0
    raise_o = raise_p = fatal_o = fatal_p = 0
    n_lines_in = n_lines_added = pages_with_split = 0
    diffs = []
    for page in iter_pages(inv, limit):
        n += 1
        res_o, err_o, _ = pipeline(orig, copy.deepcopy(page), scorer)
        res_p, err_p, _ = pipeline(port, copy.deepcopy(page), scorer)
        raise_o += err_o is not None
        raise_p += err_p is not None
        # "identical" includes the raise set: both raise or both don't, with the same message
        ok = res_o == res_p and err_o == err_p
        same += ok
        if res_o is not None:
            n_in = len(page.get_lines())
            n_out = sum(len(c) for c in res_o[0]) + sum(len(e) for e in res_o[1])
            n_lines_in += n_in
            n_lines_added += n_out - n_in
            pages_with_split += n_out > n_in
        if not ok and len(diffs) < 5:
            diffs.append({"page": page.id, "err_orig": err_o, "err_port": err_p,
                          "first_diff": _first_diff(res_o, res_p)})
        if n % 100 == 0:
            print(f"  {n} pages, identical {same} ({time.time()-t0:.0f}s)", flush=True)
    summary = {"inv": inv, "pages_compared": n, "identical": same, "raising_original": raise_o,
               "raising_port": raise_p, "lines_in": n_lines_in, "lines_added_by_original": n_lines_added,
               "pages_with_split_original": pages_with_split, "first_diffs": diffs, "seconds": round(time.time() - t0)}
    print(json.dumps(summary, indent=1))
    if out_path:
        with open(out_path, "w") as fh:
            json.dump(summary, fh, indent=1)
        os.chmod(out_path, 0o664)


def _first_diff(a, b):
    if a is None or b is None:
        return f"orig={'None' if a is None else 'ok'} port={'None' if b is None else 'ok'}"
    if len(a[0]) != len(b[0]):
        return f"n columns {len(a[0])} vs {len(b[0])}"
    for ci, (ca, cb) in enumerate(zip(a[0], b[0])):
        if len(ca) != len(cb):
            return f"col {ci}: n lines {len(ca)} vs {len(cb)}"
        for li, (la, lb) in enumerate(zip(ca, cb)):
            if la != lb:
                return f"col {ci} line {li}: {la[:2]} vs {lb[:2]}"
    return "extra differs"


if __name__ == "__main__":
    main()
