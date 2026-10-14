"""Packaging tests: the light core (no torch/vision/notebook/llm extras) must import cleanly.

The core checks run in a subprocess in which the heavy optional modules are blocked
(a meta-path finder makes `import name` raise ImportError; planting `sys.modules[name] = None`
instead breaks libraries such as sklearn that probe `sys.modules` for torch), so they hold in any
environment, including one that has the extras installed.
"""

import subprocess
import sys
import textwrap

import pytest

BLOCKED = [
    "torch", "transformers", "hdbscan", "umap", "cv2", "ipywidgets", "ipyevents",
    "ipycanvas", "IPython", "anthropic", "skimage", "PIL", "matplotlib", "seaborn",
]

PRELUDE = f"""
import sys

class _Blocker:
    blocked = frozenset({BLOCKED!r})

    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in self.blocked:
            raise ImportError(f"{{name}} is blocked in this test")

sys.meta_path.insert(0, _Blocker())
"""


def run_blocked(body: str) -> subprocess.CompletedProcess:
    code = PRELUDE + textwrap.dedent(body)
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)


def test_core_modules_import_without_extras():
    result = run_blocked("""
        import importlib, pkgutil
        import archival_structures.parsers as parsers
        import archival_structures.model as model

        names = ["archival_structures.annotation.ground_truth",
                 "archival_structures.stream_analysis.corruption"]
        for pkg in (parsers, model):
            names += [m.name for m in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + ".")]
        assert len(names) > 8, names
        for name in names:
            importlib.import_module(name)
        print("imported", len(names))
    """)
    assert result.returncode == 0, result.stderr


def test_stream_analysis_package_is_lazy():
    result = run_blocked("""
        import archival_structures.stream_analysis as sa
        assert len(sa.__all__) > 0
        assert "run_overview" in dir(sa)
        # corruption names resolve with the heavy modules blocked
        sa.simulate_collection, sa.CorruptedStream, sa.run_length_encode
        loaded = [n for n in ("torch", "transformers", "hdbscan", "umap", "cv2")
                  if n in sys.modules]
        assert not loaded, loaded
    """)
    assert result.returncode == 0, result.stderr


def test_stream_analysis_unknown_attribute_raises():
    import archival_structures.stream_analysis as sa

    with pytest.raises(AttributeError):
        sa.does_not_exist


def test_all_exported_names_resolve():
    """Needs the full environment: some names live in submodules that import the extras."""
    pytest.importorskip("torch")
    pytest.importorskip("hdbscan")
    pytest.importorskip("ipywidgets")
    import archival_structures.stream_analysis as sa

    for name in sa.__all__:
        assert getattr(sa, name) is not None, name
        ns = {}
        exec(f"from archival_structures.stream_analysis import {name}", ns)
        assert ns[name] is getattr(sa, name)
