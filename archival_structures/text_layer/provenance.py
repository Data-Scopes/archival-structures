"""Package version and git-state helpers for the text-layer provenance (step 15)."""

import re
from pathlib import Path

MIN_PAGEXML_TOOLS = (0, 9, 1)


def package_info(dist_name, module_name=None):
    """Version and path of an installed package plus, for a git checkout, its git state."""
    import importlib
    import importlib.metadata
    import subprocess
    module_name = module_name or dist_name.replace("-", "_")
    module = importlib.import_module(module_name)
    package_dir = Path(module.__file__).parent
    info = {"version": importlib.metadata.version(dist_name), "path": str(package_dir)}
    repo = package_dir.resolve().parent
    if (repo / ".git").exists():
        try:
            head = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                  capture_output=True, text=True, timeout=20).stdout.strip()
            dirty = subprocess.run(["git", "-C", str(repo), "status", "--porcelain", "--",
                                    package_dir.name],
                                   capture_output=True, text=True, timeout=20).stdout
            info["git_head"] = head
            info["git_dirty_files"] = [line[3:] for line in dirty.splitlines() if line.strip()]
        except (OSError, subprocess.SubprocessError):
            pass
    return info


def pagexml_tools_info():
    return package_info("pagexml-tools", "pagexml")


def check_min_version(dist_name, minimum):
    """Return the installed version string; assert it is at least ``minimum`` (a tuple)."""
    from importlib.metadata import version
    raw = version(dist_name)
    parts = tuple(int(p) for p in re.findall(r"\d+", raw)[:3])
    assert parts >= tuple(minimum), f"{dist_name} {raw} < required {tuple(minimum)}"
    return raw


def check_pagexml_tools_version():
    return check_min_version("pagexml-tools", MIN_PAGEXML_TOOLS)
