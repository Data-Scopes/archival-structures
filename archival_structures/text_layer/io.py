"""I/O for the shared text layer: deterministic TSV (gzip) and JSON writers, readers,
input discovery for PageXML zips/directories, and reading the lines sidecar per region.

Ported verbatim from NLP's ``startpage/voc_text_layer.py`` (except the inventory sort key,
which uses ``InventoryNumber``). No pagexml or pandas import.
"""

import gzip
import io
import json
import os
import re
import tarfile
import zipfile
from collections import defaultdict
from pathlib import Path

from archival_structures.model.inventory_number import (
    InvalidInventoryNumberError, InventoryNumber)

SANITISE_PATTERN = re.compile(r"[\t\r\n]")


def inventory_sort_key(inv: str):
    """Natural order of inventory numbers via InventoryNumber ('9626' < '9626A' < '9627').
    Names that are not inventory numbers sort last, by string."""
    try:
        return (0, InventoryNumber(inv).key)
    except InvalidInventoryNumberError:
        return (1, inv)


def sanitise(text, counter):
    """Replace each tab/CR/LF with one space, counting replacements per kind."""
    if text is None:
        return ""
    if not SANITISE_PATTERN.search(text):
        return text
    for char, name in (("\t", "tab"), ("\r", "cr"), ("\n", "lf")):
        n = text.count(char)
        if n:
            counter[name] += n
    return SANITISE_PATTERN.sub(" ", text)


def format_box(coords):
    """`x-y-w-h` from a pagexml Coords, as in region_paragraphs.tsv.gz."""
    if coords is None:
        return ""
    box = coords.box
    return f"{box['x']}-{box['y']}-{box['w']}-{box['h']}"


def _assert_clean(fields):
    for value in fields:
        assert "\t" not in value and "\n" not in value and "\r" not in value, repr(value)


def write_tsv_gz(path, header, rows):
    """Deterministic gzip TSV (mtime=0, no embedded name), written atomically."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    n = 0
    with open(tmp, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="\n") as handle:
                handle.write("\t".join(header) + "\n")
                for row in rows:
                    fields = [str(v) for v in row]
                    _assert_clean(fields)
                    handle.write("\t".join(fields) + "\n")
                    n += 1
    os.replace(tmp, path)
    return n


def read_tsv_gz(path):
    """Yield dict rows. Splits on tab/newline only (QUOTE_NONE, no NaN coercion)."""
    with gzip.open(path, "rt", encoding="utf-8", newline="\n") as handle:
        header = handle.readline().rstrip("\n").split("\t")
        for line in handle:
            values = line.rstrip("\n").split("\t")
            assert len(values) == len(header), (path, line[:200])
            yield dict(zip(header, values))


def write_text_atomic(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    os.replace(tmp, path)


def write_json_atomic(path, obj):
    write_text_atomic(path, json.dumps(obj, ensure_ascii=False, indent=1, sort_keys=True) + "\n")


def write_plain_tsv(path, header, rows):
    """Small uncompressed TSV, QUOTE_NONE (no quoting or escaping at all)."""
    out = ["\t".join(header)]
    for row in rows:
        fields = [str(v) for v in row]
        _assert_clean(fields)
        out.append("\t".join(fields))
    write_text_atomic(path, "\n".join(out) + "\n")


# --------------------------------------------------------------------------
# Input discovery
# --------------------------------------------------------------------------

def discover_inventories(pagexml_dir):
    """{inv: Path} for every `<inv>.zip` or `<inv>/` under pagexml_dir.

    When both exist (as locally), the zip wins: one sequential read instead
    of ~1,000 small files.
    """
    pagexml_dir = Path(pagexml_dir)
    entries = [e for e in pagexml_dir.iterdir() if not e.name.startswith(".")]
    found = {e.name: e for e in entries if e.is_dir()}
    found.update({e.stem: e for e in entries if e.is_file() and e.suffix == ".zip"})
    return dict(sorted(found.items(), key=lambda kv: inventory_sort_key(kv[0])))


def _is_pagexml_member(name):
    base = name.rsplit("/", 1)[-1]
    return name.endswith(".xml") and not base.startswith(".") and "__MACOSX" not in name


def _is_tar(source):
    return source.name.endswith((".tar.gz", ".tgz"))


def _is_resource_fork(name):
    return "__MACOSX/" in name or name.rsplit("/", 1)[-1].startswith("._")


def input_bytes(source):
    """(stored_bytes, xml_bytes): bytes on disk and uncompressed PageXML bytes.

    Cheap for a zip: reads its central directory only, never parses. A .tar.gz has
    to be decompressed to be sized.
    """
    source = Path(source)
    if source.suffix == ".zip":
        with zipfile.ZipFile(source) as zf:
            xml = sum(i.file_size for i in zf.infolist() if _is_pagexml_member(i.filename))
        return source.stat().st_size, xml
    if _is_tar(source):
        with tarfile.open(source, "r:gz") as tf:
            xml = sum(m.size for m in tf if m.isfile() and _is_pagexml_member(m.name))
        return source.stat().st_size, xml
    xml = sum(p.stat().st_size for p in source.rglob("*.xml") if _is_pagexml_member(str(p)))
    return xml, xml


def iter_pagexml(source, *, skip_resource_forks: bool = False):
    """Yield (member_name, bytes) for each PageXML file, sorted by file name.

    Sources: a .zip, a .tar.gz/.tgz, or a directory searched recursively. The default
    filter (`_is_pagexml_member`) already ignores dot-files and `__MACOSX`; with
    `skip_resource_forks=True` AppleDouble `._*` members and `__MACOSX/` paths are in
    addition excluded explicitly (a no-op today, kept as a guard for v2 presets).
    """
    source = Path(source)

    def wanted(name):
        return _is_pagexml_member(name) and not (skip_resource_forks and _is_resource_fork(name))

    if source.suffix == ".zip":
        with zipfile.ZipFile(source) as zf:
            names = sorted((n for n in zf.namelist() if wanted(n)),
                           key=lambda n: n.rsplit("/", 1)[-1])
            for name in names:
                yield name, zf.read(name)
    elif _is_tar(source):
        with tarfile.open(source, "r:gz") as tf:
            members = sorted((m for m in tf if m.isfile() and wanted(m.name)),
                             key=lambda m: m.name.rsplit("/", 1)[-1])
            for member in members:
                yield member.name, tf.extractfile(member).read()
    else:
        for path in sorted(source.rglob("*.xml"), key=lambda p: p.name):
            if wanted(str(path)):
                yield str(path), path.read_bytes()


def discover_inventories_v2(pagexml_dir, pattern):
    """{inv: Path} for every entry (file or directory) of `pagexml_dir` whose name
    `fullmatch`es `pattern`, a regex with a named group `inv`. Sorted like v1; dot-files
    are ignored. If an inventory has several entries, a file (archive) wins over a dir."""
    regex = re.compile(pattern)
    found = {}
    for entry in sorted(Path(pagexml_dir).iterdir()):
        if entry.name.startswith("."):
            continue
        match = regex.fullmatch(entry.name)
        if not match:
            continue
        inv = match.group("inv")
        if inv not in found or entry.is_file():
            found[inv] = entry
    return dict(sorted(found.items(), key=lambda kv: inventory_sort_key(kv[0])))


def read_region_lines(layer_root, inv):
    """[(scan_id, region_id, [line_text, ...]), ...] in sidecar order, zero-line
    regions included (from the skeleton)."""
    lines_by_region = defaultdict(list)
    for row in read_tsv_gz(Path(layer_root) / "lines" / f"{inv}.lines.tsv.gz"):
        lines_by_region[(row["scan_id"], row["region_id"])].append(row["line_text"])
    regions = []
    for row in read_tsv_gz(Path(layer_root) / "regions_meta" / f"{inv}.tsv.gz"):
        key = (row["scan_id"], row["region_id"])
        texts = lines_by_region.get(key, [])
        assert len(texts) == int(row["n_lines"]), (inv, key)
        regions.append((row, texts))
    return regions
