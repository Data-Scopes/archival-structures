"""Ground truth format for archival document annotations.

The format is organised as a set of named *layers*, each containing a list of
annotation *elements*.  Every element carries an explicit list of *spans* — the
physical locations on scans that make up that element.

A span is the pair ``(scan_id, location)``, where location is either a bounding
box ``[x, y, w, h]`` in original-scan pixel coordinates or a ``page_side``
string (``"verso"``, ``"recto"``, or ``"full"``).  Bounding boxes are preferred
whenever pixel coordinates are available; ``page_side`` is used when only the
page-half is known (e.g. when derived from a page-range without PageXML access).

Scan dimensions (needed to map bounding boxes onto thumbnails) are stored in a
top-level ``scans`` mapping keyed by ``scan_id``; it is omitted when empty.

One YAML file is written per inventory number so that working with a subset of
inventories does not require parsing a large archive-wide file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Internal YAML helper — renders Span dicts in flow style so each span takes
# exactly one line rather than two, keeping large span lists readable.
# ---------------------------------------------------------------------------

class _FlowDict(dict):
    """dict subclass that the custom YAML dumper serialises in flow style."""


class _GroundTruthDumper(yaml.Dumper):
    pass


def _flow_dict_representer(dumper: yaml.Dumper, data: _FlowDict) -> yaml.MappingNode:
    return dumper.represent_mapping(
        'tag:yaml.org,2002:map', data.items(), flow_style=True
    )


_GroundTruthDumper.add_representer(_FlowDict, _flow_dict_representer)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Span:
    """A single physical location on a scan.

    Exactly one of ``bbox`` or ``page_side`` should be set.

    Parameters
    ----------
    scan_id:
        Filename of the scan (including extension), e.g.
        ``"NL-HaNA_1.01.02_3771_0023.jpg"``.
    bbox:
        Bounding box ``[x, y, w, h]`` in original scan pixel coordinates.
        Use when precise coordinates are available (e.g. from PageXML).
    page_side:
        ``"verso"`` | ``"recto"`` | ``"full"``.  Use when only the page
        half is known and pixel coordinates are not yet available.
    """

    scan_id: str
    bbox: list[int] | None = None
    page_side: str | None = None

    def as_dict(self) -> _FlowDict:
        d: _FlowDict = _FlowDict(scan_id=self.scan_id)
        if self.bbox is not None:
            d['bbox'] = self.bbox
        elif self.page_side is not None:
            d['page_side'] = self.page_side
        return d


@dataclass
class AnnotationElement:
    """A single annotated unit within a layer.

    Parameters
    ----------
    id:
        Stable identifier unique within the inventory, e.g. ``"sec_001"``.
    type:
        Layer-specific type label, e.g. ``"index_page"``, ``"session"``.
    spans:
        Ordered list of all physical locations that make up this element.
        For sub-page elements (resolutions, attendance lists) this is the
        annotator-assigned reading order.  For whole-scan or whole-page
        elements (sections, title pages) it is the sequential scan order.
    metadata:
        Any additional key/value pairs specific to the element type, e.g.
        ``date`` for sessions or ``page_num`` for title pages.
    """

    id: str
    type: str
    spans: list[Span]
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            'id': self.id,
            'type': self.type,
        }
        d.update(self.metadata)
        d['spans'] = [s.as_dict() for s in self.spans]
        return d


@dataclass
class InventoryGroundTruth:
    """Ground truth annotations for a single archival inventory number.

    Parameters
    ----------
    inventory_id:
        Combined identifier, e.g. ``"NL-HaNA_1.01.02_3771"``.
    ead_ref:
        Pointer to the EAD finding aid.  Expected keys:

        - ``unitid`` — archive ID (EAD component unitid), e.g.
          ``"NL-HaNA_1.01.02"``.
        - ``file_unitid`` — inventory number within the archive, e.g.
          ``"3771"``.

    document_type:
        Coarse document type, e.g. ``"resolutions_book"``.
    scans:
        Optional mapping from ``scan_id`` to ``{"width": int, "height": int}``
        in original scan pixels.  Used to map bounding boxes onto thumbnails.
        Omitted from the serialised file when empty.
    layers:
        Named annotation layers.  Common layer names for resolution books:
        ``"section"``, ``"title_page"``, ``"session"``, ``"attendance_list"``,
        ``"resolution"``.  Only layers that have at least one annotation need
        to be present.
    """

    inventory_id: str
    ead_ref: dict[str, str]
    document_type: str
    scans: dict[str, dict[str, int]] = field(default_factory=dict)
    layers: dict[str, list[AnnotationElement]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            'inventory_id': self.inventory_id,
            'ead_ref': self.ead_ref,
            'document_type': self.document_type,
        }
        if self.scans:
            d['scans'] = self.scans
        d['layers'] = {
            layer: [e.as_dict() for e in elements]
            for layer, elements in self.layers.items()
            if elements
        }
        return d


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def write_ground_truth(gt: InventoryGroundTruth, path: Path) -> None:
    """Serialise *gt* to a YAML file at *path*, creating parent dirs as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        yaml.dump(
            gt.as_dict(),
            fh,
            Dumper=_GroundTruthDumper,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )


def read_ground_truth(path: Path) -> InventoryGroundTruth:
    """Deserialise a ground truth YAML file written by :func:`write_ground_truth`."""
    path = Path(path)
    with open(path, 'r', encoding='utf-8') as fh:
        data = yaml.safe_load(fh)

    scans: dict[str, dict[str, int]] = data.get('scans', {})

    layers: dict[str, list[AnnotationElement]] = {}
    for layer_name, raw_elements in data.get('layers', {}).items():
        elements = []
        for raw in raw_elements:
            spans = [
                Span(
                    scan_id=s['scan_id'],
                    bbox=s.get('bbox'),
                    page_side=s.get('page_side'),
                )
                for s in raw.get('spans', [])
            ]
            metadata = {
                k: v for k, v in raw.items() if k not in ('id', 'type', 'spans')
            }
            elements.append(AnnotationElement(
                id=raw['id'],
                type=raw['type'],
                spans=spans,
                metadata=metadata,
            ))
        layers[layer_name] = elements

    return InventoryGroundTruth(
        inventory_id=data['inventory_id'],
        ead_ref=data['ead_ref'],
        document_type=data['document_type'],
        scans=scans,
        layers=layers,
    )
