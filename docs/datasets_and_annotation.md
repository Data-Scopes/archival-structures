# Datasets and ground-truth annotation

{mod}`archival_structures.datasets.annotations` defines the ground-truth label schema for the
text-analysis tasks: {class}`~archival_structures.datasets.annotations.OpeningLabel` (task 1),
a free-text page-layout label (task 2), per-line type labels (task 3), and
{class}`~archival_structures.datasets.annotations.Element`/
{class}`~archival_structures.datasets.annotations.ElementSpan` for cross-page document elements
(task 4). Labels are plain JSON, one file per scan (or per inventory number for elements),
referencing PageXML ids rather than re-encoding coordinates, so they stay valid however the
underlying scan/thumbnail files are organised on disk.

The [scan annotation](notebooks/annotate-scans) demo notebook is an ipywidgets-based app
(built on {mod}`archival_structures.image.image_drawing`) for producing this ground truth
interactively: step through an inventory number's scans, label each one's opening/page-layout/
line-type properties, and save.

Two other tools have produced labels for the same scans in different, incompatible formats.
{func}`~archival_structures.datasets.annotations.import_bulk_image_labels` and
{func}`~archival_structures.datasets.annotations.migrate_legacy_region_annotations` bridge
them into this module's `ScanAnnotation` ground truth -- see
[Findings](findings.md#reconciling-annotation-formats) for what each one does and why.

{mod}`archival_structures.datasets.images_transcriptions` holds this package's own test-fixture
data (paths into `data/`, plus hand-picked image/PageXML pairs with known properties used as
small real-data regression checks across the test suite).

```{eval-rst}
.. automodule:: archival_structures.datasets.annotations
   :members:

.. automodule:: archival_structures.datasets.images_transcriptions
   :members:
```
