# Datasets and ground-truth annotation

{mod}`archival_structures.datasets.annotations` defines the ground-truth label schema for the
text-analysis tasks: {class}`~archival_structures.datasets.annotations.OpeningLabel` (task 1,
unchanged -- a precise, numeric split coordinate rather than a tag), and three levels of tagged
content -- `ScanAnnotation.tags` (whole scan), `.pages` (verso/recto of a two-page opening),
`.lines`/`.regions` (task 3, individual lines and zones) -- using the tag vocabulary described
in [Vocabulary](vocabulary.md). {class}`~archival_structures.datasets.annotations.Element`/
{class}`~archival_structures.datasets.annotations.ElementSpan` cover cross-page document
elements (task 4). Labels are plain JSON, one file per scan (or per inventory number for
elements), referencing PageXML ids rather than re-encoding coordinates where possible, so they
stay valid however the underlying scan/thumbnail files are organised on disk.

The [scan annotation](notebooks/annotate-scans) demo notebook is an ipywidgets-based app
(built on {mod}`archival_structures.image.image_drawing`) for producing this ground truth
interactively. **Note:** the notebook currently still targets the pre-vocabulary schema
(a single free-text `page_layout` string and one label per line) and needs updating to the
tag-based fields described above before it's usable again.

Two other tools have produced labels for the same scans in different, incompatible formats.
{func}`~archival_structures.datasets.annotations.import_bulk_image_labels` and
{func}`~archival_structures.datasets.annotations.migrate_legacy_region_annotations` bridge
them into this module's `ScanAnnotation` ground truth -- see
[Findings](findings.md#reconciling-annotation-formats) for what each one does and why.

{mod}`archival_structures.datasets.bulk_tagging` is a third way to produce `ScanAnnotation.tags`:
a paginated, checkbox-selectable grid (like the bulk image tagger above, but writing structured
tags straight into `ScanAnnotation` rather than free text into an intermediate format) for
tagging many scans at once by cluster -- see the
[bulk-tag annotation](notebooks/bulk-tag-annotation-demo) demo notebook. It has no dependency on
any particular clustering pipeline, just a plain list of image paths (e.g. one cluster's members
from {func}`~archival_structures.analysis.page_layout_clustering.cluster_page_layouts`, as the
demo notebook uses, or from an external pipeline).

{mod}`archival_structures.datasets.images_transcriptions` holds this package's own test-fixture
data (paths into `data/`, plus hand-picked image/PageXML pairs with known properties used as
small real-data regression checks across the test suite).

```{eval-rst}
.. automodule:: archival_structures.datasets.annotations
   :members:

.. automodule:: archival_structures.datasets.bulk_tagging
   :members:

.. automodule:: archival_structures.datasets.images_transcriptions
   :members:
```
