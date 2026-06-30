# Coordinate-space bridging

A scan has its own native pixel coordinates (from its PageXML `Coords`); a thumbnail of that
scan has its own, smaller pixel coordinates; a canvas rendering a cropped/scaled selection of
the thumbnail has yet another. {class}`archival_structures.model.image.Transform` is the affine
mapping between any two of these spaces; {class}`~archival_structures.model.image.ImageSelection`
and {class}`~archival_structures.model.image.ImageCanvasSelection` compute the right `Transform`
for a given scan+thumbnail(+canvas) combination so call sites never re-derive the scaling math
themselves. See the [region-linking](notebooks/pagexml-image-region-linking) demo notebook for
both directions: drawing PageXML regions on a thumbnail, and converting a manually-drawn
thumbnail selection back into scan-space coordinates to attach as a new PageXML region.

{mod}`archival_structures.image.pagexml_bridge` converts between PageXML `Coords` objects and
this package's own `Box` type. {mod}`archival_structures.image.image_base` builds
`ImageSelection`s and loads thumbnails. {mod}`archival_structures.image.image_drawing` and
{mod}`archival_structures.image.image_annotation` are ipywidgets-based interactive
tools for drawing/tagging directly on thumbnails in a notebook (see the
[scan annotation](notebooks/annotate-scans) demo notebook, which builds the ground-truth
annotation data described in [Datasets and annotation](datasets_and_annotation.md)).
{mod}`archival_structures.image.image_processing` handles region extraction/clamping and
thumbnail-file generation/layout. {mod}`archival_structures.image.crop_borders` is a standalone
CLI script for cropping black/white scanner borders from document images.

```{eval-rst}
.. automodule:: archival_structures.model.image
   :members:

.. automodule:: archival_structures.image.image_base
   :members:

.. automodule:: archival_structures.image.pagexml_bridge
   :members:

.. automodule:: archival_structures.image.image_drawing
   :members:

.. automodule:: archival_structures.image.image_annotation
   :members:

.. automodule:: archival_structures.image.image_processing
   :members:

.. automodule:: archival_structures.image.crop_borders
   :members:

.. automodule:: archival_structures.utils.image_utils
   :members:
```
