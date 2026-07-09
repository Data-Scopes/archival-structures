# Ink colour, multi-colour text, and missing transcriptions

The multiotsu + connected-component-shape technique for separating ink from paper lives in
{mod}`archival_structures.clustering.ink_luminosity`:
{func}`~archival_structures.clustering.ink_luminosity.find_ink_luminosity_class` fits multiotsu
thresholds on a `pixel_mask`'s luminosity, then picks whichever resulting class fragments into
many small connected components -- the signature of genuine ink (one blob per glyph), unlike
paper or a uniform artefact (sticker, glare, stain), which form one or a few large ones
regardless of how bright or dark they are. This replaced an earlier Otsu-threshold +
mean-luminosity-based approach that turned out to be fragile to small bright artefacts on an
otherwise dark page -- see [Findings](findings.md).
{func}`~archival_structures.clustering.ink_luminosity.find_ink_like_classes` extends this to
return *every* ink-like class rather than a single best one, for pages with genuinely
multi-coloured text.

Nothing in `pixel_mask` is specific to PageXML -- it is just a boolean array. This module is used
two ways:

- **From PageXML** (`pixel_mask` = a text-line mask), in
  {mod}`archival_structures.clustering.colour_clustering`, which re-exports the functions above
  and builds on them for three related problems:
  - **Ink-pixel extraction**: {func}`~archival_structures.clustering.colour_clustering.extract_ink_pixels`
    unions every ink-like luminosity class within a text-line mask into one mask, feeding the
    ink-colour signature in {func}`~archival_structures.clustering.colour_clustering.process_image_pair`.
  - **Multi-colour-text screening**: {func}`~archival_structures.clustering.colour_clustering.score_multi_colour_text`
    ranks pages by how likely they are to contain more than one ink colour (e.g. a different
    writer or stage of an administrative process), using the standard deviation of LAB chroma
    among ink pixels -- not the number of multiotsu luminosity classes, which turned out not to
    discriminate on its own (see [Findings](findings.md)). The
    [multi-colour explorer](notebooks/page-analysis/pagexml-image-multicolour-explorer) demo
    notebook screens a sample of scans and lets you inspect the top candidates.
  - **Missing-transcription detection**: {func}`~archival_structures.clustering.colour_clustering.find_missing_text_candidates`
    flags a PageXML scan's untranscribed `empty_regions` whose pixels look like genuine ink
    rather than blank paper (against *any* ink-like class fit on the page's confirmed text
    lines, not just the single best one), comparing each candidate against the page's own
    background ink-fraction baseline. Also demonstrated in the
    [multi-colour explorer](notebooks/page-analysis/pagexml-image-multicolour-explorer) notebook.
- **From the whole image, no PageXML** (`pixel_mask` = every pixel), in
  {func}`~archival_structures.stream_analysis.overview.layout_analysis.extract_layout_features`
  (see [Stream analysis](stream_analysis.md)): `ink_density`/`text_density` and the other
  content-density features are computed from the union of every ink-like class found on the
  whole thumbnail, and `n_ink_luminosity_classes`/`ink_chroma_spread` surface the same
  multi-colour-text signal `score_multi_colour_text` does, without needing a transcription at
  all. The density threshold that decides "ink-like" needed a lower default for a whole-image
  mask than for a PageXML text-line mask (0.02 vs. 0.15) -- validated against
  `NL-AsdSAA_89_3.1` (heterogeneous document types and layouts, often multiple ink colours on a
  single scan) and `NL-HaNA_2.10.50_1` (stable ink colour but variable paper tone, including
  scans with a lighter sheet glued on top); see that function's module docstring for the
  numbers. The [multi-colour explorer](notebooks/page-analysis/pagexml-image-multicolour-explorer)
  notebook's pure-image section compares this against the PageXML-anchored screening on the
  same sample of scans.

## Related/superseded modules

{mod}`archival_structures.clustering.colour_quantisation` (whole-image k-means colour
quantisation) and the peak-location analysis in
{mod}`archival_structures.image.image_processing` predate the multiotsu-based approach above and
are considered a dead end for ink/text-colour classification specifically, though
{func}`~archival_structures.clustering.colour_quantisation.cluster_main_image_colours` is still
used for general whole-image colour clustering.
{mod}`archival_structures.clustering.image_clustering` clusters whole thumbnails by dominant
colour (a coarser, page-level alternative). {mod}`archival_structures.analysis.stream_analysis`
does whole-page luminosity categorisation (background/inverted/quarantined flags) for a stream
of scans -- a quality-control screen, not ink-colour classification.

```{eval-rst}
.. automodule:: archival_structures.clustering.ink_luminosity
   :members:

.. automodule:: archival_structures.clustering.colour_clustering
   :members:

.. automodule:: archival_structures.clustering.colour_quantisation
   :members:

.. automodule:: archival_structures.clustering.image_clustering
   :members:

.. automodule:: archival_structures.analysis.stream_analysis
   :members:
```
