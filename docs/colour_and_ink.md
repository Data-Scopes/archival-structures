# Ink colour, multi-colour text, and missing transcriptions

{mod}`archival_structures.clustering.colour_clustering` is the most developed module in this
package, covering three related problems:

- **Robust ink/paper separation**: {func}`~archival_structures.clustering.colour_clustering.find_ink_luminosity_class`
  fits multiotsu thresholds on a page's text-line luminosity, then picks whichever resulting
  class fragments into many small connected components -- the signature of genuine ink (one
  blob per glyph), unlike paper or a uniform artefact (sticker, glare, stain), which form one or
  a few large ones regardless of how bright or dark they are. This replaced an earlier
  Otsu-threshold + mean-luminosity-based approach that turned out to be fragile to small bright
  artefacts on an otherwise dark page -- see [Findings](findings.md).
- **Multi-colour-text screening**: {func}`~archival_structures.clustering.colour_clustering.score_multi_colour_text`
  ranks pages by how likely they are to contain more than one ink colour (e.g. a different
  writer or stage of an administrative process), using the standard deviation of LAB chroma
  among ink pixels -- not the number of multiotsu luminosity classes, which turned out not to
  discriminate (see [Findings](findings.md)). The
  [multi-colour explorer](notebooks/pagexml-image-multicolour-explorer) demo notebook screens a
  sample of scans and lets you inspect the top candidates.
- **Missing-transcription detection**: {func}`~archival_structures.clustering.colour_clustering.find_missing_text_candidates`
  flags a PageXML scan's untranscribed `empty_regions` whose pixels look like genuine ink rather
  than blank paper, comparing each candidate against the page's own background ink-fraction
  baseline. Also demonstrated in the
  [multi-colour explorer](notebooks/pagexml-image-multicolour-explorer) notebook.

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
.. automodule:: archival_structures.clustering.colour_clustering
   :members:

.. automodule:: archival_structures.clustering.colour_quantisation
   :members:

.. automodule:: archival_structures.clustering.image_clustering
   :members:

.. automodule:: archival_structures.analysis.stream_analysis
   :members:
```
