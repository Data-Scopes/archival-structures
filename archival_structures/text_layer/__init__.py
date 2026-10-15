"""Shared text layer: a versioned, per-inventory layer of PageXML lines, regions and
word-break decisions, used by the sibling projects.

Modules:

- ``io``: deterministic TSV/JSON readers and writers, input discovery, region-line reading;
- ``word_breaks``: per-inventory detection of word-break characters (C14);
- ``extract``, ``join``, ``provenance``, ``presets``, ``build``, ``compare``: added by
  steps 15 and 15c.

Nothing is imported here, so importing the package pulls in no pagexml or pandas.
"""
