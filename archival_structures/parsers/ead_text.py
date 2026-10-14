"""Text extraction from EAD elements."""

from __future__ import annotations

import re
from typing import List

#: Stands in for `<lb/>` during the walk; XML cannot contain NUL, so it never clashes with text.
_LB_MARK = "\x00"
#: A line break directly before one of these is dropped rather than turned into a space, so
#: `Eems<lb/>. Met` is `Eems. Met` and not `Eems . Met`. The quotes are the ASCII `'` and `"`
#: and the closing typographic `\u2019` and `\u201d`; an ASCII quote that opens a quotation after
#: a line break therefore loses its space (`a<lb/>"b"` is `a"b"`).
_NO_SPACE_BEFORE = re.compile(_LB_MARK + r"(?=[\x01\x02]*[.,;:!?)\]'\"\u2019\u201d])")
#: Private hook for `ead_fields.split_unittitle_dates`: bracket a marked child's text so its
#: offsets survive the walk. XML 1.0 cannot contain U+0001/U+0002, so they never clash with text,
#: and they live only in Python strings (lxml rejects control characters in the tree).
_SPAN_START = "\x01"
_SPAN_END = "\x02"


#: The only element that is a line break in running text. Everything else is "inline" markup.
_LINE_BREAK = "lb"


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


def _walk(element, parts: List[str], marked=frozenset()) -> None:
    if element.text:
        parts.append(element.text)
    for child in element:
        if isinstance(child.tag, str):
            if _local(child.tag) == _LINE_BREAK:
                parts.append(_LB_MARK)
            elif child in marked:
                parts.append(_SPAN_START)
                _walk(child, parts, marked)
                parts.append(_SPAN_END)
            else:
                _walk(child, parts, marked)
        # comments and processing instructions contribute nothing, only their tail
        if child.tail:
            parts.append(child.tail)


def raw_element_text(element) -> str:
    """`element_text` before the final whitespace collapse: the `<lb/>` rule applied, nothing
    else changed. Exposed for callers that need character offsets into the text
    (`" ".join(raw_element_text(e).split()) == element_text(e)`)."""
    parts: List[str] = []
    _walk(element, parts)
    return _NO_SPACE_BEFORE.sub("", "".join(parts)).replace(_LB_MARK, " ")


def _raw_marked_text(element, marked) -> str:
    """`raw_element_text` with `_SPAN_START`/`_SPAN_END` around each child in `marked`."""
    parts: List[str] = []
    _walk(element, parts, frozenset(marked))
    return _NO_SPACE_BEFORE.sub("", "".join(parts)).replace(_LB_MARK, " ")


def element_text(element) -> str:
    """The text content of `element`, whitespace-normalised (runs of whitespace become one
    space, ends stripped). Returns `""` for an empty element.

    Rule: a single walk over the tree in which

    * `<lb/>` (a line break) contributes one space, so `eerste<lb/>deel` is `eerste deel`,
      except directly before `. , ; : ! ? ) ]` or a quote (`'`, `"`, `\u2019`, `\u201d`), where it
      contributes nothing (`Eems<lb/>. Met` is `Eems. Met`, `1674<lb/>'` is `1674'`);
    * comments and processing instructions contribute only their tail text;
    * every other element (`<emph>`, `<unitdate>`, `<extref>`, ...) is descended into *raw*:
      its text and tail are used exactly as written and no space is added at its edges.
      Markup that splits a word without whitespace (`Ge<emph>heim</emph>`) therefore stays
      one word, and whitespace that is written at a child's edge (`derden<unitdate> September
      </unitdate>`) is kept, so it is neither lost nor doubled;
    * whitespace is collapsed once, at the end, so a `<lb/>` next to existing whitespace does
      not give a double space.

    Not collapsing per child (as a "collapse each child and join" recipe does) is what keeps
    child-edge whitespace intact.
    """
    return " ".join(raw_element_text(element).split())
