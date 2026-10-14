"""Text extraction from EAD elements."""

from __future__ import annotations

import re
from typing import List

#: Stands in for `<lb/>` during the walk; XML cannot contain NUL, so it never clashes with text.
_LB_MARK = "\x00"
#: A line break directly before one of these is dropped rather than turned into a space, so
#: `Eems<lb/>. Met` is `Eems. Met` and not `Eems . Met`.
_NO_SPACE_BEFORE = re.compile(_LB_MARK + r"(?=[.,;:!?)\]])")

#: The only element that is a line break in running text. Everything else is "inline" markup.
_LINE_BREAK = "lb"


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


def _walk(element, parts: List[str]) -> None:
    if element.text:
        parts.append(element.text)
    for child in element:
        if isinstance(child.tag, str):
            if _local(child.tag) == _LINE_BREAK:
                parts.append(_LB_MARK)
            else:
                _walk(child, parts)
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


def element_text(element) -> str:
    """The text content of `element`, whitespace-normalised (runs of whitespace become one
    space, ends stripped). Returns `""` for an empty element.

    Rule: a single walk over the tree in which

    * `<lb/>` (a line break) contributes one space, so `eerste<lb/>deel` is `eerste deel`,
      except directly before `. , ; : ! ? ) ]`, where it contributes nothing (`Eems<lb/>. Met`
      is `Eems. Met`). Quotes are ambiguous (opening or closing) and do get the space;
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
