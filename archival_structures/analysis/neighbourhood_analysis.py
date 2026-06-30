"""Reading-order adjacency between PageXML text lines.

`get_neighbouring_line_pairs` groups lines into horizontal rows and pairs each line with its
immediate left/right/above/below neighbour (or `None` at a row/column edge), giving the
correct top-to-bottom, column-aware reading order that raw region order doesn't guarantee --
used by `archival_structures.analysis.sequence_patterns` to build a corpus-wide line sequence.
"""

from collections import defaultdict
from typing import List, Tuple, Union

import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import horizontally_group_lines


class LineNeighbourHood:
    """Index of each line's nearest neighbour in each of the four directions
    (`above`/`below`/`left`/`right`), built from `get_neighbouring_line_pairs`."""

    def __init__(self, lines: List[pdm.PageXMLTextLine] = None, max_vertical_dist: int = None):
        self.lines_pairs = []
        self.has_rel_neighbour = defaultdict(lambda: defaultdict(list))
        self.rel_neighbours = {
            'above': [],
            'below': [],
            'left': [],
            'right': []
        }
        self.have_rel = {}
        if lines is not None:
            self.add_lines(lines, max_vertical_dist=max_vertical_dist)

    def add_lines(self, lines: List[pdm.PageXMLTextLine], max_vertical_dist: int = None):
        """Index `lines`' neighbour relations into this `LineNeighbourHood` (in addition to
        any lines already added)."""
        line_pairs = get_neighbouring_line_pairs(lines, max_vertical_dist=max_vertical_dist)
        self.lines_pairs.extend(line_pairs)
        for l1, l2, rel in line_pairs:
            self.has_rel_neighbour[l1][rel].append(l2)
            self.rel_neighbours[rel].append((l1, l2))
            if l1 is not None and l2 is not None:
                self.have_rel[(l1, l2)] = rel
                num_pairs = len(self.have_rel)
                # print(f"{num_pairs: >4}\t{'None' if l1 is None else l1.id: >6} - "
                #       f"{'None' if l2 is None else l2.id: >6} - {rel}")

    def get_rel_neighbour(self, line: pdm.PageXMLTextLine, rel: str):
        """`line`'s neighbours in direction `rel` (one of `'above'`/`'below'`/`'left'`/
        `'right'`), or `None` if `line` has no neighbour in that direction."""
        neighbours = [n for n in self.has_rel_neighbour[line][rel] if n is not None]
        return neighbours if neighbours else None

    def left(self, line: pdm.PageXMLTextLine):
        """`line`'s left neighbour(s), if any."""
        return self.get_rel_neighbour(line, 'left')

    def right(self, line: pdm.PageXMLTextLine):
        """`line`'s right neighbour(s), if any."""
        return self.get_rel_neighbour(line, 'right')

    def top(self, line: pdm.PageXMLTextLine):
        """`line`'s neighbour(s) above, if any."""
        return self.get_rel_neighbour(line, 'above')

    def bottom(self, line: pdm.PageXMLTextLine):
        """`line`'s neighbour(s) below, if any."""
        return self.get_rel_neighbour(line, 'below')


def line_vertical_gap(line1: pdm.PageXMLTextLine, line2: pdm.PageXMLTextLine) -> int:
    """The vertical gap between `line1` and `line2` (0 if they vertically overlap), using each
    line's baseline extent if available, else its coordinates' bounding box.

    `pagexml.model.physical_document_model.vertical_distance` mismeasures this for
    baseline-bearing lines -- it takes a `hasattr(doc, 'baseline')` branch that's true for
    essentially every `PageXMLTextLine` (the attribute exists, as `None`, even without real
    baseline data) and returns `abs(bottom1 - bottom2)` (a baseline-to-baseline offset) instead
    of an actual gap. This function always computes the real top/bottom gap instead."""
    if line1.baseline is not None and line1.baseline.points is not None:
        top1, bottom1 = line1.baseline.top, line1.baseline.bottom
    else:
        top1, bottom1 = line1.coords.top, line1.coords.bottom
    if line2.baseline is not None and line2.baseline.points is not None:
        top2, bottom2 = line2.baseline.top, line2.baseline.bottom
    else:
        top2, bottom2 = line2.coords.top, line2.coords.bottom
    if bottom1 < top2:
        return top2 - bottom1
    elif top1 > bottom2:
        return top1 - bottom2
    else:
        return 0


def get_neighbouring_line_pairs(lines: List[pdm.PageXMLTextLine], max_vertical_dist: int = None,
                                debug: int = 0) -> List[Tuple[Union[None, pdm.PageXMLTextLine],
                                                              Union[None, pdm.PageXMLTextLine], str]]:
    """For each line in `lines`, find its immediate left/right neighbour within its horizontal
    row (`horizontally_group_lines`) and its immediate above/below neighbour in the adjacent
    row. Returns one `(line, neighbour, direction)` triple per line per direction, with
    `neighbour=None` at a row/column edge or when `max_vertical_dist` is exceeded."""
    grouped_lines = horizontally_group_lines(lines)
    if debug > 0:
        print(f'number of lines: {len(lines)}')
        print(f'number of grouped_lines: {len(grouped_lines)}')
    if len(grouped_lines) == 0:
        return []
    line_pairs: List[Tuple[Union[None, pdm.PageXMLTextLine], Union[None, pdm.PageXMLTextLine], str]] = []
    line_pairs.extend([(line, None, 'above') for line in grouped_lines[0]])

    for ci, curr_group in enumerate(grouped_lines):
        adjacent_lines = [None] + curr_group + [None]
        if debug > 0:
            print("\n\n")
            print(f"adjacent_lines: {adjacent_lines}")
            print(f"adjacent_lines[1:-1]: {adjacent_lines[1:-1]}")
        for li, line in enumerate(adjacent_lines):
            if line is None:
                continue
            line_pairs.append((line, adjacent_lines[li-1], 'left'))
            line_pairs.append((line, adjacent_lines[li+1], 'right'))
            right = adjacent_lines[li+1]
            if debug > 0:
                print(f"\tright ({li}): {line.id} #{right if right is None else right.id}#")
        if ci >= len(grouped_lines) - 1:
            break
        next_group = grouped_lines[ci+1]
        for li, curr_line in enumerate(curr_group):
            for next_line in next_group:
                if not next_line.is_below(curr_line, direct_only=True):
                    continue
                vdist = line_vertical_gap(curr_line, next_line)
                if max_vertical_dist is not None and vdist > max_vertical_dist:
                    line_pairs.append((curr_line, None, 'below'))
                    line_pairs.append((next_line, None, 'above'))
                else:
                    line_pairs.append((curr_line, next_line, 'below'))
                    line_pairs.append((next_line, curr_line, 'above'))
    line_pairs.extend([(line, None, 'below') for line in grouped_lines[-1]])
    if debug > 0:
        print(f'number of line_pairs: {len(line_pairs)}')
    return line_pairs
