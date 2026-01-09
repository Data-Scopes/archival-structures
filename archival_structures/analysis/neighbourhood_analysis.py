from collections import defaultdict
from typing import List, Tuple, Union

import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import horizontally_group_lines


class LineNeighbourHood:

    def __init__(self, lines: List[pdm.PageXMLTextLine] = None, max_vertical_dist: int = None):
        self.above = defaultdict(list)
        self.below = defaultdict(list)
        self.left = defaultdict(list)
        self.right = defaultdict(list)
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
        if line in self.has_rel_neighbour[line][rel]:
            return self.has_rel_neighbour[line][rel]

    def left(self, line: pdm.PageXMLTextLine):
        return self.get_rel_neighbour(line, 'left')

    def right(self, line: pdm.PageXMLTextLine):
        return self.get_rel_neighbour(line, 'right')

    def top(self, line: pdm.PageXMLTextLine):
        return self.get_rel_neighbour(line, 'top')

    def bottom(self, line: pdm.PageXMLTextLine):
        return self.get_rel_neighbour(line, 'bottom')


def get_neighbouring_line_pairs(lines: List[pdm.PageXMLTextLine], max_vertical_dist: int = None,
                                debug: int = 0):
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
                vdist = pdm.vertical_distance(curr_line, next_line)
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
