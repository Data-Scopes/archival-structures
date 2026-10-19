"""Merged-line splitter for two-column printed pages, ported from republic-project.

Source: ``republic/parser/pagexml/long_line_splitter.py`` of republic-project
(/Users/marijnkoolen/Code/Huygens/republic-project), last commit ``47dfc010`` (2026-01-23).
Copyright (c) the republic-project authors (Marijn Koolen, Rik Hoekstra, Hayco de Jong).
Licence: MIT (per republic-project's ``pyproject.toml``); the MIT permission notice is the one
in AS's ``LICENSE``.

Ported verbatim; changes listed below. Every function body is unchanged.

Changes:

1. Imports. ``copy_page`` comes from ``pagexml.helper.pagexml_helper`` (republic only re-exports
   it). ``BigramCollocationFinder`` is no longer imported (it was only a type hint).
2. ``horizontal_group_lines`` is copied from ``republic/helper/pagexml_helper.py`` (L336-371).
3. The type hint ``ngram_scorer: BigramCollocationFinder`` became
   ``ngram_scorer: Callable[[str, str], float]``: the argument is used as a callable.
4. Fix 1 (crash found by the A/B): ``page_split_columns`` calls ``_page_text_regions(page)``
   instead of ``page.get_all_text_regions()``, which returns an empty set for a page with
   columns in pagexml-tools >= 0.8.0 (test ``test_page_split_columns_sides``).

The AS adapter (parameters, scorer, scan hook) is below the ``# --- AS adapter ---`` line.
"""

import copy
import re
from typing import Callable, List, Tuple, Union

import numpy as np
import pagexml.model.physical_document_model as pdm
from pagexml.analysis.layout_stats import sort_coords_above_below_baseline
from pagexml.analysis.layout_stats import compute_baseline_distances
from pagexml.helper.pagexml_helper import copy_page


def width_in_range(doc: pdm.PageXMLDoc, min_width: int = None, max_width: int = None) -> bool:
    """Check if a PageXMLDoc object has a width within a given range."""
    if min_width is not None and doc.coords.w < min_width:
        return False
    if max_width is not None and doc.coords.w > max_width:
        return False
    return True


def get_line_sequences_in_width_range(lines: List[pdm.PageXMLTextLine],
                                      min_width: int = None,
                                      max_width: int = None) -> List[List[pdm.PageXMLTextLine]]:
    """Return a list of lists of consecutive lines in a text region that have a
    width within a given width range"""
    sequences = []
    sequence = []
    for line in lines:
        if width_in_range(line, min_width=min_width, max_width=max_width) is False:
            if len(sequence) > 0:
                sequences.append(sequence)
            sequence = []
            continue
        sequence.append(line)
    if len(sequence) > 0:
        sequences.append(sequence)
    return sequences


class Word:

    def __init__(self, word: str, index: int, line: pdm.PageXMLTextLine, lg_index: int = None):
        self.word = word
        self.index = index
        self.line = line
        self.lg_index = lg_index

    def __repr__(self):
        return f"'{self.word}'"

    def __len__(self):
        return len(self.word)


def get_words(line: pdm.PageXMLTextLine):
    """Return the word tokens of a line."""
    if line.words is not None and len(line.words) > 0:
        words = [word.text for word in line.words]
    elif line.text is not None:
        words = line.text.split(' ')
    else:
        return None
    return [Word(word, wi, line) for wi, word in enumerate(words)]


def get_split_indexes(words: List[Word], min_offset: int, max_offset: int):
    offset = 0
    split_indexes = []
    for wi, word in enumerate(words):
        word_start, word_end = offset, offset + len(word)
        if min_offset <= word_start <= max_offset:
            split_indexes.append(wi)
        elif min_offset < word_end <= max_offset and word_end - min_offset > min_offset - word_start:
            # most of the word comes after the min offset, so add it too
            split_indexes.append(wi)
        offset = word_end + 1
    return split_indexes


class SplitWord:

    def __init__(self, word: Word, word_pos: str):
        self.word = word.word
        self.index = word.index
        self.line = word.line
        self.word_pos = word_pos

    def __len__(self):
        return len(self.word)

    def __repr__(self):
        attr_string = (f"word='{self.word}', index={self.index}, line_id='{self.line.id}', \n"
                       f"\tword_pos='{self.word_pos}'")
        return f"{self.__class__.__name__}({attr_string})"


class ColumnLines:

    def __init__(self, lines: pdm.PageXMLTextLine, left_first: SplitWord = None,
                 left_last: Union[SplitWord, List[SplitWord]] = None,
                 right_first: Union[SplitWord, List[SplitWord]] = None, right_last: SplitWord = None
                 ):
        self.lines = lines
        self.num_columns = 2
        self.left_first = left_first
        self.left_last = [left_last] if isinstance(left_last, SplitWord) else left_last
        self.right_first = right_first
        self.right_last = right_last


class LineInfo:

    def __init__(self, line_group: List[pdm.PageXMLTextLine],
                 column_sep_width: int, default_line_num_chars: int,
                 dist_to_prev: int, dist_to_next: int, default_line_width: int = None,
                 text_left_boundary: int = None, text_right_boundary: int = None,
                 words: List[Word] = None, split_indexes: List[int] = None):
        self.line_group = line_group
        self.min_left = line_group[0].coords.left
        self.max_right = line_group[-1].coords.right
        self.column_sep_width = column_sep_width
        self.default_line_num_chars = default_line_num_chars
        self.text_left_boundary = text_left_boundary
        self.text_right_boundary = text_right_boundary
        self.default_line_width = default_line_width
        self.width = self.max_right - self.min_left
        self.dist_to_prev = dist_to_prev
        self.dist_to_next = dist_to_next
        self.words = words
        self.split_indexes = split_indexes
        self.split_words = {}
        self.left_col_left = None
        self.left_col_right = None
        self.right_col_left = None
        self.right_col_right = None
        if self.text_left_boundary is not None and self.text_right_boundary is not None:
            self.left_col_left = self.text_left_boundary
            self.left_col_right = self.text_left_boundary + self.default_line_width
            self.right_col_left = self.left_col_right + column_sep_width
            self.right_col_right = self.text_right_boundary

    def update_boundaries(self, text_left_boundary: int, text_right_boundary: int):
        self.text_left_boundary = text_left_boundary
        self.text_right_boundary = text_right_boundary
        self.default_line_width = (self.text_right_boundary - self.text_left_boundary - self.column_sep_width) / 2
        self.left_col_left = self.text_left_boundary
        self.left_col_right = self.text_left_boundary + self.default_line_width
        self.right_col_left = self.left_col_right + self.column_sep_width
        self.right_col_right = self.text_right_boundary


def add_word_info(line_info: LineInfo):
    """Add information about words and word positions in each line and in the line group."""
    line_info.words = [word for line in line_info.line_group for word in get_words(line)]
    for wi, word in enumerate(line_info.words):
        word.lg_index = wi


class MergedLineContext:

    def __init__(self,
                 curr_line_info: LineInfo,
                 prev_line_info: LineInfo,
                 next_line_info: LineInfo,
                 column_sep_width: int,
                 default_line_num_chars: int,
                 default_line_width: int = None,
                 ):
        """A merged line with its surrounding lines (assuming a 2-column layout)
        and the left and right boundaries of the main text on the page. The left
        boundary is the left side of the left-most column, the right boundary is
        the right side of the right-most column.

        :param curr_line_info: the current merged line that is the focus for splitting
        :type: LineInfo
        :param prev_line_info: the previous line in the right-side column
        :type: LineInfo
        :param next_line_info: the next line in the left-side column
        :type: LineInfo
        :param column_sep_width: the width in pixels of the gap between the left
        and right columns
        :type column_sep_width: int
        :param default_line_width: the default width of a text line (if not given,
        it is derived from the text_left_boundary, text_right_boundary and
        column_sep_width
        :type default_line_width: int
        :param default_line_num_chars: the default number of characters of a text
        line.
        """
        # self.curr_group = {'lines': curr_line_group}
        # self.prev_group = {'lines': prev_line_group}
        # self.next_group = {'lines': next_line_group}
        self.curr_info = curr_line_info
        self.prev_info = prev_line_info
        self.next_info = next_line_info
        self.text_left_boundary = curr_line_info.text_left_boundary
        self.text_right_boundary = curr_line_info.text_right_boundary
        self.column_sep_width = column_sep_width
        self.default_line_num_chars = default_line_num_chars
        self.min_offset = default_line_num_chars - 15
        self.max_offset = default_line_num_chars + 15
        if default_line_width is None:
            self.default_line_width = (self.text_right_boundary - self.text_left_boundary - column_sep_width) / 2
        else:
            self.default_line_width = default_line_width
        # self.curr_info.split_indexes = get_split_indexes(self.curr_info.words, self.min_offset, self.max_offset)
        # print(f"long_line_splitter.MergedLineContext - prev_info:")
        # print(self.prev_info)
        # self.prev_info.split_indexes = []
        # self.next_info.split_indexes = []


def get_dist_to_other(line_group1: List[pdm.PageXMLTextLine], line_group2: List[pdm.PageXMLTextLine]) -> int:
    dist_list = compute_baseline_distances(line_group1, line_group2)
    return int(np.median(dist_list))


def extract_left_right_split_from_combination(line_info: LineInfo, combination):
    """Determine the x coordinate at which to split a line given a split combination. """

    left_last = combination['curr_left_last']
    merged_line = [line for line in line_info.line_group if line.id == left_last.line.id][0]
    right_first = combination['curr_right_first']

    left_words = [word for word in line_info.words[:left_last.index + 1] if word.line == merged_line]
    right_words = [word for word in line_info.words[right_first.index:] if word.line == merged_line]

    left_text = ' '.join([word.word for word in left_words])
    right_text = ' '.join([word.word for word in right_words])

    if len(left_text) == 0:
        merged_text = right_text
    elif len(right_text) == 0:
        merged_text = left_text
    else:
        merged_text = left_text + " " + right_text
    if merged_text != merged_line.text:
        print(f"long_line_splitter._extract_left_right_split_from_combination - splitting error:")
        print(f"line.text: {[line.text for line in line_info.line_group]}")
        print(f"merged_line.text: #{merged_line.text}#")
        print(f"\tleft_text: #{left_text}#")
        print(f"\tright_text: #{right_text}#")
        print(f"\tcombined_text: #{left_text + ' ' + right_text}#")
        print(len(left_text + " " + right_text), len(merged_line.text))
        for i in range(len(merged_text)):
            if merged_text[i] == merged_line.text[i]:
                continue
            print(f" discrepancy in index {i}, left+right: {merged_text[i]}\toriginal: {merged_line.text[i]}")
        raise ValueError(f"splits of merged_hyphenated_line do not combine to the original line")
    return left_text, right_text


def extract_left_right_splits_from_regex_match(line: pdm.PageXMLTextLine, match: re.Match, min_offset: int):
    """Determine the x coordinate at which to split a line given a hyphen/word-break
    match. """
    start, end = match.span()
    hyphen_offset = min_offset - 1 + start + len(match.group(1))
    left_text = line.text[:hyphen_offset + 1]
    right_text = line.text[hyphen_offset + 2:]
    if left_text + " " + right_text != line.text:
        print(f"long_line_splitter._extract_left_right_splits - splitting error:")
        print(f"line.text: {line.text}")
        print(f"\tleft_text: #{left_text}#")
        print(f"\tright_text: #{right_text}#")
        print(f"\tcombined_text: #{left_text + ' ' + right_text}#")
        raise ValueError(f"splits of merged_hyphenated_line do not combine to the original line")
    return left_text, right_text


def make_split_lines(line: pdm.PageXMLTextLine, left_line_left: int, left_line_right: int,
                     right_line_left: int, right_line_right: int,
                     left_line_text: str, right_line_text: str, debug: int = 0):
    left_baseline = make_split_baseline(line, left_line_right=left_line_right)
    right_baseline = make_split_baseline(line, right_line_left=right_line_left)
    left_coords = make_split_coords(line, left_line_right=left_line_right, debug=debug)
    right_coords = make_split_coords(line, right_line_left=right_line_left, debug=debug)
    if debug > 0:
        print("long_line_splitter.make_split_lines - ")
        print(f"    line.left: {line.coords.left}\tline.right: {line.coords.right}")
        print(f"    line.baseline: {line.baseline.points}")
        print(f"    line.coords: {line.coords.points}")
        print(f"    left_line_left: {left_line_left}\tleft_line_right: {left_line_right}")
        print(f"    right_line_left: {right_line_left}\tright_line_right: {right_line_right}")
        print(f"    left_baseline: {left_baseline.points}")
        print(f"    right_baseline: {right_baseline.points}")
        print(f"    left_coords: {left_coords.points}")
        print(f"    right_coords: {right_coords.points}")
    if left_baseline is None or left_coords is None:
        return None, line
    elif right_baseline is None or right_coords is None:
        return line, None
    left_line = pdm.PageXMLTextLine(metadata=copy.deepcopy(line.metadata), coords=left_coords,
                                    baseline=left_baseline, text=left_line_text)
    right_line = pdm.PageXMLTextLine(metadata=copy.deepcopy(line.metadata), coords=right_coords,
                                     baseline=right_baseline, text=right_line_text)
    left_line.set_derived_id(line.metadata['scan_id'])
    right_line.set_derived_id(line.metadata['scan_id'])
    return left_line, right_line


def make_split_baseline(line: pdm.PageXMLTextLine, left_line_right: int = None,
                        right_line_left: int = None, debug: int = 0) -> Union[pdm.Baseline, None]:
    """Make baselines for the left and right parts of the merged line."""
    if right_line_left is not None:
        points = [point for point in line.baseline.points if point[0] >= right_line_left]
        if debug > 0:
            print(f"right_line_left: {right_line_left}\tpoints: {points}")
        if len(points) == 0:
            return None
        if min([point[0] for point in points]) > right_line_left:
            # add a left_most point with the same height as the first selected point in the baseline
            right_line_left_point = (right_line_left, points[0][1])
            points = [right_line_left_point] + points
        return pdm.Baseline(points)
    elif left_line_right is not None:
        points = [point for point in line.baseline.points if point[0] <= left_line_right]
        if debug > 0:
            print(f"left_line_right: {left_line_right}\tpoints: {points}")
        if len(points) == 0:
            return None
        if max([point[0] for point in points]) < left_line_right:
            # add a right_most point with the same height as the last selected point in the baseline
            left_line_right_point = (left_line_right, points[0][1])
            points.append(left_line_right_point)
        return pdm.Baseline(points)
    else:
        raise ValueError("one of 'left' and 'right' must be an integer")


def make_split_coords(line: pdm.PageXMLTextLine, left_line_right: int = None,
                      right_line_left: int = None, debug: int = 0) -> Union[pdm.Coords, None]:
    """Make bounding box coordinates for the left and right parts of the merged line."""
    sort_coords_above_below_baseline(line)
    above_points, below_points = sort_coords_above_below_baseline(line)
    if len(above_points) == 0 or len(below_points) == 0:
        # For some reason the bounding box is not covering the baseline.
        # Hack: create a bounding box that is 40 pixels above and below the baseline.
        above_points = [[p[0], p[1] - 40] for p in line.baseline.points]
        below_points = [[p[0], p[1] + 20] for p in line.baseline.points]
    if right_line_left is not None:
        above_right_line_points = [point for point in above_points if point[0] >= right_line_left]
        below_right_line_points = [point for point in below_points if point[0] >= right_line_left]
        if debug > 0:
            print(f"right_line_left: {right_line_left}\tabove_right_line_points: {above_right_line_points}")
            print(f"\tbelow_right_line_points: {below_right_line_points}")
        if len(above_right_line_points) == 0 or len(below_right_line_points) == 0:
            return None
        elif len(above_right_line_points) == 0:
            above_right_line_points = [
                [right_line_left, above_points[0][1]],
                [line.coords.right, above_points[0][1]]
            ]
        elif len(below_right_line_points) == 0:
            below_right_line_points = [
                [right_line_left, below_points[0][1]],
                [line.coords.right, below_points[0][1]]
            ]
        if min([point[0] for point in above_right_line_points]) > right_line_left:
            # add a left_most point with the same height as the first selected point in the coords
            above_right_line_points = [[right_line_left, above_points[0][1]]] + above_right_line_points
        if min([point[0] for point in below_right_line_points]) > right_line_left:
            # add a left_most point with the same height as the first selected point in the coords
            below_right_line_points += [[right_line_left, below_points[0][1]]]
        return pdm.Coords(above_right_line_points + below_right_line_points)
    elif left_line_right is not None:
        above_left_line_points = [point for point in above_points if point[0] <= left_line_right]
        below_left_line_points = [point for point in below_points if point[0] <= left_line_right]
        if debug > 0:
            print(f"left_line_right: {left_line_right}\tabove_left_line_points: {above_left_line_points}")
            print(f"\tbelow_left_line_points: {below_left_line_points}")
        if len(above_left_line_points) == 0 and len(below_left_line_points) == 0:
            return None
        elif len(above_left_line_points) == 0:
            above_left_line_points = [
                [left_line_right, above_points[0][1]],
                [line.coords.left, above_points[0][1]]
            ]
        elif len(below_left_line_points) == 0:
            below_left_line_points = [
                [left_line_right, below_points[0][1]],
                [line.coords.left, below_points[0][1]]
            ]
        if max([point[0] for point in above_left_line_points]) < left_line_right:
            # add a left_most point with the same height as the first selected point in the coords
            above_left_line_points += [[left_line_right, above_points[0][1]]]
        if max([point[0] for point in below_left_line_points]) < left_line_right:
            # add a left_most point with the same height as the first selected point in the coords
            below_left_line_points = [[left_line_right, below_points[0][1]]] + below_left_line_points
        return pdm.Coords(above_left_line_points + below_left_line_points)
    else:
        raise ValueError("one of 'left' and 'right' must be an integer")


class LineSplitter:

    def __init__(self, min_merged_width: int,
                 default_line_width: int, default_line_num_chars: int,
                 text_left_boundary: int, text_right_boundary: int,
                 column_sep_width: int = 20):
        self.min_merged_width = min_merged_width
        self.default_line_num_chars = default_line_num_chars
        self.default_line_width = default_line_width
        self.text_left_boundary = text_left_boundary
        self.text_right_boundary = text_right_boundary
        self.min_offset = default_line_num_chars - 15
        self.max_offset = default_line_num_chars + 15
        self.column_sep_width = column_sep_width
        if default_line_width is None:
            self.default_line_width = int((text_right_boundary - text_left_boundary - column_sep_width) / 2)
        self.left_col_left = text_left_boundary
        self.left_col_right = text_left_boundary + self.default_line_width
        self.right_col_left = self.left_col_right + column_sep_width
        self.right_col_right = text_right_boundary

    def is_merged_line(self, line: pdm.PageXMLTextLine, min_merged_width: int = None):
        """Check if a text line is a merge of lines from adjacent columns."""
        if line.text is None:
            return None
        if len(line.text) < self.default_line_num_chars:
            return None
        if min_merged_width is None:
            min_merged_width = self.min_merged_width
        return width_in_range(line, min_width=min_merged_width)

    def is_merged_hyphenated_line(self, line: pdm.PageXMLTextLine, min_merged_width: int = None,
                                  debug: int = 0):
        """Check if a text line is a merge of lines from adjacent columns, where
        the first line ends in a hyphenated word break."""
        if line.text is None:
            return None
        if self.is_merged_line(line, min_merged_width=min_merged_width) is False:
            return False
        if debug > 1:
            print(f"min_offset: {self.min_offset}\tmax_offset: {self.max_offset}")
        if match := re.search(r"(\w+)- (\w+)", line.text[self.min_offset-1:]):
            start, end = match.span()
            hyphen_offset = self.min_offset - 1 + start + len(match.group(1))
            if debug > 1:
                print(f"start: {start}\tend: {end}\thyphen_offset: {hyphen_offset}")
            return self.min_offset <= hyphen_offset <= self.max_offset
        else:
            return False

    def get_left_right_ends(self, line: pdm.PageXMLTextLine, line_info: LineInfo,
                            column_sep_width: int = None):
        """Determine the left and right side coordinates of the left and right parts of the
        merged line."""

        left_indent = line.coords.left - line_info.text_left_boundary
        if left_indent < 0:
            left_indent = 0
        if column_sep_width is None:
            column_sep_width = self.column_sep_width
        left_line_left = line.coords.left
        left_line_right = line.coords.left + self.default_line_width - left_indent
        # add average column separation space
        right_line_left = line.coords.left + column_sep_width + self.default_line_width - left_indent
        right_line_right = line.coords.right
        if any([pos is None for pos in [left_line_left, left_line_right, right_line_left, right_line_right]]):
            print("long_line_splitter.get_left_right_ends:")
            print(f"    left_line_left: #{left_line_left}#")
            print(f"    left_line_right: #{left_line_right}#")
            print(f"    right_line_left: #{right_line_left}#")
            print(f"    right_line_right: #{right_line_right}#")
            raise ValueError(f"invalid left/right position value")
        return left_line_left, left_line_right, right_line_left, right_line_right

    def make_split_lines(self, line: pdm.PageXMLTextLine, line_info: LineInfo,
                         left_line_text: str, right_line_text: str, debug: int = 0):
        left_line_left, left_line_right, right_line_left, right_line_right = self.get_left_right_ends(line, line_info)
        return make_split_lines(line, left_line_left, left_line_right, right_line_left, right_line_right,
                                left_line_text, right_line_text, debug=debug)

    def split_merged_hyphenated_line(self, line: pdm.PageXMLTextLine, line_info: LineInfo,
                                     debug: int = 0) -> Tuple[pdm.PageXMLTextLine, Union[pdm.PageXMLTextLine, None]]:
        """Split a merged line where the left part ends in a word-break with a hyphen."""
        match = re.search(r"(\w+)- (\w+)", line.text[self.min_offset-1:self.max_offset+3])
        if match is None:
            return line, None
        left_line_text, right_line_text = extract_left_right_splits_from_regex_match(line, match, self.min_offset)
        left_line, right_line = self.make_split_lines(line, line_info, left_line_text,
                                                      right_line_text, debug=debug)
        return left_line, right_line

    def get_merged_hyphenated_lines(self, doc: Union[pdm.PageXMLTextRegion, List[pdm.PageXMLTextLine]],
                                    min_merged_width: int = None):
        """Return all lines from a text region that are merged hyphenated lines"""
        if min_merged_width is None:
            min_merged_width = self.min_merged_width
        lines = doc if isinstance(doc, list) else doc.get_lines()
        return [line for line in lines if self.is_merged_hyphenated_line(line, min_merged_width=min_merged_width)]

    def get_merged_lines(self, doc: Union[pdm.PageXMLTextRegion, List[pdm.PageXMLTextLine]],
                         min_merged_width: int = None):
        """Return all lines from a text region that are merged hyphenated lines"""
        lines = doc if isinstance(doc, list) else doc.get_lines()
        if min_merged_width is None:
            min_merged_width = self.min_merged_width
        return [line for line in lines if self.is_merged_line(line, min_merged_width=min_merged_width)]

    def has_merged_lines(self, doc: Union[pdm.PageXMLTextRegion, List[pdm.PageXMLTextLine]],
                         min_merged_width: int = None) -> bool:
        """Check if a PageXMLTextRegion document has any lines
        that are the merge of lines from two adjacent columns."""
        lines = doc if isinstance(doc, list) else doc.get_lines()
        if min_merged_width is None:
            min_merged_width = self.min_merged_width
        return any([self.is_merged_line(line, min_merged_width=min_merged_width) for line in lines])

    @staticmethod
    def doc_has_regular_lines(doc: Union[pdm.PageXMLTextRegion, List[pdm.PageXMLTextLine]], max_width: int) -> bool:
        """Check if a PageXMLTextRegion document has any lines
        that are the merge of lines from two adjacent columns."""
        lines = doc if isinstance(doc, list) else doc.get_lines()
        return any([width_in_range(line, max_width=max_width) for line in lines])


def get_split_word_candidates(line_splitter: LineSplitter, line_info: LineInfo, debug: int = 0):
    split_words = {
        'left_first': [],
        'left_last': [],
        'right_first': [],
        'right_last': [],
    }
    text_lines = [line for line in line_info.line_group if line.text is not None]
    if len(text_lines) == 0:
        return split_words
    first_line, last_line = text_lines[0], text_lines[-1]
    left_indent_width = first_line.coords.left - line_info.left_col_left
    right_indent_width = line_info.right_col_right - last_line.coords.right
    left_indent = True if left_indent_width > 250 else False
    right_indent = True if right_indent_width > 100 else False

    if debug > 0:
        print(f"long_line_splitter.get_split_word_candidates:")
        print(f"    first_line.coords.left: {first_line.coords.left}\tleft_col_left: {line_info.left_col_left}")
        print(f"   left_indent: {left_indent}\tright_indent: {right_indent}")
    # if the first line is not left indented, it starts on the left side of the column and
    # its first word probably is the continuation of the sentence of the previous left-column
    # line
    if left_indent is False:
        first_line_words = [word for word in line_info.words if word.line == first_line]
        split_words['left_first'] = [first_line_words[0]]

    # if the last line is not right indented, it ends on the right side of the column and
    # its last word probably is the location of the line break, and the sentence is continued
    # on the next line
    if right_indent is False:
        last_line_words = [word for word in line_info.words if word.line == last_line]
        split_words['right_last'] = [last_line_words[-1]]

    for line in line_info.line_group:
        line_words = [word for word in line_info.words if word.line == line]
        if abs(line.coords.right - line_info.left_col_right) < 100:
            # Line ends close to right side of left column.
            # Its last word is probably at the line break
            split_words['left_last'] = [line_words[-1]]
        elif abs(line.coords.left - line_info.right_col_left) < 100:
            # Line starts close to the left side of right column.
            # Its first word is probably the continuation after the previous line break
            split_words['right_first'] = [line_words[0]]
        if line_splitter.is_merged_line(line):
            # TO DO: check if there are relatively fewer characters for a full width line
            # Consider changing the offsets based on that.
            # If the line is left-indented (typically the start of a paragraph or an
            # unrecognised drop capital), adjust the min offset accordingly
            min_offset = line_splitter.min_offset
            if left_indent_width > 100:
                left_indent_width = line.coords.left - line_info.text_left_boundary
                avg_char_width = line.coords.width / len(line.text)
                offset_shift = int(left_indent_width / avg_char_width)
                min_offset -= offset_shift
                if debug > 0:
                    print(f"adjusting the min offset by {offset_shift} for line {line.id}")
            split_indexes = get_split_indexes(line_words, min_offset=min_offset,
                                              max_offset=line_splitter.max_offset)
            if debug > 0 and left_indent is True:
                print(f"min_offset: {min_offset}")
                print(f"split_indexes: {split_indexes}")
            sws = [line_words[si] for si in split_indexes]
            if debug > 0:
                print(f"merged_line - words: {line_words}")
                print(f"merged_line - split_indexes: {split_indexes}")
                print(f"merged_line - sws: {sws}")
            split_words['left_last'] = sws
            split_words['right_first'] = sws
    return split_words


def make_page_merged_line_contexts(line_splitter: LineSplitter, page: pdm.PageXMLPage,
                                   debug: int = 0):
    content_lines = [line for col in page.columns for line in col.get_lines() if line.text is not None]
    if not all([line.id is None or line.id.startswith('NL-HaNA') for line in content_lines]):
        print(f"long_line_splitter.make_page_merged_line_contexts - some of content_lines have no ID")
        for li, line in enumerate(content_lines):
            print(f"    content_lines[{li}]: #{line.id}#\t{line.text}")
    if debug > 0:
        print("long_line_splitter.make_page_merged_line_contexts:")
        print(f"\t{page.id}, number of content lines: {len(content_lines)}")
    line_groups = horizontal_group_lines(content_lines)
    line_infos = []
    for li, line_group in enumerate(line_groups):
        if not all([line.id is None or line.id.startswith('NL-HaNA') for line in line_group]):
            print(f"long_line_splitter.make_page_merged_line_contexts - some of line_group have no ID")
            for lgi, line in enumerate(line_group):
                print(f"    line_group[{lgi}]: #{line.id}#\t{line.text}")
        dist_to_prev = None if li == 0 else get_dist_to_other(line_group, line_groups[li-1])
        dist_to_next = None if li == len(line_groups) - 1 else get_dist_to_other(line_group, line_groups[li+1])
        line_info = LineInfo(line_group, line_splitter.column_sep_width,
                             line_splitter.default_line_num_chars,
                             dist_to_prev, dist_to_next)
        add_word_info(line_info)
        line_infos.append(line_info)
    assert len(line_infos) == len(line_groups)
    for li, line_info in enumerate(line_infos):
        context_infos = [line_info]
        if line_info.dist_to_prev is not None:
            max_dist_to_prev = 400
            sum_dist = line_info.dist_to_prev
            for pi in range(0, li):
                prev_info = line_infos[pi]
                if sum_dist > max_dist_to_prev:
                    break
                context_infos.append(prev_info)
                if prev_info.dist_to_prev is not None:
                    sum_dist = sum_dist + prev_info.dist_to_prev
        if line_info.dist_to_next is not None:
            max_dist_to_next = 400
            sum_dist = line_info.dist_to_next
            for ni in range(li+1, len(line_infos)):
                next_info = line_infos[ni]
                if sum_dist > max_dist_to_next:
                    break
                context_infos.append(next_info)
                if next_info.dist_to_next is not None:
                    sum_dist = sum_dist + next_info.dist_to_next
        full_width_context = [ci for ci in context_infos if ci.width > 1700]
        if len(full_width_context) == 0:
            full_width_context = context_infos
        context_lefts = np.array([ci.min_left for ci in full_width_context])
        context_rights = np.array([ci.max_right for ci in full_width_context])
        line_info.update_boundaries(int(context_lefts.mean()), int(context_rights.mean()))
        line_info.split_words = get_split_word_candidates(line_splitter, line_info)
    merged_context_windows = []
    for li, line_info in enumerate(line_infos):
        # print(f"long_line_splitter.make_page_line_context_windows - line_info")
        line_info = line_infos[li]
        # print(line_info)
        if len(line_infos) == 1:
            prev_group = None
            next_group = None
        else:
            prev_group = line_infos[-1] if li == 0 else line_infos[li-1]
            next_group = line_infos[0] if li == len(line_infos) - 1 else line_infos[li+1]
        mlc = MergedLineContext(line_info, prev_group, next_group,
                                column_sep_width=line_splitter.column_sep_width,
                                default_line_num_chars=line_splitter.default_line_num_chars)
        # mlc.prev_info.split_words = get_split_word_candidates(line_splitter, mlc.prev_info)
        # mlc.curr_info.split_words = get_split_word_candidates(line_splitter, mlc.curr_info)
        # mlc.next_info.split_words = get_split_word_candidates(line_splitter, mlc.next_info)
        merged_context_windows.append(mlc)
    return merged_context_windows


def replace_merged_line(merged_line: pdm.PageXMLTextLine, left_line: pdm.PageXMLTextLine,
                        right_line: pdm.PageXMLTextLine):
    """Replace the merged lines in its parent text region with the left and right lines."""
    merged_line_tr = merged_line.parent
    new_lines = []
    for tr_line in merged_line_tr.lines:
        if tr_line == merged_line:
            new_lines.extend([left_line, right_line])
        else:
            new_lines.append(tr_line)
    merged_line_tr.lines = new_lines


def update_merged_line_context(line_splitter: LineSplitter, mlc: MergedLineContext, merged_line: pdm.PageXMLTextLine,
                               left_line: pdm.PageXMLTextLine, right_line: pdm.PageXMLTextLine):
    """Replace the merged line with the left and right split lines in the merged line context
    and update the split words in the current line info."""
    new_line_group = []
    for line in mlc.curr_info.line_group:
        if line == merged_line:
            new_line_group.extend([left_line, right_line])
        else:
            new_line_group.append(line)
    # add the updates lines with split lines as the new line group
    mlc.curr_info.line_group = new_line_group
    # update the word info list
    add_word_info(mlc.curr_info)
    # update the split word candidates
    mlc.curr_info.split_words = get_split_word_candidates(line_splitter, mlc.curr_info)


def page_split_merged_hyphenated_lines(line_splitter: LineSplitter, page: pdm.PageXMLPage):
    new_page = copy_page(page)
    mlcs = make_page_merged_line_contexts(line_splitter, new_page)
    pdm.set_parentage(new_page)
    for mlc in mlcs:
        for line in mlc.curr_info.line_group:
            if line.text is not None and line_splitter.is_merged_hyphenated_line(line):
                left_line, right_line = line_splitter.split_merged_hyphenated_line(line, mlc.curr_info, debug=0)
                if left_line is None or right_line is None:
                    # if one of the split lines is None, it means the original line is not a merged line.
                    # This happens for example when there are two hyphens close to each other, as in
                    # the following line: "Ontfangen een Missive van den Vice- Admi-".
                    # The first hyphen triggers the merged hyphenation splitter, but splitting on the
                    # regular line width sets "Admi-" as the last word and finds nothing after it for
                    # the right line.
                    continue
                replace_merged_line(line, left_line, right_line)
    return new_page


def split_merged_non_hyphenated_lines(line_splitter: LineSplitter, mlc: MergedLineContext,
                                      ngram_scorer: Callable[[str, str], float], debug: int = 0):
    if debug > 0:
        print([line.id for line in mlc.curr_info.line_group])
        print([line.text for line in mlc.prev_info.line_group])
        print([line.text for line in mlc.curr_info.line_group])
        print([line.text for line in mlc.next_info.line_group])
    combinations = make_bigram_combinations(mlc, ngram_scorer, debug=0)
    if len(combinations) == 0:
        print("long_line_splitter.split_merged_non_hyphenated_lines:")
        line_ids = [line.id for line in mlc.curr_info.line_group]
        print(f"\tno combinations for lines {line_ids}!")
        print([line.text for line in mlc.curr_info.line_group])
        return None
    best_comb = select_best_combination(combinations)
    if debug > 0:
        print(f"\nbest_comb:")
        print_comb(best_comb)
        if 'curr_left_before_last' in best_comb:
            print([best_comb['curr_left_before_last'], best_comb['curr_left_last'],
                   best_comb['next_left_first']])
        else:
            print([best_comb['curr_left_last'], best_comb['next_left_first']])
        if 'prev_right_before_last' in best_comb:
            print([best_comb['prev_right_before_last'], best_comb['prev_right_last'],
                   best_comb['curr_right_first']])
        else:
            print([best_comb['prev_right_last'], best_comb['curr_right_first']])
    left_text, right_text = extract_left_right_split_from_combination(mlc.curr_info, best_comb)
    if left_text is None or right_text is None:
        # if one of the split line texts is None, it means there is no good place to split the original.
        return None
    left_last = best_comb['curr_left_last']
    merged_line = [line for line in mlc.curr_info.line_group if line.id == left_last.line.id][0]
    left_line, right_line = line_splitter.make_split_lines(merged_line, mlc.curr_info, left_text, right_text)
    if left_line is None or right_line is None:
        # if one of the split lines is None, it means the original line is not a merged line.
        return None
    replace_merged_line(merged_line, left_line, right_line)
    try:
        update_merged_line_context(line_splitter, mlc, merged_line, left_line, right_line)
    except IndexError:
        print("long_line_splitter.split_merged_non_hyphenated_lines - error updating merged line context:")
        print(f"  merged_line: {merged_line.text}")
        print(f"  left_line: {left_line.text}")
        print(f"  right_line: {right_line.text}")
        raise
    if debug > 0:
        print('\n------------------\n')


def get_page_content_lines(page: pdm.PageXMLPage) -> List[pdm.PageXMLTextLine]:
    return [line for col in page.columns for tr in col.text_regions for line in tr.lines]


def is_merged_line_distractor(mlc: MergedLineContext):
    if 'left_last' not in mlc.curr_info.split_words:
        return False
    if len(mlc.curr_info.split_words['left_last']) > 1:
        # There are multiple words
        return False
    # elif line_splitter.is_merged_line(line_info.line_group[0]):
    #     return False
    else:
        return True


def page_split_merged_non_hyphenated_lines(line_splitter: LineSplitter, page: pdm.PageXMLPage,
                                           ngram_scorer: Callable[[str, str], float],
                                           debug: int = 0):
    new_page = copy_page(page)

    if debug > 0:
        print(f"long_line_splitter.page_split_merged_non_hyphenated_lines - iterating over mlcs")
    mlcs = make_page_merged_line_contexts(line_splitter, new_page)
    for mi, mlc in enumerate(mlcs):
        if mlc == mlcs[0] and len(mlcs) > 1:
            # There is no line above, so prev is bottom of the column.
            # Use left_last as right_last
            prev_split_words = copy.deepcopy(mlc.prev_info.split_words)
            mlc.prev_info = LineInfo(mlc.prev_info.line_group, line_splitter.column_sep_width,
                                     line_splitter.default_line_num_chars, mlc.prev_info.dist_to_prev,
                                     mlc.prev_info.dist_to_next, line_splitter.default_line_width,
                                     mlc.prev_info.text_left_boundary, mlc.prev_info.text_right_boundary,
                                     mlc.prev_info.words, split_indexes=mlc.prev_info.split_indexes)
            mlc.prev_info.split_words = prev_split_words
            mlc.prev_info.split_words['right_last'] = mlc.prev_info.split_words['left_last']
            mlc.prev_info.split_words['left_last'] = []
        if mlc == mlcs[-1] and len(mlcs) > 1:
            # There is no line below, so next is top of the column.
            # Use right_first as left_first
            mlc.prev_info.split_words['left_first'] = mlc.prev_info.split_words['right_first']
            mlc.prev_info.split_words['right_first'] = []
        if line_splitter.has_merged_lines(mlc.curr_info.line_group):
            if is_merged_line_distractor(mlc) is False:
                if mlcs[mlcs.index(mlc)] == mlcs[-1]:
                    next_mlc = mlcs[0]
                else:
                    next_mlc = mlcs[mlcs.index(mlc) + 1]
                if debug > 0:
                    print(f"prev lines in next mlc before: {[line.text for line in next_mlc.prev_info.line_group]}")
                split_merged_non_hyphenated_lines(line_splitter, mlc, ngram_scorer, debug=debug)
                if debug > 0:
                    print(f"curr lines: {[line.text for line in mlc.curr_info.line_group]}")
                    print(f"prev lines in next mlc after: {[line.text for line in next_mlc.prev_info.line_group]}")
                if mlc == mlcs[-1]:
                    continue
                # if this merged line is split in this MergedLineContext, it
                # must be updated in the next MergedLineContext as well.
                # next_mlc = mlcs[mi+1]
                # next_mlc.prev_info
    return new_page


def evaluate_combination(combination):
    """Score a candidate line split position."""
    # 		curr_left_before_last	'de'
    # 		curr_left_last	'Provincie'
    # 		curr_right_first	'van'
    # 		curr_right_after_first	'den'
    # 		curr_left_last_curr_right_first	3.8382985408021213
    # 		prev_right_before_last	'geaddresseert'
    # 		prev_right_last	'aen'
    # 		prev_right_last_curr_right_first	-7.021463352203963
    # 		next_right_after_first	'op'
    # 		next_left_first	'Zeelandt.'
    # 		curr_left_last_next_left_first	-6.998636944631681
    scores = []
    for field in ['prev_right_last_curr_right_first', 'curr_left_last_next_left_first']:
        if field in combination:
            scores.append(combination[field])
    neg_scores = [score for score in scores if score < 0.0]
    pos_scores = [score for score in scores if score > 0.0]
    return {
        'scores': scores,
        'continue': combination['curr_left_last_curr_right_first'],
        'num_pos': len(pos_scores),
        'num_neg': len(neg_scores),
        'min_neg': min(neg_scores) if len(neg_scores) > 0 else 0.0,
        'sum_scores': sum(scores)
    }


def select_best_combination(combinations):
    """Select the best line split position."""
    best_comb = None
    best_scores = None
    for combination in combinations:
        scores = evaluate_combination(combination)
        if best_scores is None:
            best_scores = scores
            best_comb = combination
        elif scores['num_neg'] < best_scores['num_neg']:
            best_scores = scores
            best_comb = combination
        elif scores['continue'] < 0.0 and scores['continue'] < best_scores['continue']:
            best_scores = scores
            best_comb = combination
        elif scores['sum_scores'] > best_scores['sum_scores']:
            best_scores = scores
            best_comb = combination
    return best_comb


def print_comb(comb, prefix=None):
    if prefix is None:
        prefix = ''
    print()
    for field in comb:
        if isinstance(comb[field], SplitWord):
            print(f"\t{prefix}{field}\t{comb[field].word}")
        else:
            print(f"\t{prefix}{field}\t{comb[field]}")
    print()


def make_bigram_combinations(mlc, ngram_scorer, debug: int = 0):
    curr_sws = mlc.curr_info.split_words
    prev_sws = mlc.prev_info.split_words
    next_sws = mlc.next_info.split_words
    if debug > 0:
        print("prev_sws:", prev_sws)
        print("curr_sws:", curr_sws)
        print("next_sws:", next_sws)

    if len(curr_sws['left_last']) == 0:
        raise ValueError(f"curr left_last is empty, line is not merged?")
    if len(curr_sws['left_last']) == 1:
        print(f"long_line_splitter.make_bigram_combinations - curr_line group:")
        print([line.text for line in mlc.curr_info.line_group])
        print(f"curr_info.split_words: {curr_sws}")
        raise ValueError(f"curr left_last is single split word, line is not merged?")
    combs = []
    prev_combs = []
    next_combs = []

    for idx in range(len(curr_sws['left_last']) - 1):
        curr_words = mlc.curr_info.words
        # print(f"idx: {idx}\tcurr left_last: {curr_sws['left_last']}\t right_first{curr_sws['right_first']}")
        curr_left_last = curr_sws['left_last'][idx]
        curr_right_first = curr_sws['left_last'][idx + 1]
        if debug > 0:
            print("curr_left_last:", curr_left_last, curr_left_last.index, curr_left_last.lg_index, curr_left_last.word)
        lg_index = curr_left_last.lg_index
        comb = {
            'curr_left_before_last': curr_words[lg_index - 1],
            'curr_left_last': curr_left_last,
            'curr_right_first': curr_words[lg_index + 1],
            'curr_right_after_first': curr_words[lg_index + 2] if len(curr_words) > lg_index + 2 else None,
            'curr_left_last_curr_right_first': ngram_scorer(curr_left_last.word, curr_right_first.word),
        }

        if debug > 1:
            print_comb(comb)
        combs.append(comb)
    if debug > 0:
        print(f"CURR: number of combinations: {len(combs)}")
        print('prev words:', mlc.prev_info.words)

    for comb in combs:
        if len(prev_sws['right_last']) == 0:
            prev_combs = [comb for comb in combs]
        for prev_right_last in prev_sws['right_last']:
            prev_right_before_last = mlc.prev_info.words[prev_right_last.lg_index - 1]
            curr_right_first = comb['curr_right_first']
            if debug > 0:
                print(prev_right_before_last, prev_right_last.word, curr_right_first.word)
            # prev_comb = copy.deepcopy(comb)
            prev_comb = {field: comb[field] for field in comb}
            prev_comb['prev_right_before_last'] = prev_right_before_last
            prev_comb['prev_right_last'] = prev_right_last
            prev_comb['prev_right_last_curr_right_first'] = ngram_scorer(prev_right_last.word, curr_right_first.word)

            prev_combs.append(prev_comb)
            if debug > 1:
                print_comb(prev_comb, '\t')

    if debug > 0:
        print(f"PREV: number of combinations: {len(prev_combs)}")
        print('next words:', mlc.next_info.words)

    for prev_comb in prev_combs:
        if len(next_sws['left_first']) == 0:
            next_combs = [comb for comb in prev_combs]
        next_words = mlc.next_info.words
        for next_left_first in next_sws['left_first']:
            lg_index = next_left_first.lg_index
            next_right_after_first = next_words[lg_index + 1] if len(next_words) > lg_index + 1 else None
            curr_left_last = prev_comb['curr_left_last']
            if debug > 0:
                print(curr_left_last, next_left_first, next_right_after_first)
            next_comb = {field: prev_comb[field] for field in prev_comb}
            next_comb['next_right_after_first'] = next_right_after_first
            next_comb['next_left_first'] = next_left_first
            next_comb['curr_left_last_next_left_first'] = ngram_scorer(curr_left_last.word, next_left_first.word)

            next_combs.append(next_comb)
            if debug > 1:
                print_comb(next_comb, '\t')
    if debug > 0:
        print(f"NEXT: number of combinations: {len(next_combs)}")
    return next_combs


def make_column_text_region(col_lines, page, first_tr):
    coords = pdm.parse_derived_coords(col_lines)
    tr = pdm.PageXMLTextRegion(metadata=copy.deepcopy(first_tr.metadata), coords=coords,
                               lines=col_lines)
    tr.set_derived_id(page.metadata['scan_id'])
    tr.set_as_parent(tr.lines)
    return tr


def determine_line_column_side(line, left_col_right, right_col_left):
    if line.coords.right < right_col_left:
        return 'left'
    if line.coords.left > left_col_right:
        return 'right'
    left_side_width = left_col_right - line.coords.left
    right_side_width = line.coords.right - right_col_left
    if left_side_width > right_side_width:
        return 'left'
    elif right_side_width > left_side_width:
        return 'right'
    else:
        print(f"line.text: {line.text}")
        print(f"line.coords.box: {line.coords.box}")
        print(f"left_col_right: {left_col_right}\tright_col_left: {right_col_left}")
        return 'left'


def make_column(col_lines, page, first_col, first_tr):
    coords = pdm.parse_derived_coords(col_lines)
    tr = make_column_text_region(col_lines, page, first_tr)
    col = pdm.PageXMLColumn(metadata=copy.deepcopy(first_col.metadata), coords=coords,
                            text_regions=[tr])
    col.set_derived_id(page.metadata['scan_id'])
    col.set_as_parent(col.text_regions)
    tr.metadata['column_id'] = col.id
    for line in tr.lines:
        line.metadata['text_region_id'] = tr.id
        line.metadata['column_id'] = col.id
    return col


def _page_text_regions(page: pdm.PageXMLPage) -> List[pdm.PageXMLTextRegion]:
    """The text regions in the columns of a page, then its extra regions (AS fix 1).

    This is what ``PageXMLPage.get_all_text_regions`` returned in pagexml-tools 0.7.x. Since
    0.8.0 the method is inherited from ``PageXMLRegion``, looks only at ``page.text_regions``
    (empty for a page with columns), so ``page_split_columns`` raised an IndexError."""
    text_regions = [tr for col in page.columns for tr in col.text_regions]
    text_regions.extend([tr for tr in page.extra])
    return text_regions


def page_split_columns(line_splitter: LineSplitter, page: pdm.PageXMLPage, debug: int = 0):
    """Split the lines of a page into two columns after the merged lines have been split."""
    page = copy_page(page)
    if page.stats['lines'] == 0:
        return page
    trs = _page_text_regions(page)
    first_tr = trs[0]
    first_col = page.columns[0]
    if debug > 0:
        print("PRE:", page.id, page.stats)
    if page.coords.width > 1500:
        col_default_width = int((page.coords.width - line_splitter.column_sep_width) / 2)
    else:
        col_default_width = 900
    left_col_right = page.coords.left + col_default_width
    right_col_left = left_col_right + line_splitter.column_sep_width
    left_col_lines = []
    right_col_lines = []
    extra_lines = []
    mlcs = make_page_merged_line_contexts(line_splitter, page)
    for mlc in mlcs:
        ci = mlc.curr_info
        curr_line_width = ci.text_right_boundary - ci.text_left_boundary
        if curr_line_width > 1500:
            left_col_right, right_col_left = ci.left_col_right, ci.right_col_left
        for line in ci.line_group:
            col_side = determine_line_column_side(line, left_col_right, right_col_left)
            if col_side == 'left':
                left_col_lines.append(line)
            elif col_side == 'right':
                right_col_lines.append(line)
            else:
                extra_lines.append(line)

    columns = []
    if len(left_col_lines) > 0:
        left_col = make_column(left_col_lines, page, first_col, first_tr)
        columns.append(left_col)
    if len(right_col_lines) > 0:
        right_col = make_column(right_col_lines, page, first_col, first_tr)
        columns.append(right_col)
    if len(columns) > 0:
        page.columns = columns
    if len(extra_lines) > 0:
        extra_tr = make_column_text_region(extra_lines, page, first_tr)
        page.extra.append(extra_tr)
    if debug > 0:
        print("POST:", page.id, page.stats)

    return page


def horizontal_group_lines(lines: List[pdm.PageXMLTextLine]) -> List[List[pdm.PageXMLTextLine]]:
    """Sort lines of a text region vertically as a list of lists,
    with adjacent lines grouped in inner lists."""
    if len(lines) == 0:
        return []
    # First, sort lines vertically
    # Important: include ALL lines with coords, including empty ones
    vertically_sorted = [line for line in sorted(lines, key=lambda line: line.coords.top)]
    if len(vertically_sorted) == 0:
        for line in lines:
            print(line.coords.box, line.text)
        return []
    # Second, group adjacent lines in vertical line stack
    horizontally_grouped_lines = [[vertically_sorted[0]]]
    rest_lines = vertically_sorted[1:]
    if len(vertically_sorted) > 1:
        for li, curr_line in enumerate(rest_lines):
            prev_line = horizontally_grouped_lines[-1][-1]
            if curr_line.is_below(prev_line):
                # directly below previous line means: start a new group
                horizontally_grouped_lines.append([curr_line])
            elif curr_line.is_next_to(prev_line):
                if any([curr_line.is_below(line) for line in horizontally_grouped_lines[-1]]):
                    # next to previous but below another line that is also next to previous
                    # means: start a new group
                    horizontally_grouped_lines.append([curr_line])
                else:
                    # next to previous line means: add to last group
                    horizontally_grouped_lines[-1].append(curr_line)
            else:
                # below but not directly means: start a new group
                horizontally_grouped_lines.append([curr_line])
    # Third, sort adjecent lines horizontally
    for line_group in horizontally_grouped_lines:
        line_group.sort(key=lambda line: line.coords.left)
    return horizontally_grouped_lines


# --- AS adapter ---
#
# Everything below is AS code (not ported). It derives the splitter parameters per inventory
# and page side from the lines themselves, fits the bigram scorer in-corpus (no labels are
# used), and wraps the ported functions as a scan hook for ``extract_inventory_v2``.

import math
import statistics
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, Optional, Sequence


@dataclass(frozen=True)
class SplitParams:
    """The arguments of :class:`LineSplitter` plus the region tags that take part."""

    min_merged_width: int
    default_line_width: int
    default_line_num_chars: int
    text_left_boundary: int
    text_right_boundary: int
    column_sep_width: int = 20
    split_region_tags: tuple = ()

    def make_splitter(self) -> "LineSplitter":
        return LineSplitter(
            min_merged_width=self.min_merged_width,
            default_line_width=self.default_line_width,
            default_line_num_chars=self.default_line_num_chars,
            text_left_boundary=self.text_left_boundary,
            text_right_boundary=self.text_right_boundary,
            column_sep_width=self.column_sep_width)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["split_region_tags"] = list(self.split_region_tags)
        return d


def _percentile(values: Sequence[float], q: float) -> float:
    return float(np.percentile(np.asarray(values, dtype=float), q))


def derive_split_params(line_rows: Iterable[dict], page_side: str,
                        split_region_tags: tuple = (), min_chars: int = 20,
                        merged_width_factor: float = 1.19) -> Optional[SplitParams]:
    """Derive the splitter parameters for one page side from v2 line rows.

    `line_rows` are dicts with ``x, y, w, h, text_raw`` (as in the v2 lines table, ints or
    numeric strings), already restricted to the participating region tags. Lines with fewer
    than `min_chars` characters are ignored. Returns None when no line qualifies.

    The text span is the 5th percentile of ``x`` to the 95th percentile of ``x + w``. Column
    lines are the rows whose width lies within 0.35-0.6 of that span; their median width and
    median number of characters are the default line width and character count. The plain
    median width is not used: in a print volume where most lines are merged it is the merged
    width. ``min_merged_width`` is `merged_width_factor` (default 1.19) times the default
    width: the notebook's 1100 px over 456's derived 924 px. 1.22 (1100/900) left 192 merged
    lines with a short right part unsplit in 456 (UA-002)."""
    rows = [r for r in line_rows
            if len(r["text_raw"]) >= min_chars and r["x"] != "" and r["w"] != ""
            and (not r.get("page_side") or r["page_side"] == page_side)]
    if not rows:
        return None
    xs = [int(r["x"]) for r in rows]
    rights = [int(r["x"]) + int(r["w"]) for r in rows]
    left = int(round(_percentile(xs, 5)))
    right = int(round(_percentile(rights, 95)))
    span = right - left
    col = [r for r in rows if 0.35 * span <= int(r["w"]) <= 0.6 * span]
    if not col:
        return None
    width = int(round(statistics.median(int(r["w"]) for r in col)))
    chars = int(round(statistics.median(len(r["text_raw"]) for r in col)))
    return SplitParams(
        min_merged_width=round(merged_width_factor * width), default_line_width=width,
        default_line_num_chars=chars, text_left_boundary=left, text_right_boundary=right,
        column_sep_width=20, split_region_tags=tuple(split_region_tags))


def train_bigram_finder(docs: Iterable[List[str]]):
    """NLTK bigram finder over documents (token lists), as the notebook built it."""
    from nltk.collocations import BigramCollocationFinder
    return BigramCollocationFinder.from_documents(list(docs))


def make_pmi_scorer(finder, min_uni_freq: int = 100, non_score: float = 0.0
                    ) -> Callable[[str, str], float]:
    """The notebook's scorer (``Splitting-Fagel-Merged-Lines.ipynb`` cell 32), `finder` passed in.

    PMI of the bigram when it was seen; otherwise, when both words are frequent
    (> `min_uni_freq`), the log of the (tiny) bigram probability over the product of the
    unigram probabilities, which is strongly negative; otherwise `non_score`."""
    from nltk.metrics import BigramAssocMeasures
    bigram_measures = BigramAssocMeasures()
    _SMALL = 1e-10

    def scorer(w1, w2):
        bf = finder.ngram_fd[(w1, w2)] if (w1, w2) in finder.ngram_fd else 0
        if bf > 0:
            return finder.score_ngram(bigram_measures.pmi, w1, w2)
        wf1 = finder.word_fd[w1] if w1 in finder.word_fd else 0
        wf2 = finder.word_fd[w2] if w2 in finder.word_fd else 0
        if bf == 0 and min(wf1, wf2) > min_uni_freq:
            return np.log(_SMALL / np.exp(np.log(wf1 / finder.N) + np.log(wf2 / finder.N)))
        return non_score
    return scorer


def training_docs_from_lines(line_runs: Iterable[Sequence[Any]], is_merged: Callable[[Any], bool],
                             get_text: Callable[[Any], Optional[str]]) -> List[List[str]]:
    """Token lists of runs of consecutive non-merged lines; a merged line ends a doc.

    `line_runs` is an iterable of line sequences (one per region); documents never span
    two regions. Lines without text also end a doc."""
    docs: List[List[str]] = []
    for lines in line_runs:
        doc: List[str] = []
        for line in lines:
            text = get_text(line)
            if text is None or text == "" or is_merged(line):
                if doc:
                    docs.append(doc)
                doc = []
                continue
            doc.extend(text.split(" "))
        if doc:
            docs.append(doc)
    return docs


def is_candidate_row(row: dict, params: SplitParams) -> bool:
    """`LineSplitter.is_merged_line`'s rule on a v2 line row: enough characters and wide."""
    if row["w"] == "" or row["x"] == "":
        return False
    return len(row["text_raw"]) >= params.default_line_num_chars and \
        int(row["w"]) >= params.min_merged_width


def merged_line_candidates(line_rows: Iterable[dict], params: SplitParams) -> List[dict]:
    """The rows that are merge candidates under `params`."""
    return [r for r in line_rows if is_candidate_row(r, params)]


@dataclass
class SplitModel:
    """Per-inventory splitter model: parameters per page side and a fitted scorer."""

    params: Dict[str, Optional[SplitParams]]
    scorer: Callable[[str, str], float]
    split_region_tags: tuple
    untagged_tag: str
    min_uni_freq: int = 100
    sizes: Optional[Dict[str, int]] = None
    other_tag_candidates: Optional[Dict[str, int]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"params": {side: (p.to_dict() if p else None) for side, p in self.params.items()},
                "split_region_tags": list(self.split_region_tags),
                "min_uni_freq": self.min_uni_freq, "finder": self.sizes,
                "scorer": "notebook PMI scorer (Splitting-Fagel-Merged-Lines.ipynb cell 32)",
                "n_candidates_other_tags_fit": self.other_tag_candidates}


def _int_row(row: dict) -> dict:
    out = dict(row)
    for key in ("x", "y", "w", "h"):
        out[key] = int(row[key]) if row[key] != "" else ""
    return out


def fit_split_model_from_rows(line_rows: Sequence[dict], tag_by_region: Dict[str, str],
                              split_region_tags: tuple, untagged_tag: str = "untagged",
                              min_uni_freq: int = 100,
                              params: Optional[Dict[str, Optional[SplitParams]]] = None
                              ) -> SplitModel:
    """Fit parameters (per page side) and the bigram scorer from v2 line rows.

    Only lines of regions whose tag is in `split_region_tags` take part. No labels are used
    (the scorer only counts the inventory's own words), so there is no leakage."""
    part = [r for r in line_rows if tag_by_region.get(r["region_id"]) in split_region_tags]
    if params is None:
        params = {side: derive_split_params(part, side, split_region_tags)
                  for side in ("left", "right")}
    docs: List[List[str]] = []
    doc: List[str] = []
    last_region = None
    for r in part:
        key = (r["scan_id"], r["region_id"])
        if key != last_region:
            if doc:
                docs.append(doc)
            doc = []
            last_region = key
        p = params.get(r["page_side"])
        text = r["text_raw"]
        if not text or (p is not None and is_candidate_row(r, p)):
            if doc:
                docs.append(doc)
            doc = []
            continue
        doc.extend(text.split(" "))
    if doc:
        docs.append(doc)
    finder = train_bigram_finder(docs)
    other = {}
    for r in line_rows:
        tag = tag_by_region.get(r["region_id"])
        if tag in split_region_tags:
            continue
        p = params.get(r["page_side"])
        if p is not None and is_candidate_row(r, p):
            other[tag] = other.get(tag, 0) + 1
    return SplitModel(
        params=params, scorer=make_pmi_scorer(finder, min_uni_freq=min_uni_freq),
        split_region_tags=tuple(split_region_tags), untagged_tag=untagged_tag,
        min_uni_freq=min_uni_freq,
        sizes={"n_docs": len(docs), "n_tokens": int(finder.N),
               "n_word_types": len(finder.word_fd), "n_bigram_types": len(finder.ngram_fd)},
        other_tag_candidates=other)


def read_line_rows(lines_path) -> Tuple[List[dict], Dict[str, str]]:
    """Line rows (ints for x/y/w/h) and region tags from a v2 ``lines/<inv>.lines.tsv.gz`` and
    its sibling ``regions_meta/<inv>.tsv.gz``."""
    import csv
    import gzip
    from pathlib import Path
    lines_path = Path(lines_path)
    inv = lines_path.name.split(".")[0]
    regions_path = lines_path.parent.parent / "regions_meta" / f"{inv}.tsv.gz"
    csv.field_size_limit(1 << 24)
    with gzip.open(regions_path, "rt", newline="") as fh:
        tag_by_region = {r["region_id"]: r["region_tag"]
                         for r in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)}
    with gzip.open(lines_path, "rt", newline="") as fh:
        rows = [_int_row(r) for r in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)]
    return rows, tag_by_region


def fit_split_model(inv: str, source, preset, lines_path=None, params=None) -> SplitModel:
    """Fit the model of one inventory.

    With `lines_path` (a v2 lines table, e.g. from an earlier unsplit extract) the rows are
    read from it. Otherwise a first pass parses `source` and builds the rows in memory with
    ``extract_scan_v2`` (nothing is written). `params` (side -> SplitParams) replaces the
    derived parameters, also for the scorer's training documents."""
    if lines_path is not None:
        rows, tag_by_region = read_line_rows(lines_path)
    else:
        from collections import Counter
        from pagexml.parser import parse_pagexml_file
        from archival_structures.text_layer.extract_v2 import _DEELOPNAME, extract_scan_v2
        from archival_structures.text_layer.io import iter_pagexml
        rows, tag_by_region = [], {}
        for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):
            try:
                scan = parse_pagexml_file(member, pagexml_data=data)
            except Exception:
                continue
            s_row, r_rows, l_rows = extract_scan_v2(
                scan, inv=inv, inv_dir=inv, member=member, preset=preset)
            for r in r_rows:
                tag_by_region[r["region_id"]] = r["region_tag"]
            rows += [_int_row(r) for r in l_rows]
    return fit_split_model_from_rows(rows, tag_by_region, preset.split_region_tags,
                                     preset.untagged_tag, params=params)


def _norm_spaces(text: Optional[str]) -> Optional[str]:
    """Runs of spaces to one space. The HTR sometimes puts two spaces where two columns meet
    in a merged line, while its word elements are joined by one: the ported code needs the
    line text to equal the joined words (it raises ValueError otherwise)."""
    return None if text is None else re.sub(" {2,}", " ", text)


def _line_text(line) -> str:
    return line.text or ""


def _side_of(line, page_w) -> str:
    box = line.coords.box
    return "left" if box["x"] + box["w"] / 2 < page_w / 2 else "right"


def _is_cand(line, params: SplitParams) -> bool:
    if line.coords is None or line.text is None:
        return False
    return len(line.text) >= params.default_line_num_chars and \
        line.coords.width >= params.min_merged_width


class SplitHook:
    """Callable scan hook: splits merged lines of the participating regions of a scan.

    ``__call__(scan) -> scan`` mutates the scan in place (replaces merged lines by halves
    and sets ``column`` on every participating line) and updates ``self.stats``. Deterministic."""

    def __init__(self, model: SplitModel):
        from collections import Counter
        self.model = model
        self.stats = Counter()
        self.error_types = Counter()
        self.single_column_sides = 0
        self.error_samples: List[dict] = []
        self._alias: Dict[str, str] = {}   # unsplit line id -> id of its text piece (degenerate)
        self.splits: List[dict] = []   # one record per split, for error analysis
        self.record_splits = False

    # ------------------------------------------------------------------
    def __call__(self, scan):
        from archival_structures.text_layer.extract import iter_text_regions, region_tag_of
        from collections import Counter
        m = self.model
        coords = scan.coords
        page_w = coords.width if coords is not None else 0
        if not page_w:
            return scan
        by_side: Dict[str, list] = {"left": [], "right": []}
        for region, _depth in iter_text_regions(scan.text_regions or []):
            if not region.lines:
                continue
            tag = region_tag_of(region, Counter(), m.untagged_tag)
            for line in region.lines:
                if line.coords is None:
                    continue
                side = _side_of(line, page_w)
                p = m.params.get(side)
                if tag in m.split_region_tags:
                    by_side[side].append((region, line))
                    self.stats["n_lines_participating"] += 1
                    if p is not None and _is_cand(line, p):
                        self.stats["n_candidates_before"] += 1
                elif p is not None and _is_cand(line, p):
                    self.stats["n_candidates_other_tags"] += 1
                    self.stats[f"n_candidates_other_tags:{tag}"] += 1
        for side in ("left", "right"):
            if by_side[side] and m.params.get(side) is not None:
                self._process_side(scan, side, by_side[side], m.params[side], page_w)
            else:
                for region, line in by_side[side]:
                    if m.params.get(side) is None:
                        self.stats["n_lines_side_without_params"] += 1
        return scan

    # ------------------------------------------------------------------
    def _process_side(self, scan, side, region_lines, params, page_w):
        import contextlib
        import io
        from pagexml.helper.pagexml_helper import copy_line
        m = self.model
        self._alias = {}
        regions: Dict[int, Any] = {}
        order: List[int] = []
        for region, line in region_lines:
            if id(region) not in regions:
                regions[id(region)] = (region, [])
                order.append(id(region))
            regions[id(region)][1].append(line)
        scan_id = scan.id[:-4] if (scan.id or "").endswith(".jpg") else scan.id
        trs = []
        for key in order:
            region, lines = regions[key]
            copies = []
            for line in lines:
                c = copy_line(line)
                c.metadata["scan_id"] = scan_id
                if c.text != _norm_spaces(c.text):
                    c.text = _norm_spaces(c.text)
                    self.stats["n_lines_text_normalised"] += 1
                copies.append(c)
            trs.append(pdm.PageXMLTextRegion(doc_id=region.id, metadata={"scan_id": scan_id},
                                             coords=pdm.parse_derived_coords(copies), lines=copies))
        col = pdm.PageXMLColumn(doc_id=f"{scan_id}-{side}-column", metadata={"scan_id": scan_id},
                                coords=pdm.parse_derived_coords(trs), text_regions=trs)
        page = pdm.PageXMLPage(doc_id=f"{scan_id}-{side}-page", metadata={"scan_id": scan_id},
                               columns=[col])
        splitter = params.make_splitter()
        n_cand = sum(1 for _, ln in region_lines if _is_cand(ln, params))
        sink = io.StringIO()
        # the ported code prints diagnostics; keep them out of the CLI output
        with contextlib.redirect_stdout(sink):
            split_page = page
            if n_cand:
                try:
                    hy = page_split_merged_hyphenated_lines(splitter, page)
                    split_page = hy
                    try:
                        split_page = page_split_merged_non_hyphenated_lines(
                            splitter, hy, m.scorer)
                    except Exception as exc:
                        self.stats["n_page_errors"] += 1
                        self.error_types[f"bigram:{type(exc).__name__}"] += 1
                        self._note_error(scan_id, side, "bigram", exc)
                        split_page = hy
                except Exception as exc:
                    self.stats["n_page_errors"] += 1
                    self.error_types[f"hyphen:{type(exc).__name__}"] += 1
                    self._note_error(scan_id, side, "hyphen", exc)
                    hy, split_page = page, page
            else:
                hy = page
            hy_ids = {ln.id for tr in hy.columns[0].text_regions for ln in tr.lines}
            orig_ids = {ln.id for _, ln in region_lines}
            hyph_new = hy_ids - orig_ids
            self._write_back(scan, regions, order, split_page, hyph_new, side)
            # columns
            try:
                colpage = page_split_columns(splitter, split_page)
            except Exception as exc:
                self.stats["n_page_errors"] += 1
                self.error_types[f"columns:{type(exc).__name__}"] += 1
                self._note_error(scan_id, side, "columns", exc)
                colpage = None
        if colpage is not None:
            self._set_columns(regions, order, colpage, params)
        for key in order:
            region, _ = regions[key]
            for line in region.lines:
                if line.coords is None or _side_of(line, page_w) != side:
                    continue
                if _is_cand(line, params):
                    self.stats["n_candidates_after"] += 1

    def _note_error(self, scan_id, side, step, exc):
        if len(self.error_samples) < 50:
            self.error_samples.append({"scan_id": scan_id, "side": side, "step": step,
                                       "error": f"{type(exc).__name__}: {str(exc)[:120]}"})

    # ------------------------------------------------------------------
    def _write_back(self, scan, regions, order, split_page, hyph_new, side):
        """Replace merged lines of the scan's regions by their halves."""
        from pagexml.helper.pagexml_helper import copy_word
        new_by_region = {tr.id: tr.lines for tr in split_page.columns[0].text_regions}
        for key in order:
            region, old_lines = regions[key]
            new_lines = new_by_region.get(region.id)
            if new_lines is None or [l.id for l in new_lines] == [l.id for l in old_lines]:
                continue
            old_by_id = {ln.id: ln for ln in old_lines}
            pieces_by_old: Dict[str, list] = {}
            result_order: List[Any] = []   # ("keep", old_line) | ("pieces", old_id)
            j = 0
            i = 0
            n = len(new_lines)
            while i < len(old_lines):
                old = old_lines[i]
                if j < n and new_lines[j].id == old.id:
                    result_order.append(("keep", old))
                    i += 1
                    j += 1
                    continue
                # consume pieces until their joined text is the old line's text
                pieces = []
                target = _norm_spaces(_line_text(old))
                while j < n and new_lines[j].id not in old_by_id:
                    pieces.append(new_lines[j])
                    j += 1
                    if " ".join(_line_text(p) for p in pieces if _line_text(p)) == target:
                        break
                if not pieces or \
                        " ".join(_line_text(p) for p in pieces if _line_text(p)) != target:
                    # cannot align: keep the scan unchanged for this region
                    self.stats["n_alignment_failures"] += 1
                    if len(self.error_samples) < 50:
                        self.error_samples.append({
                            "scan_id": scan.id, "side": side, "step": "align", "error":
                            f"line {old.id}: {old.text!r} vs pieces {[p.text for p in pieces]!r}"})
                    result_order = None
                    break
                if any(not _line_text(p).strip() for p in pieces):
                    # degenerate split (the chosen split word is the first or last word): the
                    # piece without text is dropped and the line stays as it was
                    self.stats["n_degenerate_splits"] += 1
                    self._alias[old.id] = [p.id for p in pieces if _line_text(p).strip()][0]
                    result_order.append(("keep", old))
                    i += 1
                    continue
                result_order.append(("pieces", old, pieces))
                i += 1
            if result_order is None or j != n:
                if result_order is not None:
                    self.stats["n_alignment_failures"] += 1
                    if len(self.error_samples) < 50:
                        self.error_samples.append({
                            "scan_id": scan.id, "side": side, "step": "align",
                            "error": f"unconsumed pieces in region {region.id}"})
                continue
            replacement = {}
            for item in result_order:
                if item[0] == "pieces":
                    _, old, pieces = item
                    replacement[id(old)] = self._make_halves(old, pieces, hyph_new, copy_word)
            # lines of the other page side (same region) stay where they are
            region.lines = [x for ln in region.lines for x in replacement.get(id(ln), [ln])]

    def _make_halves(self, old, pieces, hyph_new, copy_word):
        n = len(pieces)
        if n == 2:
            suffixes = ["_L", "_R"]
        else:
            suffixes = [f"_p{k + 1}" for k in range(n)]
            self.stats["n_splits_more_than_two_pieces"] += 1
        kinds = ["hyphen" if p.id in hyph_new else "bigram" for p in pieces]
        kind = "hyphen" if all(k == "hyphen" for k in kinds) else "bigram"
        self.stats[f"n_split_{kind}"] += 1
        old_words = list(old.words or [])
        tokens = (_norm_spaces(old.text) or "").split(" ")
        can_words = len(old_words) == len(tokens)
        pos = 0
        out = []
        for k, (piece, suffix) in enumerate(zip(pieces, suffixes)):
            piece.id = f"{old.id}{suffix}"
            piece.conf = old.conf
            md = piece.metadata
            md["split_from"] = old.id
            md["edit_flags"] = f"split:{kind}"
            md["column"] = "1" if k == 0 else "2"
            if can_words:
                cnt = len((piece.text or "").split(" "))
                piece.words = [copy_word(w) for w in old_words[pos:pos + cnt]]
                pos += cnt
            out.append(piece)
        if self.record_splits:
            self.splits.append({"line_id": old.id, "kind": kind, "text": old.text,
                                "pieces": [p.text for p in pieces],
                                "box": [old.coords.box[c] for c in "xywh"]})
        return out

    # ------------------------------------------------------------------
    def _set_columns(self, regions, order, colpage, params):
        """Set ``column`` on every participating line from `page_split_columns`' output."""
        label: Dict[str, str] = {}
        cols = list(colpage.columns)
        if len(cols) == 2:
            names = ["1", "2"]
        elif len(cols) == 1:
            self.single_column_sides += 1
            left_right = params.text_left_boundary + params.default_line_width
            mid = left_right + params.column_sep_width / 2
            names = ["1" if (cols[0].coords.left + cols[0].coords.right) / 2 < mid else "2"]
        else:
            names = []
        for name, c in zip(names, cols):
            for ln in c.get_lines():
                label[ln.id] = name
        for tr in colpage.extra:
            for ln in tr.get_lines():
                label[ln.id] = "x"
        for key in order:
            region, _ = regions[key]
            for line in region.lines:
                md = line.metadata
                lab = label.get(line.id, label.get(self._alias.get(line.id)))
                if md.get("split_from"):
                    # halves keep the 1/2 of their position in the merged line
                    if lab is not None and lab != md.get("column"):
                        self.stats["n_halves_column_disagree"] += 1
                    if lab is not None:
                        self.stats["n_lines_with_column"] += 1
                        self.stats[f"n_column_{md['column']}"] += 1
                    continue
                if lab is not None:
                    md["column"] = lab
                    self.stats["n_lines_with_column"] += 1
                    self.stats[f"n_column_{lab}"] += 1

    # ------------------------------------------------------------------
    def stats_dict(self) -> Dict[str, Any]:
        s = self.stats
        other = {k.split(":", 1)[1]: v for k, v in s.items()
                 if k.startswith("n_candidates_other_tags:")}
        return {
            "n_lines_participating": s["n_lines_participating"],
            "n_candidates_before": s["n_candidates_before"],
            "n_split_hyphen": s["n_split_hyphen"], "n_split_bigram": s["n_split_bigram"],
            "n_candidates_after": s["n_candidates_after"],
            "n_page_errors": s["n_page_errors"], "error_types": dict(self.error_types),
            "n_candidates_other_tags": s["n_candidates_other_tags"],
            "n_candidates_other_tags_by_tag": other,
            "error_samples": self.error_samples,
            "n_lines_text_normalised": s["n_lines_text_normalised"],
            "n_degenerate_splits": s["n_degenerate_splits"],
            "n_alignment_failures": s["n_alignment_failures"],
            "n_splits_more_than_two_pieces": s["n_splits_more_than_two_pieces"],
            "n_lines_with_column": s["n_lines_with_column"],
            "n_column_1": s["n_column_1"], "n_column_2": s["n_column_2"],
            "n_column_x": s["n_column_x"],
            "n_sides_single_column": self.single_column_sides,
            "n_halves_column_disagree": s["n_halves_column_disagree"],
            "n_lines_side_without_params": s["n_lines_side_without_params"],
            "params": {side: (p.to_dict() if p else None)
                       for side, p in self.model.params.items()},
        }


def make_split_hook(model: SplitModel) -> Callable:
    """The scan hook for ``extract_inventory_v2(..., scan_hook=hook)``. A :class:`SplitHook`."""
    return SplitHook(model)


def extract_inventory_v2_split(inv, source, out_root, preset, *, lines_path=None, params=None,
                               record_splits=False):
    """Stage 1 (v2) with the merged-line splitter: fit the model, extract with the hook, and
    write ``split_model/<inv>.json`` and ``split_stats/<inv>.json`` under `out_root`.

    `params` (a dict side -> SplitParams) overrides the derived parameters (e.g. the notebook's)."""
    from pathlib import Path
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    from archival_structures.text_layer.io import write_json_atomic
    model = fit_split_model(inv, source, preset, lines_path=lines_path, params=params)
    hook = make_split_hook(model)
    hook.record_splits = record_splits
    result = extract_inventory_v2(inv, source, out_root, preset, scan_hook=hook)
    root = Path(out_root)
    write_json_atomic(root / "split_model" / f"{inv}.json", model.to_dict())
    stats = hook.stats_dict()
    write_json_atomic(root / "split_stats" / f"{inv}.json", stats)
    if record_splits:
        write_json_atomic(root / "split_stats" / f"{inv}.splits.json", hook.splits)
    result["split"] = {k: stats[k] for k in ("n_candidates_before", "n_split_hyphen",
                                              "n_split_bigram", "n_candidates_after",
                                              "n_page_errors")}
    return result
