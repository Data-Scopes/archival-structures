from collections import defaultdict, Counter
from typing import Dict, List

import pagexml.helper.pagexml_helper as pagexml_helper
import pagexml.model.physical_document_model as pdm


def get_height_profile(scans: List[pdm], interval_width: int = 5, doc_indent: int = 25):
    """Determine the text height profile along the X-axis of a set of scans. For each scan,
    the height of the text lines at regular intervals along the X-axis is computed. These
    height profiles can be used to determine:

        - where a scan has empty columns along its X axis (gaps between columns on a page or
          between pages on a scan
        - whether a set of scans consists of book openings (two pages per scan).

    The line_indent can be used to capture the fact that there are two horizontally
    separable sets of lines that overlap only minimally because e.g. their baselines extend
    beyond the first and/or last text character. By indenting the lines, it is measurable
    that the sets of lines have non-overlapping centres.

       ------
            ------
       ------
            ------
       ------
            ------
       ------
            ------

    In the above example, there are two sets of lines, forming two separate columns. By
    indenting the lines at both ends, the two columns become horizontally separated, making
    them splittable.

        ----
             ----
        ----
             ----
        ----
             ----
        ----
             ----

    :param scans: a list of PageXMLScan objects
    :param interval_width: the interval at which to measure the height profile (default is 5,
                           i.e. at every 5 pixels)
    :param doc_indent: the amount of pixels to reduce from a doc's width at both ends.
    """
    height_profile = defaultdict(Counter)
    for scan in scans:
        for x in range(0, scan.coords.width + 1, interval_width):
            height_profile[scan.id][x] = 0
        for line in scan.get_lines():
            height = line.xheight if line.xheight is not None else line.coords.height / 2
            left, right = pagexml_helper.get_doc_indent_left_right(line, doc_indent=doc_indent)
            left_inter = int(left / interval_width) * interval_width
            for x in range(left_inter, right + 1, interval_width):
                height_profile[scan.id][x] += height
    return height_profile


def get_separation_point(scan: pdm.PageXMLScan, height_profile: Dict[str, Dict[int, float]],
                         interval_width: int = 5, window_size: int = 200):
    """Find the most central x-coordinate where there is no text (a gap between text columns).
    If there is no point where there is no text, return the point in the central window of a
    scan that has the lowest text height. The size of the central window is determined by the
    `window_size` parameter (default is 200)."""
    mid_point = scan.coords.width / 2
    mid_point_inter = int(mid_point / interval_width) * interval_width
    mid_min_x, min_text_height = None, None
    separation_point = None
    empty_mid = False
    for x in range(mid_point_inter - window_size, mid_point_inter + window_size + 1, interval_width):
        if min_text_height is None or height_profile[scan.id][x] < min_text_height:
            mid_min_x = x
            min_text_height = height_profile[scan.id][x]
        if height_profile[scan.id][x] == 0:
            empty_mid = True
            if separation_point is None:
                separation_point = x
            elif abs(x - mid_point) < abs(separation_point - mid_point):
                separation_point = x
    if empty_mid is False:
        separation_point = mid_min_x
    return pdm.Point(separation_point, min_text_height)


def get_separation_points(scans: List[pdm.PageXMLScan], height_profile: Dict[str, Dict[int, float]],
                          interval_width: int = 5, window_size: int = 200):
    """Find the separation points for a set of scans. The separation point of a scan is the
    most central x-coordinate where there is no text (a gap between text columns), or where
    the text height is lowest in the central window of the scan. The size of the central
    window is determined by the `window_size` parameter (default is 200). """
    separation_point = {}
    for scan in scans:
        separation_point[scan.id] = get_separation_point(scan, height_profile, interval_width=interval_width,
                                                         window_size=window_size)
    return separation_point
