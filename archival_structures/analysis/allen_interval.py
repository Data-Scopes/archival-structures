from collections import Counter

import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import horizontal_group_lines


ALLEN_INTERVALS = {
    'after', 'meets', 'o',
    'fi', 'di', 's', 'equal', 'si', 'd', 'f',
    'oi', 'met by', 'before'
}

EXTENDED_ALLEN_INTERVALS = {
    'after', 'meets',
    'mom', 'mol', 'lom', 'lol',
    'm', 'mfi', 'lfi',
    'ldi', 'cdi', 'rdi',
    'ms', 'ls', 'equal', 'lsi', 'msi',
    'ld', 'cd', 'rd', 'lf', 'mf',
    'momi', 'moli', 'lomi', 'loli',
    'met by', 'before'
}


def get_doc_allen_interval_profile(doc: pdm.PageXMLRegion, use_extended: bool = True):
    interval_func = get_extended_allen_interval if use_extended else get_allen_interval
    hor_group_lines = horizontal_group_lines(doc.get_lines())
    prev_group = None
    ai_freq = Counter()
    for ci, curr_group in enumerate(hor_group_lines):
        if prev_group is not None:
            ai_freq.update([interval_func(pl, cl) for cl in curr_group for pl in prev_group])
        if ci < len(hor_group_lines) - 1:
            next_group = hor_group_lines[ci+1]
            ai_freq.update([interval_func(cl, nl) for cl in curr_group for nl in next_group])
        prev_group = curr_group
    return ai_freq


def get_normalised_diff(x: int, y: int, max_diff: int = 0):
    diff = x - y
    return 0 if abs(diff) <= max_diff else diff


def determine_overlap_relation(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc):
    overlap = pdm.get_horizontal_overlap(region1, region2)
    if overlap == 0:
        return None, None
    relation1 = 'm' if overlap / region1.coords.width >= 0.5 else 'l'
    relation2 = 'm' if overlap / region2.coords.width >= 0.5 else 'l'
    return relation1, relation2


def get_normalised_region_diffs(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc) -> tuple[int, int, int, int]:
    if pdm.has_baseline(region1) and pdm.has_baseline(region2):
        # only use baseline when both have them, otherwise, use bounding boxes
        coords1 = region1.baseline
        coords2 = region2.baseline
    else:
        coords1, coords2 = region1.coords, region2.coords
    l1l2 = get_normalised_diff(coords1.left, coords2.left)
    l1r2 = get_normalised_diff(coords1.left, coords2.right)
    r1l2 = get_normalised_diff(coords1.right, coords2.left)
    r1r2 = get_normalised_diff(coords1.right, coords2.right)
    return l1l2, l1r2, r1l2, r1r2


def get_allen_interval(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc,
                       debug: int = 0):
    """Determine the (Extended) Allen Interval.

    Use `max_diff` to work with thick boundaries.

    :param region1: a PageXML document with spatial coordinates
    :param region2: a PageXML document with spatial coordinates
    :param debug: print debugging info
    """
    l1l2, l1r2, r1l2, r1r2 = get_normalised_region_diffs(region1, region2)
    overlap1, overlap2 = determine_overlap_relation(region1, region2)
    if debug > 0:
        print(f"allen_interval.get_extended_allen_interval\n\tl1: left: {pdm.coords_as_span(region1.coords)}")
        print(f"allen_interval.get_extended_allen_interval\n\tl2: left: {pdm.coords_as_span(region2.coords)}")
        print(f"allen_interval.get_extended_allen_interval\n\tl1l2: {l1l2}  r1r2: {r1r2}  "
              f"r1l2: {r1l2}\n\toverlap1: {overlap1}  overlap2: {overlap2}")
    if l1l2 < 0:
        if r1r2 < 0:
            if r1l2 < 0:
                # --
                #     --
                return 'after'
            elif r1l2 == 0:
                # ---
                #    ---
                return 'meets'
            else:
                # ---
                #  -----
                return 'o'
        elif r1r2 == 0:
            # ------
            #   ----
            return "fi"
        else:
            # ------
            #  --
            return 'di'
    elif l1l2 == 0:
        if r1r2 < 0:
            # ----
            # ------
            return 's'
        elif r1r2 == 0:
            # no shift, complete overlap (equal)
            # ------
            # ------
            return 'equal'
        else:
            # r1r2 > 0
            # ------
            # ----
            return 'si'
    else:
        # l1l2 > 0
        if r1r2 < 0:
            #  --
            # ------
            return 'd'
        elif r1r2 == 0:
            #   ----
            # ------
            return 'f'
        else:
            # r1r2 > 0
            if l1r2 < 0:
                #  -----
                # ---
                return 'oi'
            elif l1r2 == 0:
                #    ---
                # ---
                return 'met by'
            else:
                #     --
                # --
                return 'before'


def get_extended_allen_interval(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc,
                                debug: int = 0):
    """Determine the (Extended) Allen Interval.

    Use `max_diff` to work with thick boundaries.

    :param region1: a PageXML document with spatial coordinates
    :param region2: a PageXML document with spatial coordinates
    :param debug: print debugging info
    """
    l1l2, l1r2, r1l2, r1r2 = get_normalised_region_diffs(region1, region2)
    overlap1, overlap2 = determine_overlap_relation(region1, region2)
    if debug > 0:
        print(f"allen_interval.get_extended_allen_interval\n\tl1: left: {pdm.coords_as_span(region1.coords)}")
        print(f"allen_interval.get_extended_allen_interval\n\tl2: left: {pdm.coords_as_span(region2.coords)}")
        print(f"allen_interval.get_extended_allen_interval\n\tl1l2: {l1l2}  r1r2: {r1r2}  "
              f"r1l2: {r1l2}\n\toverlap1: {overlap1}  overlap2: {overlap2}")
    if l1l2 < 0:
        if r1r2 < 0:
            if r1l2 < 0:
                # --
                #    --
                return 'after'
            elif r1l2 == 0:
                # ---
                #    ---
                return 'meets'
            else:
                if overlap1 == 'l' and overlap2 == 'm':
                    # right-shift, shrink, most part 2 overlapped by least part 1 (lom)
                    # -----
                    #    ---
                    return 'lom'
                elif overlap1 == 'm' and overlap2 == 'l':
                    # right-shift, stretch, most part 1 overlaps least part 2 (mol)
                    # ---
                    #  -----
                    return 'mol'
                elif overlap1 == 'm' and overlap2 == 'm':
                    # right-shift, most part overlaps most part (mom)
                    # -----
                    #  -----
                    return 'mom'
                elif overlap1 == 'l' and overlap2 == 'l':
                    # right-shift, least part overlaps least part (lol)
                    # ----
                    #    ---
                    return 'lol'
                else:
                    raise ValueError(f"unexpected situation - l1l2: {l1l2}  r1r2: {r1r2}  "
                                     f"r1l2: {r1l2}  overlap1: {overlap1}  overlap2: {overlap2}")
        elif r1r2 == 0:
            if overlap1 == 'm':
                # right-shift, finished by, most part overlap (mfi)
                # ------
                #   ----
                return 'mfi'
            elif overlap1 == 'l':
                # right-shift, finished by, least part overlap (lfi)
                # ------
                #     --
                return "lfi"
            else:
                raise ValueError(f"unexpected situation - l1l2: {l1l2}  r1r2: {r1r2}  "
                                 f"r1l2: {r1l2}  l1r2: {l1r2}  "
                                 f"overlap1: {overlap1}  overlap2: {overlap2}")
        else:
            diff = get_normalised_diff(abs(l1l2), abs(r1r2))
            if diff < 0:
                # right-shift, contained by, right-aligned
                # ------
                #  --
                return 'ldi'
            elif diff == 0:
                # no shift, contained by, center-aligned
                # ------
                #  ----
                return 'cdi'
            else:
                # left-shift, contained by, left-aligned
                # ------
                #    --
                return 'rdi'
    elif l1l2 == 0:
        if r1r2 < 0:
            if overlap2 == 'm':
                # right-shift, starts, overlaps with most part (ms)
                # ----
                # ------
                return 'ms'
            elif overlap2 == 'l':
                # right-shift, starts, overlaps with least part (ls)
                # --
                # ------
                return 'ls'
            else:
                raise ValueError(f"unexpected situation - l1l2: {l1l2}  r1r2: {r1r2}  "
                                 f"r1l2: {r1l2}  l1r2: {l1r2}  "
                                 f"overlap1: {overlap1}  overlap2: {overlap2}")
        elif r1r2 == 0:
            # no shift, complete overlap (equal)
            # ------
            # ------
            return 'equal'
        else:
            # r1r2 > 0
            if overlap1 == 'm':
                # left-shift, starts, most part overlaps
                # ------
                # ----
                return 'msi'
            elif overlap1 == 'l':
                # left-shift, starts, least part overlaps
                # ------
                # --
                return 'lsi'
            else:
                raise ValueError(f"unexpected situation - l1l2: {l1l2}  r1r2: {r1r2}  "
                                 f"r1l2: {r1l2}  l1r2: {l1r2}  "
                                 f"overlap1: {overlap1}  overlap2: {overlap2}")
    else:
        # l1l2 > 0
        if r1r2 < 0:
            diff = get_normalised_diff(abs(l1l2), abs(r1r2))
            if diff < 0:
                # right-shift, contains, left-aligned
                #  --
                # ------
                return 'ld'
            elif diff == 0:
                # no shift, contains, center-aligned
                #  ----
                # ------
                return 'cd'
            else:
                # left-shift, contains, right-aligned
                #    --
                # ------
                return 'rd'
        elif r1r2 == 0:
            if overlap2 == 'm':
                # left-shift, finishes, most part overlap (mfi)
                #   ----
                # ------
                return 'mf'
            elif overlap2 == 'l':
                # left-shift, finishes, least part overlap (lfi)
                #     --
                # ------
                return 'lf'
            else:
                raise ValueError(f"unexpected situation - l1l2: {l1l2}  r1r2: {r1r2}  "
                                 f"r1l2: {r1l2}  l1r2: {l1r2}  "
                                 f"overlap1: {overlap1}  overlap2: {overlap2}")
        else:
            # r1r2 > 0
            if l1r2 < 0:
                if overlap1 == 'l' and overlap2 == 'm':
                    # left-shift, shrink, least part 2 overlapped by most part 1 (lomi)
                    #  -----
                    # ---
                    return 'lomi'
                elif overlap1 == 'm' and overlap2 == 'l':
                    # left-shift, stretch, least part 2 overlaps most part 1 (moli)
                    #    ---
                    # -----
                    return 'moli'
                elif overlap1 == 'm' and overlap2 == 'm':
                    # left-shift, most part overlaps most part (momi)
                    #  -----
                    # -----
                    return 'momi'
                elif overlap1 == 'l' and overlap2 == 'l':
                    # left-shift, least part overlaps least part (loli)
                    #    ---
                    # ----
                    return 'loli'
                else:
                    print(f"region 1: {region1.id} {region1.coords.box_string}")
                    print(f"region 2: {region2.id} {region2.coords.box_string}")
                    raise ValueError(f"unexpected situation - l1l2: {l1l2}  r1r2: {r1r2}  "
                                     f"r1l2: {r1l2}  l1r2: {l1r2}  "
                                     f"overlap1: {overlap1}  overlap2: {overlap2}")
            elif l1r2 == 0:
                #    ---
                # ---
                return 'met by'
            else:
                #     --
                # --
                return 'before'
