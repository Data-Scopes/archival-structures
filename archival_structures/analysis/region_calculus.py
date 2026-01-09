import pagexml.model.physical_document_model as pdm


def get_start_end_overlap(start1: int, start2: int, end1: int, end2: int):
    max_start = max(start1, start2)
    min_end = min(end1, end2)
    return max(0, min_end - max_start)


def get_start_end_relation(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc,
                           start1: int, start2: int, end1: int, end2: int,
                           max_diff: int = 0, debug: int = 0):
    """Determine the region relation of two regions, using region connection
    calculus (RCC).

    Use `max_diff` to work with thick boundaries.

    :param region1: a PageXML document with spatial coordinates
    :param region2: a PageXML document with spatial coordinates
    :param start1: the start (left or top) of region1
    :param start2: the start (left or top) of region2
    :param end1: the end (right or bottom) of region1
    :param end2: the end (right or bottom) of region2
    :param max_diff: the maximum difference in pixels to be considered connected
    :param debug: print debugging info
    """
    overlap = get_start_end_overlap(start1, start2, end1, end2)
    if debug > 0:
        print(f"region_calculus.get_start_end_relation - overlap: {overlap}")
    if overlap >= min(region1.coords.width, region2.coords.width):
        # full overlap -> equal (EQ) or proper part (PP)
        start_diff = start1 - start2
        end_diff = end1 - end2
        if abs(start_diff) <= max_diff and abs(end_diff) <= max_diff:
            return 'EQ'
        elif abs(start_diff) <= max_diff:
            # tangential / touching
            return 'TPPi' if end_diff > 0 else 'TPP'
        elif abs(end_diff) <= max_diff:
            # tangential / touching
            return 'TPP' if start_diff > 0 else 'TPPi'
        else:
            return 'NTPP' if start_diff > 0 else 'NTPPi'
    elif overlap > max_diff:
        return 'PO'
    else:
        max_start = max(region1.coords.left, region2.coords.left)
        min_end = min(region1.coords.right, region2.coords.right)
        if max_start - min_end <= max_diff:
            return 'EC'
        else:
            return 'DC'


def get_horizontal_region_relation(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc,
                                   max_diff: int = 0, debug: int = 0) -> str:
    relation = get_start_end_relation(region1, region2, max_diff=max_diff,
                                      start1=region1.coords.left, start2=region2.coords.left,
                                      end1=region1.coords.right, end2=region2.coords.right, debug=debug)
    return relation


def get_vertical_region_relation(region1: pdm.PageXMLDoc, region2: pdm.PageXMLDoc,
                                 max_diff: int = 0, debug: int = 0) -> str:
    return get_start_end_relation(region1, region2, max_diff=max_diff,
                                  start1=region1.coords.top, start2=region2.coords.top,
                                  end1=region1.coords.bottom, end2=region2.coords.bottom, debug=debug)
