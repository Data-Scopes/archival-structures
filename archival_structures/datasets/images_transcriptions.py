"""Fixture data for this package's own tests: paths to the repo's `data/` directory, and
hand-picked `test_pairs`/`test_images` lists (image + PageXML path, with known properties like
`inverted` or a `description`) used as small real-data regression checks across the test suite
(e.g. `archival_structures.analysis.opening_detection`'s opening/non-opening examples).

Some entries are known-stale (the underlying files moved to a different inventory number with
non-corresponding filenames) and have been left unfixed rather than guessed at -- see the
`NL-AsdSAA_89_14_KLAB08969` entries below.
"""

from pathlib import Path

import archival_structures

MODULE_DIR = Path(archival_structures.__path__[0])
PACKAGE_DIR = (MODULE_DIR / '../').resolve()
DATA_DIR = PACKAGE_DIR / 'data'
IMAGE_DIR = DATA_DIR / 'thumbs'
PAGEXML_DIR = DATA_DIR / 'PageXML'


test_images = [
    {
        'image_path': IMAGE_DIR / 'NAA_A2478' / 'NAA_ItemNumber1194430'/ 'NAA_ItemNumber1194430-page-4.png',
        'inverted': False,
        'num_docs': 1,
        'has_border': True,
        'script_types': ['printed', 'handwritten', 'typed']
    },
    {
        'image_path': IMAGE_DIR / 'NAA_A2478' / 'NAA_ItemNumber1194442'/ 'NAA_ItemNumber1194442-page-26.png',
        'inverted': False,
        'num_docs': 1,
        'has_border': True,
        'script_types': ['printed', 'handwritten', 'typed']
    },
    {
        'image_path': IMAGE_DIR / 'NAA_A2478' / 'NAA_ItemNumber1194442'/ 'NAA_ItemNumber1194442-page-35.png',
        'inverted': False,
        'num_docs': 2,
        'has_border': True,
        'script_types': ['printed', 'handwritten', 'typed']
    },
    {
        'image_path': IMAGE_DIR / 'NAA_A2478' / 'NAA_ItemNumber1194442'/ 'NAA_ItemNumber1194442-page-36.png',
        'inverted': False,
        'num_docs': 2,
        'has_border': True,
        'script_types': ['printed', 'handwritten', 'typed']
    },
]

test_pairs = [
    {
        'image_path': IMAGE_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1' / 'thumb-width_300-scan-NL-AsnDA_0114.11_1_0002.jp2.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1' / 'NL-AsnDA_0114.11_1_0002.xml',
        'description': 'early modern, handwritten, two pages, two columns'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1' / 'thumb-width_300-scan-NL-AsnDA_0114.11_1_0005.jp2.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1' / 'NL-AsnDA_0114.11_1_0005.xml',
        'description': 'early modern, handwritten, two pages, one columns'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1' / 'thumb-width_300-scan-NL-AsnDA_0114.11_1_0026.jp2.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1' / 'NL-AsnDA_0114.11_1_0026.xml',
        'description': 'early modern, handwritten, two pages, two columns, red seal'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0001_00001.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0001_00001.xml',
        'description': 'modern, printed, one page, almost empty, cover page (yellow/brown) with attached paper slip (beige) in top right corner'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0002_00002.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0002_00002.xml',
        'description': 'modern, handwritten, one page, one column, letter with letter head, grey pencil, red pen scribble'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0008_00008.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0008_00008.xml',
        'description': 'modern, printed, two pages, one columns, cover page (verso) and print page (recto)'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0027_00027.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0027_00027.xml',
        'description': 'modern, printed/handwritten, one page, one columns, letter with letter head and logo, grey pencil, red pen scribble, black stamp'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0029_00029.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0029_00029.xml',
        'description': 'modern, printed, one page, drawing, dark blue background, design with writing'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0039_00039.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0039_00039.xml',
        'description': 'modern, printed/typed, one page, one column, printing in black, typing in blue and red, scribbles in multiple colours'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0047_00047.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0047_00047.xml',
        'description': 'modern, printed, one page, form with handwritten notes and stamp, logo in orange (top right), multiple colours of pen'
    },
    {
        'image_path': IMAGE_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / 'thumb-width_600-scan-0051_00051.jpg.png',
        'pagexml_path': PAGEXML_DIR / 'NL-AsdSAA' / 'NL-AsdSAA_89_14' / 'NL-AsdSAA_89_14_KLAB08969' / '0051_00051.xml',
        'description': 'modern, printed, one page, form with handwritten notes and stamp, logo in orange (top right), multiple colours of pen'
    },
]