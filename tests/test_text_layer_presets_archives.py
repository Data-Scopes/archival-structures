import dataclasses
from unittest import TestCase

from archival_structures.text_layer.presets import PRESETS, VOC_1_04_02_V5SG

KEYS = ['na_1.05.06_v5sg', 'na_2.10.50_v5sg', 'na_2.10.01_v5sg', 'asdsaa_5023_v5sg', 'asdsaa_5075_v5sg',
        'asdsaa_89_v5sg', 'asnda_0114.11_v5sg']
ENTRIES = {'na_1.05.06_v5sg': ('NL-HaNA_1.05.06_44', '44'), 'na_2.10.50_v5sg': ('NL-HaNA_2.10.50_85', '85'),
           'na_2.10.01_v5sg': ('1090.zip', '1090'), 'asdsaa_5023_v5sg': ('NL-AsdSAA_5023_4', '4'),
           'asdsaa_5075_v5sg': ('NL-AsdSAA_5075_2409', '2409'), 'asdsaa_89_v5sg': ('NL-AsdSAA_89_3.1', '3.1'),
           'asnda_0114.11_v5sg': ('NL-AsnDA_0114.11_1', '1')}
DIFF = {'name', 'archive_id', 'scan_id_prefix', 'source_pattern', 'description'}


class TestArchivePresets(TestCase):

    def test_keys_registered(self):
        for k in KEYS:
            self.assertIn(k, PRESETS)
        self.assertIn('voc_1.04.02_v5sg', PRESETS)

    def test_differ_only_in_plumbing(self):
        base = dataclasses.asdict(VOC_1_04_02_V5SG)
        for k in KEYS:
            with self.subTest(k):
                p = dataclasses.asdict(PRESETS[k])
                self.assertEqual({f: v for f, v in base.items() if f not in DIFF},
                                 {f: v for f, v in p.items() if f not in DIFF})
                self.assertNotEqual(base['archive_id'], p['archive_id'])

    def test_patterns_fullmatch_entries(self):
        import re
        for k, (entry, inv) in ENTRIES.items():
            with self.subTest(k):
                m = re.fullmatch(PRESETS[k].source_pattern, entry)
                self.assertIsNotNone(m)
                self.assertEqual(inv, m.group('inv'))

    def test_patterns_reject_appledouble(self):
        import re
        self.assertIsNone(re.fullmatch(PRESETS['asdsaa_5023_v5sg'].source_pattern, '._NL-AsdSAA_5023_4'))
        self.assertIsNone(re.fullmatch(PRESETS['na_1.05.06_v5sg'].source_pattern, '._NL-HaNA_1.05.06_44'))

    def test_names_and_prefixes(self):
        self.assertEqual('NL-HaNA_1.05.06_text_layer_v5sg', PRESETS['na_1.05.06_v5sg'].name)
        self.assertEqual('NL-HaNA_2.10.01', PRESETS['na_2.10.01_v5sg'].scan_id_prefix)
        self.assertIsNone(PRESETS['asnda_0114.11_v5sg'].scan_id_prefix)
