import importlib.util
import math
import re
from pathlib import Path
from unittest import TestCase, skipUnless

from archival_structures.analysis import listlike as L

SHARED = Path('/Users/Shared/claude_projects/data_scopes/shared-tools/listlike.py')

FIXTURE_TEXTS = [
    ("a", ["Item betaelt aen Jan Pietersz. 25 gl.", "Item aen Cornelis voor hout 120 gl.", "Voor hout .... 120",
           "Voor yser gl. 33", "iv. Voor steen", "Anno 1650", "den 3 Maart 1700"]),
    ("b", ["Paye aux ouvriers 300 livres", "Pour le bois 45 sols", "Fait le 4 mai 1699", "IX. Pour le fer",
           "Voor loon fol. 12", "Dese brief is gesloten ende gesegelt."]),
]


def _fixture():
    rows = []
    for g, texts in FIXTURE_TEXTS:
        for i, t in enumerate(texts):
            rows.append(dict(text=t, x0=(0.0 if i % 3 else 5.0), x1=100.0 - (11 * i) % 50, group=g))
    return rows


def _load_shared():
    spec = importlib.util.spec_from_file_location('shared_listlike', SHARED)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _same(a, b):
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b


def _selftest_docs():
    prose = ["Op heden is by haer Hoog Mogende vergaderd geweest de", "heeren Gedeputeerden, die hebben gehoort het rapport van", "den Raedt, ende daerop gedelibereert zynde, is goetgevonden",
             "dat men de voorsz. Commissie sal verlenen, mits dat de", "selve binnen drie maenden sal worden uitgevoert ende", "daervan rapport sal gedaen worden aen haer Ho: Mo:", "ende dat de Griffier daervan sal ontfangen eene copie"]
    nums = ["1. Voor het leveren van hout", "2. Voor het maken van een brugge", "3. Voor het verven van de poort", "4. Voor het repareren van de sluys", "5. Voor het graven van een gracht", "6. Voor het schoonmaken van de haven", "7. Voor het betalen van den wacht"]
    table = ["Aen Jan Pietersz. voor hout gl. 120", "Aen Cornelis Jansz. voor steen gl. 45", "Aen Willem de Vries voor kalck gl. 33", "Aen Hendrick Claesz. voor yser gl. 210", "Aen Dirck Gerritsz. voor loon gl. 18", "Aen Pieter Adriaensz. voor teer gl. 9", "Aen Claes Jacobsz. voor tou gl. 64"]
    names = ["Jan Pietersz. van Beuningen", "Cornelis de Witt", "Willem van Oranje", "Hendrick Casimir", "Dirck van Hoorn", "Pieter de Groot", "Claes van Rijn"]
    docs = {"prose": L._doc(prose), "numbered list": L._doc(nums), "table": L._doc(table), "names": L._doc(names, width=60.0)}
    for k, d in docs.items():
        for r in d:
            r["flags"] = {"name": 1.0 if k == "names" else 0.0}
    return docs


RULE4 = {"conditions": [("cap_share", ">=", 0.8), ("numamt_share", ">=", 0.3), ("short_share", ">=", 0.5), ("name_win", ">=", 0.5)], "min": 2}


class TestListlike(TestCase):

    def test_selftest_runs(self):
        L.selftest()

    def test_selftest_mean_scores(self):
        got = {}
        for k, d in _selftest_docs().items():
            s = L.list_score(L.window_features(d), RULE4)
            got[k] = round(sum(s) / len(s), 2)
        self.assertEqual({"prose": 0.14, "numbered list": 1.14, "table": 2.14, "names": 2.57}, got)

    @skipUnless(SHARED.exists(), 'shared-tools listlike absent')
    def test_equivalence_features(self):
        S = _load_shared()
        docs = list(_selftest_docs().values()) + [_fixture()]
        for d in docs:
            a, b = L.window_features(d), S.window_features(d)
            self.assertEqual(len(a), len(b))
            for fa, fb in zip(a, b):
                self.assertEqual(set(fa), set(fb))
                for k in fa:
                    self.assertTrue(_same(fa[k], fb[k]), (k, fa[k], fb[k]))
        self.assertEqual(S.DEFAULT_CFG, L.DEFAULT_CFG)
        self.assertEqual(S.DEFAULT_RULE, L.DEFAULT_RULE)

    @skipUnless(SHARED.exists(), 'shared-tools listlike absent')
    def test_equivalence_scores_all_cells(self):
        S = _load_shared()
        fx = _fixture()
        feats = S.window_features(fx)
        for name, (cfg, rule) in L.LISTLIKE_CELLS.items():
            with self.subTest(name):
                self.assertEqual(S.list_score(feats, rule), L.list_score(L.window_features(fx), rule))
                self.assertEqual(S.list_flag(feats, rule), L.list_flag(L.window_features(fx), rule))
        # C1 differs from C0 only in cfg
        self.assertEqual(['amount_end'], list(L.LISTLIKE_CFG_C1))

    def test_c1_amount_end(self):
        rx = re.compile(L.LISTLIKE_CFG_C1['amount_end'], re.I)
        for t in ("den 3 Maart 1700", "Anno 1650"):
            self.assertIsNone(rx.search(t.strip()), t)
        for t in ("Voor hout .... 120", "Voor hout gl. 120", "fol. 12", "120 livres", "Item betaelt 25 gl."):
            self.assertIsNotNone(rx.search(t.strip()), t)

    def test_c1_flags_fewer_lines(self):
        fx = _fixture()
        c0 = L.list_flag(L.window_features(fx, cfg=None), L.DEFAULT_RULE)
        c1 = L.list_flag(L.window_features(fx, cfg=L.LISTLIKE_CFG_C1), L.DEFAULT_RULE)
        self.assertLessEqual(sum(c1), sum(c0))

    def test_rules(self):
        self.assertEqual(2, L.RULE_R1['min'])
        self.assertEqual(3, L.RULE_R2['min'])
        self.assertIn(("closed_share", "<=", 0.4), L.RULE_R1['conditions'])
        self.assertEqual(6, len(L.LISTLIKE_CELLS))

    def test_group_shares(self):
        self.assertEqual({'a': 0.5, 'b': 1.0}, L.group_shares([True, False, True], ['a', 'a', 'b']))
        self.assertEqual({}, L.group_shares([], []))
        with self.assertRaises(ValueError):
            L.group_shares([True], [])
