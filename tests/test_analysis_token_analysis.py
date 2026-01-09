from string import punctuation
from unittest import TestCase

import archival_structures.analysis.token_analysis as token_analysis
from archival_structures.utils.wordlist import read_wordlist


class TestTokenAnalyser(TestCase):

    def setUp(self) -> None:
        wordlist = read_wordlist('resources/wordlists/wordlist-lang_nl-period_early_modern.tsv')
        self.number_words = set(['one', 'two', 'three'])
        self.punct_tokens = set([punct for punct in punctuation] + ['„'])
        self.non_content_words = set()
        word_types = ['ADP', 'PD', 'CONJ', 'PC']
        for word_type in word_types:
            self.non_content_words.update(wordlist[word_type])

    def test_digit_number(self):
        token = token_analysis.token_is_number('512')
        self.assertEqual(True, token)

    def test_number_word(self):
        token = token_analysis.token_is_number('three', number_words=self.number_words)
        self.assertEqual(True, token)

    def test_number_comma_separator(self):
        token = token_analysis.token_is_number('512,12')
        self.assertEqual(True, token)

    def test_number_dot_separator(self):
        token = token_analysis.token_is_number('512.42')
        self.assertEqual(True, token)

    def test_number_dot_separator_comma_decimal(self):
        token = token_analysis.token_is_number('512.424,46')
        self.assertEqual(True, token)

    def test_number_comma_separator_dot_decimal(self):
        token = token_analysis.token_is_number('512,592.42')
        self.assertEqual(True, token)

    def test_punctuation_base(self):
        token = token_analysis.token_is_punct('.')
        self.assertEqual(True, token)

    def test_punctuation_extension(self):
        token = token_analysis.token_is_punct('„', punct_tokens=self.punct_tokens)
        self.assertEqual(True, token)

    def test_content_words(self):
        is_content = token_analysis.token_is_content_word('bicycle')
        self.assertEqual(True, is_content)

    def test_content_words_uses_non_content_words(self):
        ncw = list(self.non_content_words)[0]
        is_content = token_analysis.token_is_punct(ncw)
        self.assertEqual(False, is_content)
