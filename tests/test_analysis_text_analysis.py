from string import punctuation
from unittest import TestCase

import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.text_analysis import CharAnalyser
from archival_structures.analysis.text_analysis import TextAnalyser
from archival_structures.utils.wordlist import read_wordlist


def make_line(text: str):
    line = pdm.PageXMLTextLine(text=text)
    return line


class TestCharAnalyser(TestCase):

    def setUp(self) -> None:
        self.analyser = CharAnalyser()

    def test_char_analyser_counts_spaces(self):
        text = 'text with some spaces'
        char_stats = self.analyser.analyse_chars(text)
        self.assertEqual(3, char_stats.num_chars_spaces)

    def test_char_analyser_counts_alpha(self):
        text = 'text with some spaces'
        char_stats = self.analyser.analyse_chars(text)
        self.assertEqual(18, char_stats.num_chars_alpha)

    def test_char_analyser_counts_digits(self):
        text = 'text with 4 spaces'
        char_stats = self.analyser.analyse_chars(text)
        self.assertEqual(1, char_stats.num_chars_numeric)

    def test_char_analyser_counts_punctuation(self):
        text = 'text with 4 spaces'
        char_stats = self.analyser.analyse_chars(text)
        self.assertEqual(0, char_stats.num_chars_punct)

    def test_char_analyser_can_set_punctuation(self):
        text = 'text with some dots ...'
        analyser = CharAnalyser(punct_tokens=',')
        char_stats = analyser.analyse_chars(text)
        self.assertEqual(0, char_stats.num_chars_punct)


class TestLineAnalyser(TestCase):

    def setUp(self) -> None:
        wordlist = read_wordlist('resources/wordlists/wordlist-lang_nl-period_early_modern.tsv')
        number_words = ['one', 'two', 'three']
        punct_tokens = [punct for punct in punctuation] + ['„']
        non_content_words = set()
        word_types = ['ADP', 'PD', 'CONJ', 'PC']
        word_lists = {'det': ['some'], 'noun': ['tokens']}
        for word_type in word_types:
            non_content_words.update(wordlist[word_type])
        self.line_analyser = TextAnalyser(wordlist=word_lists, number_words=number_words,
                                          punct_tokens=punct_tokens)

    def test_line_as_text(self):
        line = make_line(" some tokens")
        token_stats = self.line_analyser.analyse_text(line.text)
        self.assertEqual(2, token_stats.num_tokens)

    def test_line_with_dots(self):
        line = make_line(" some tokens with dots ... and spaced . . .")
        token_stats = self.line_analyser.analyse_text(line.text)
        self.assertEqual(2, token_stats.num_tokens_dots)

    def test_line_as_line(self):
        line = make_line(" some tokens")
        token_stats = self.line_analyser.analyse_text(line)
        self.assertEqual(2, token_stats.num_tokens)

    def test_line_with_wordlist_words(self):
        line = make_line(" some tokens")
        token_stats = self.line_analyser.analyse_text(line)
        self.assertEqual(2, token_stats.num_tokens)

    def test_region(self):
        line1 = make_line("some tokens")
        line2 = make_line("more tokens")
        region = pdm.PageXMLTextRegion(lines=[line1, line2])
        token_stats = self.line_analyser.analyse_text(region)
        self.assertEqual(4, token_stats.num_tokens_word)
