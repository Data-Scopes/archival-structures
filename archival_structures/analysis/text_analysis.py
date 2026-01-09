import re
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as data_class_field
from string import punctuation
from typing import Dict, List, Set, Union

import pagexml.model.physical_document_model as pdm
from fuzzy_search.tokenization.token import Tokenizer

import archival_structures.analysis.token_analysis as token_analysis


def char_is_punct(char: str, punct_tokens: Set[str] = punctuation):
    return char in punct_tokens


@dataclass
class CharStats:

    num_chars: int = 0
    num_chars_numeric: int = 0
    num_chars_alpha: int = 0
    num_chars_alphanum: int = 0
    num_chars_punct: int = 0
    num_chars_spaces: int = 0


class CharAnalyser:

    def __init__(self, punct_tokens: Union[str, Set[str]] = None):
        if punct_tokens is None:
            punct_tokens = set(punctuation)
        elif isinstance(punct_tokens, str):
            punct_tokens = set(punct_tokens)
        self.punct_tokens = punct_tokens

    def analyse_chars(self, chars: str, char_stats: CharStats = None):
        if char_stats is None:
            char_stats = CharStats()
        char_stats.num_chars += len(chars)
        for char in chars:
            if char.isalnum():
                char_stats.num_chars_alphanum += 1
            if char.isnumeric():
                char_stats.num_chars_numeric += 1
            if char.isalpha():
                char_stats.num_chars_alpha += 1
            if char.isspace():
                char_stats.num_chars_spaces += 1
            if char_is_punct(char, self.punct_tokens):
                char_stats.num_chars_punct += 1
        return char_stats


def line_is_para_line_full(line: pdm.PageXMLTextLine, min_chars: int = 25):
    if line.text is None:
        return False
    alpha_chars = [char for char in line.text if char.isalnum()]
    return len(alpha_chars) >= min_chars


def get_num_dots_tokens(text: str):
    if text is None:
        return False
    return len(list(re.finditer(r"(\. ?){3,}", text)))


@dataclass
class TokenStats:

    num_tokens: int = 0
    num_tokens_word: int = 0
    num_tokens_punct: int = 0
    num_tokens_number: int = 0
    num_tokens_title: int = 0
    num_tokens_upper: int = 0
    num_tokens_dots: int = 0
    num_tokens_wordlist: Dict[str, int] = data_class_field(default_factory=dict)


def init_token_stats(wordlist: Dict[str, Set[str]]):
    token_stats = TokenStats()
    for word_type in wordlist:
        token_stats.num_tokens_wordlist[word_type] = 0
    return token_stats


def optimise_wordlist(wordlist: Dict[str, Union[List[str], Set[str]]] = None):
    new_wordlist: Dict[str, Set[str]] = {}
    if wordlist is None:
        return defaultdict(set)
    for word_type in wordlist:
        new_wordlist[word_type] = set(wordlist[word_type])
    return new_wordlist


class TokenAnalyser:

    def __init__(self, wordlist: Dict[str, Union[List[str], Set[str]]] = None,
                 punct_tokens: Union[List[str], Set[str]] = None, number_words: Union[List[str], Set[str]] = None):
        self.wordlist = optimise_wordlist(wordlist)
        self.punct_tokens = set(punct_tokens) if punct_tokens is not None else set(punctuation)
        self.number_words = set(number_words) if number_words is not None else set()

    def analyse_tokens(self, tokens: List[str], token_stats: TokenStats = None):
        if token_stats is None:
            token_stats = init_token_stats(self.wordlist)
        token_stats.num_tokens += len(tokens)
        for token in tokens:
            if token.isalnum():
                token_stats.num_tokens_word += 1
                if token.istitle():
                    token_stats.num_tokens_title += 1
                if token.isupper():
                    token_stats.num_tokens_upper += 1
            if token_analysis.token_is_number(token, self.number_words):
                token_stats.num_tokens_number += 1
            if token_analysis.token_is_punct(token, self.punct_tokens):
                token_stats.num_tokens_punct += 1
            for word_type in self.wordlist:
                if token in self.wordlist[word_type]:
                    token_stats.num_tokens_wordlist[word_type] += 1
        return token_stats


@dataclass
class TextStats(CharStats, TokenStats):

    @property
    def dict(self):
        data_dict = self.__dict__
        data = {}
        for field in data_dict:
            if field == 'num_tokens_wordlist':
                for word_type in data_dict[field]:
                    field_ext = f"{field}_{word_type}"
                    data[field_ext] = data_dict[field][word_type]
            else:
                data[field] = data_dict[field]
        return data


class TextAnalyser:

    def __init__(self, tokenizer: Tokenizer = None, wordlist: Dict[str, Union[List[str], Set[str]]] = None,
                 punct_tokens: Union[List[str], Set[str]] = None,
                 number_words: Union[List[str], Set[str]] = None):
        self.tokenizer = tokenizer if tokenizer is not None else Tokenizer(remove_punctuation=False)
        self.token_analyser = TokenAnalyser(wordlist, number_words=number_words)
        self.char_analyser = CharAnalyser(punct_tokens=punct_tokens)
        self.wordlist = optimise_wordlist(wordlist)
        self.punct_tokens = set(punct_tokens) if punct_tokens is not None else set()
        self.number_words = set(number_words) if number_words is not None else set()

    def _init_text_stats(self):
        text_stats = TextStats()
        for list_name in self.wordlist:
            text_stats.num_tokens_wordlist[list_name] = 0
        return text_stats

    def analyse_text(self, text: Union[str, pdm.PageXMLDoc]):
        text_stats = self._init_text_stats()
        if isinstance(text, str):
            texts = [text]
        elif isinstance(text, pdm.PageXMLTextLine):
            texts = [text.text] if text.text is not None else ['']
        elif isinstance(text, pdm.PageXMLRegion):
            lines = text.get_lines()
            texts = [line.text if line.text is not None else '' for line in lines]
        else:
            raise TypeError(f"text passed to TextAnalyser.analyse_text must string or PageXMLTextLine")
        for text in texts:
            self.char_analyser.analyse_chars(chars=text, char_stats=text_stats)
            tokens = [token.n for token in self.tokenizer.tokenize(text)]
            self.token_analyser.analyse_tokens(tokens=tokens, token_stats=text_stats)
            text_stats.num_tokens_dots = get_num_dots_tokens(text)
        return text_stats

    def classify_line_relation(self, line1: pdm.PageXMLTextLine, line2: pdm.PageXMLTextLine):
        """Determine if line1 is left, right, above or below line2, touching or with gap."""
        relation = None
        if pdm.is_horizontally_overlapping(line1, line2):
            if line1.coords.right < line2.coords.left:
                hor_pos1, hor_pos2 = 'left', 'right'
            elif line1.coords.left < line2.coords.right:
                hor_pos1, hor_pos2 = 'right', 'left'
            else:
                hor_overlap = pdm.get_horizontal_overlap_ratio(line1, line2)
                overlap_ratio1 = hor_overlap / line1.coords.width
                overlap_ratio2 = hor_overlap / line2.coords.width
                hor_pos1, hor_pos2 = 'right', 'left'
        return relation


def get_line_width(line: pdm.PageXMLTextLine):
    if line.baseline is not None and line.baseline.points is not None:
        return line.baseline.width
    elif line.coords is not None and line.coords.points is not None:
        return line.coords.width
    else:
        return None
