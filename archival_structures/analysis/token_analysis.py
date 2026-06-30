"""Token-level classification helpers (punctuation, numbers, content words) used by
`archival_structures.analysis.text_analysis`'s `TokenAnalyser` to build per-line/region token
statistics.
"""

import re
from string import punctuation
from typing import Dict, Iterable, List, Set, Union


REPEAT_SYMBOLS = {',,', '„'}


def token_in_wordlists(token: str, wordlist: Dict[str, Set[str]]):
    """Return all word types for which the token occur in the list of words of that type."""
    return [word_type for word_type in wordlist if token in wordlist[word_type]]


def token_is_punct(token: str, punct_tokens: Set[str] = None) -> bool:
    """True if `token` is a punctuation character/string, or made up entirely of one (using
    `string.punctuation` if `punct_tokens` isn't given)."""
    if punct_tokens is None:
        punct_tokens = punctuation
    if token in punct_tokens:
        return True
    if all(token_char in punct_tokens for token_char in token):
        return True
    return False


def token_is_number(token: str, number_words: Set[str] = None) -> bool:
    """True if `token` is a digit string, a number written out as words (if `number_words` is
    given), or matches a simple decimal/thousands-separated number pattern."""
    if token.isdigit():
        return True
    if number_words is not None and token in number_words:
        return True
    if re.match(r"\d+([,.]\d+)*", token):
        return True
    return False


def token_is_content_word(token: str, non_content_words: Set[str] = None) -> bool:
    """True if `token` isn't in `non_content_words` (stopwords/function words); true for
    every token if `non_content_words` isn't given."""
    if non_content_words is None:
        return True
    return token not in non_content_words


def token_is_repeat_symbol(token: str) -> bool:
    """True if `token` is a ditto-style repeat symbol (see `REPEAT_SYMBOLS`)."""
    return token in REPEAT_SYMBOLS


class Token:
    """A token with its word-type membership and (optionally precomputed) classification
    flags -- `is_punct`/`is_number`/`is_content`/`is_stopword`."""

    def __init__(self, token: str, word_types: Union[str, List[str]], is_punct: bool = None,
                 is_number: bool = None, is_content: bool = None, is_stopword: bool = None):
        self.token = token
        self.word_types = word_types if isinstance(word_types, Iterable) else list(word_types)
        self.is_punct = is_punct
        self.is_number = is_number
        self.is_content = is_content
        self.is_stopword = is_stopword

    def __repr__(self):
        return (f"{self.__class__.__name__}(token='{self.token}', word_type={self.word_types}\n"
                f"{' '*len(self.__class__.__name__)} is_punct={self.is_punct} is_number={self.is_number}\n"
                f"{' '*len(self.__class__.__name__)} is_content={self.is_content} is_stopword={self.is_stopword})")


def tokens_are_running_text(tokens: List[Token], min_tokens: int = 2) -> bool:
    """True if `tokens` has at least `min_tokens` tokens, but fewer than `min_tokens` of them
    are flagged `is_content` (e.g. a short label or a list of names/numbers, as opposed to
    flowing prose dense with content words)."""
    if len(tokens) < min_tokens:
        return False
    return len([token for token in tokens if token.is_content]) < min_tokens
