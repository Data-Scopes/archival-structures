import re
from string import punctuation
from typing import Dict, Iterable, List, Set, Union


REPEAT_SYMBOLS = {',,', '„'}


def token_in_wordlists(token: str, wordlist: Dict[str, Set[str]]):
    """Return all word types for which the token occur in the list of words of that type."""
    return [word_type for word_type in wordlist if token in wordlist[word_type]]


def token_is_punct(token: str, punct_tokens: Set[str] = None):
    if punct_tokens is None:
        punct_tokens = punctuation
    if token in punct_tokens:
        return True
    if all(token_char in punct_tokens for token_char in token):
        return True
    return False


def token_is_number(token: str, number_words: Set[str] = None):
    if token.isdigit():
        return True
    if number_words is not None and token in number_words:
        return True
    if re.match(r"\d+([,.]\d+)*", token):
        return True
    return False


def token_is_content_word(token: str, non_content_words: Set[str] = None):
    if non_content_words is None:
        return True
    return token not in non_content_words


def token_is_repeat_symbol(token: str):
    return token in REPEAT_SYMBOLS


class Token:

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


def tokens_are_running_text(tokens: List[Token], min_tokens: int = 2):
    if len(tokens) < min_tokens:
        return False
    return len([token for token in tokens if token.is_content]) < min_tokens
