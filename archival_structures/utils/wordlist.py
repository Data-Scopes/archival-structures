"""Read/write tab-separated word-type/word-token lists (used by
`archival_structures.analysis.token_analysis`/`text_analysis` to classify tokens by type, e.g.
stopwords or honorifics)."""

from collections import defaultdict

from archival_structures.utils.file import open_file


def write_wordlist(wordlist, filename: str):
    """Write `wordlist` (`{word_type: {word, ...}}`) to `filename` as tab-separated
    `word_type\\tword` lines (gzip-compressed if `filename` ends in `.gz`)."""
    with open_file(filename, 'wt') as fh:
        for word_type in wordlist:
            for word in wordlist[word_type]:
                fh.write(f"{word_type}\t{word}\n")
    return None


def read_wordlist(filename: str):
    """Read a word list file, which consists of two columns, word type and word token."""
    wordlist = defaultdict(set)
    with open_file(filename, 'rt') as fh:
        for li, line in enumerate(fh):
            try:
                word_type, word = line.strip('\n').split()
            except ValueError:
                print(f"invalid line ({li+1}): \"{line}\"")
                raise
            wordlist[word_type].add(word)
    return wordlist
