import gzip


def get_open_func(filename: str):
    return gzip.open if filename.endswith('.gz') else open


def open_file(filename: str, mode: str):
    open_func = get_open_func(filename)
    return open_func(filename, mode)
