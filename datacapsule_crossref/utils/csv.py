import csv
import os

import six


TEMP_FILE_SUFFIX = '.part'


def gzip_open(filename, mode):
    import gzip

    if mode == 'w' and not six.PY2:
        from io import TextIOWrapper

        return TextIOWrapper(gzip.open(filename, mode))
    else:
        return gzip.open(filename, mode)


def optionally_compressed_open(filename, mode):
    if filename.endswith('.gz') or filename.endswith('.gz' + TEMP_FILE_SUFFIX):
        return gzip_open(filename, mode)
    else:
        return open(filename, mode)


def open_csv_output(filename):
    return optionally_compressed_open(filename, 'w')


def write_csv_rows(writer, iterable):
    if six.PY2:
        for row in iterable:
            writer.writerow([
                x.encode('utf-8') if isinstance(x, six.text_type) else x
                for x in row
            ])
    else:
        for row in iterable:
            writer.writerow(row)


def write_csv_row(writer, row):
    write_csv_rows(writer, [row])


def csv_delimiter_by_filename(filename):
    if '.tsv' in filename:
        return '\t'
    else:
        return ','


def write_csv(filename, columns, iterable, delimiter=None):
    if delimiter is None:
        delimiter = csv_delimiter_by_filename(filename)
    is_stdout = filename in {'stdout', '/dev/stdout'}
    temp_filename = (
        filename + TEMP_FILE_SUFFIX
        if not is_stdout
        else filename
    )
    if not is_stdout and os.path.isfile(filename):
        os.remove(filename)
    with open_csv_output(temp_filename) as csv_f:
        writer = csv.writer(csv_f, delimiter=delimiter)
        write_csv_rows(writer, [columns])
        write_csv_rows(writer, iterable)
    if not is_stdout:
        os.rename(temp_filename, filename)
