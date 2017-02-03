from test_filesystem_low_level import fs
from plaraefs.read_iterator import ReadIterator
from plaraefs.write_iterator import WriteIterator
from plaraefs.filesystem import FileSystem


def test_small_single_read(fs: FileSystem):
    file_id = fs.create_new_file()

    wi = WriteIterator(fs, file_id, 0)
    wi.write(b"abcdef" * 10, flush=True)

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read() == b"abcdef" * 10


def test_small_multi_read(fs: FileSystem):
    file_id = fs.create_new_file()

    wi = WriteIterator(fs, file_id, 0)
    wi.write(b"abcdef" * 10, flush=True)

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read(6) == b"abcdef"
    assert ri.read(6 * 8) == b"abcdef" * 8
    assert ri.read() == b"abcdef"


def test_large_single_read(fs: FileSystem):
    file_id = fs.create_new_file()

    data = b"abcdef" * 2 ** 20

    wi = WriteIterator(fs, file_id, 0)
    wi.write(data, flush=True)

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read() == data
