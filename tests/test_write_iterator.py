import pytest
from test_filesystem_low_level import fs
from plaraefs.write_iterator import WriteIterator
from plaraefs.filesystem import FileSystem


def test_take_unflushed(fs: FileSystem):
    wi = WriteIterator(fs, 0, 0)
    wi.add_unflushed(b"abc")
    wi.add_unflushed(b"def")
    wi.add_unflushed(b"ghi")

    assert wi.unflushed_data == [b"abc", b"def", b"ghi"]
    assert wi.unflushed_data_length == 9
    assert wi.unflushed_data_first_item_start == 0

    data = wi.take_unflushed(2)

    assert data == b"ab"
    assert wi.unflushed_data == [b"abc", b"def", b"ghi"]
    assert wi.unflushed_data_length == 7
    assert wi.unflushed_data_first_item_start == 2

    data = wi.take_unflushed(2)

    assert data == b"cd"
    assert wi.unflushed_data == [b"def", b"ghi"]
    assert wi.unflushed_data_length == 5
    assert wi.unflushed_data_first_item_start == 1

    data = wi.take_unflushed(10)

    assert data is None

    data = wi.take_unflushed(10, force=True)

    assert data == b"efghi"
    assert wi.unflushed_data == []
    assert wi.unflushed_data_length == 0
    assert wi.unflushed_data_first_item_start == 0


def test_small_single_write(fs: FileSystem):
    file_id = fs.create_new_file()

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)

    wi.write(b"abcdef" * 10, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)

    assert token != token2
    assert data_after.index(b"a") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert data_before[:fs.FILE_HEADER_SIZE] + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after


def test_small_multi_write(fs: FileSystem):
    file_id = fs.create_new_file()

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)

    wi.write(b"abcdef" * 1, flush=True)
    wi.write(b"abcdef" * 8)
    wi.write(b"abcdef" * 1)
    wi.write(None, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)

    assert token != token2
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert data_before[:fs.FILE_HEADER_SIZE] + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after


@pytest.mark.xfail
def test_large_single_write(fs: FileSystem):
    file_id = fs.create_new_file()

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)

    data = b"abcdef" * 2 ** 10
    wi.write(data, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)

    assert token != token2
    assert data_after.index(b"a") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:] == data[:fs.FILE_HEADER_DATA_SIZE]
