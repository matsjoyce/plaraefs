from test_filesystem_low_level import fs
from plaraefs.write_iterator import WriteIterator
from plaraefs.filesystem import FileSystem


def test_take_unbuffered(fs: FileSystem):
    wi = WriteIterator(fs, 0, 0)
    wi.add_unbuffered(b"abc")
    wi.add_unbuffered(b"def")
    wi.add_unbuffered(b"ghi")

    assert wi.unflushed_data == [b"abc", b"def", b"ghi"]
    assert wi.unflushed_data_length == 9
    assert wi.unflushed_data_first_item_start == 0

    data = wi.take_unbuffered(2)

    assert data == b"ab"
    assert wi.unflushed_data == [b"abc", b"def", b"ghi"]
    assert wi.unflushed_data_length == 7
    assert wi.unflushed_data_first_item_start == 2

    data = wi.take_unbuffered(2)

    assert data == b"cd"
    assert wi.unflushed_data == [b"def", b"ghi"]
    assert wi.unflushed_data_length == 5
    assert wi.unflushed_data_first_item_start == 1

    data = wi.take_unbuffered(10)

    assert data is None

    data = wi.take_unbuffered(10, force=True)

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
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert data_before[:fs.FILE_HEADER_SIZE] + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after


def test_small_multi_write(fs: FileSystem):
    file_id = fs.create_new_file()

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)

    wi.write(b"abcdef" * 1, flush=True)
    wi.write(b"abcdef" * 8)
    wi.write(b"abcdef" * 1, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)

    assert token != token2
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert data_before[:fs.FILE_HEADER_SIZE] + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
