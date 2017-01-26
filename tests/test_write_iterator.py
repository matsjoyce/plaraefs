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


def test_offsets(fs: FileSystem):
    wi = WriteIterator(fs, 0, 0)

    assert wi.data_in_block(0, 0) == fs.FILE_HEADER_DATA_SIZE
    assert wi.data_in_block(0, 1) == fs.blockfs.LOGICAL_BLOCK_SIZE
    assert wi.data_in_block(0, fs.BLOCK_IDS_PER_HEADER) == fs.blockfs.LOGICAL_BLOCK_SIZE
    assert wi.data_in_block(1, 0) == fs.FILE_CONTINUATION_HEADER_DATA_SIZE
    assert wi.data_in_block(1, 1) == fs.blockfs.LOGICAL_BLOCK_SIZE

    header = block = offset = counter = 0
    for _ in range(5000):
        if offset >= wi.data_in_block(header, block):
            offset = offset - wi.data_in_block(header, block)
            block += 1
            if block >= fs.BLOCK_IDS_PER_HEADER + 1:
                h, block = divmod(block, (fs.BLOCK_IDS_PER_HEADER + 1))
                header += h

        if block == 0:
            if header == 0:
                header_size = fs.FILE_HEADER_SIZE
            else:
                header_size = fs.FILE_CONTINUATION_HEADER_SIZE
        else:
            header_size = 0

        assert wi.block_from_offset(counter) == (header, block, offset + header_size), counter
        offset += 255
        counter += 255
    print(header, block, offset)
    assert header > 1

    assert wi.block_from_offset(0) == (0, 0, fs.FILE_HEADER_SIZE)
    assert wi.block_from_offset(fs.FILE_HEADER_DATA_SIZE - 1) == (0, 0, fs.blockfs.LOGICAL_BLOCK_SIZE - 1)
    assert wi.block_from_offset(fs.FILE_HEADER_DATA_SIZE) == (0, 1, 0)


def test_small_single_write(fs: FileSystem):
    file_id = fs.create_new_file()

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)

    wi.write(b"abcdef" * 10, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)

    assert token != token2
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert data_before[:fs.FILE_HEADER_SIZE] + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
