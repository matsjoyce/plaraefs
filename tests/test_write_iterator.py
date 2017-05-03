from test_filelevelfilesystem import fs
from plaraefs.write_iterator import WriteIterator
from plaraefs.filelevelfilesystem import FileLevelFilesystem, FileHeader


def test_take_unflushed(fs: FileLevelFilesystem):
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


def test_small_single_write(fs: FileLevelFilesystem):
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)
    writes_before = fs.blockfs.block_writes

    wi = WriteIterator(fs, file_id, 0)

    wi.write(b"abcdef" * 10, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, b"", len(b"abcdef" * 10), 0, []))

    assert token != token2
    assert data_after.index(b"a") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert header + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
    assert fs.blockfs.block_writes == writes_before + 1


def test_small_single_overwrite(fs: FileLevelFilesystem):
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)
    wi.write(b"abcdef" * 10, flush=True)

    wi = WriteIterator(fs, file_id, 0)
    wi.write(b"123456" * 5, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, b"", len(b"abcdef" * 10), 0, []))

    assert token != token2
    assert data_after.index(b"1") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"123456" * 5 + b"abcdef" * 5
    assert header + b"123456" * 5 + b"abcdef" * 5 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after


def test_small_multi_write(fs: FileLevelFilesystem):
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)
    writes_before = fs.blockfs.block_writes

    wi = WriteIterator(fs, file_id, 0)

    wi.write(b"abcdef" * 1, flush=True)
    wi.write(b"abcdef" * 8)
    wi.write(b"abcdef" * 1)
    wi.write(None, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, b"", len(b"abcdef" * 10), 0, []))

    assert token != token2
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert header + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
    assert fs.blockfs.block_writes == writes_before + 2


def test_large_single_write(fs: FileLevelFilesystem):
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)
    writes_before = fs.blockfs.block_writes

    wi = WriteIterator(fs, file_id, 0)

    data = b"abcdef" * 2 ** 20
    wi.write(data, flush=True)

    assert fs.get_file_header(file_id, 0)[1].size == len(data)
    assert fs.blockfs.block_writes == (writes_before + fs.num_file_blocks(file_id) * 2)

    data_pos = 0
    for i in range(fs.num_file_blocks(file_id)):
        bdata = fs.blockfs.read_block(file_id + i)
        if i % fs.FILE_HEADER_INTERVAL == 0:
            if i == 0:
                bdata = bdata[fs.FILE_HEADER_SIZE:]
            else:
                bdata = bdata[fs.FILE_CONTINUATION_HEADER_SIZE:]

        corresponding_data = data[data_pos:data_pos + len(bdata)]
        assert bdata[:len(corresponding_data)] == corresponding_data
        assert bdata[len(corresponding_data):].count(b"\0") == len(bdata[len(corresponding_data):])
        data_pos += len(bdata)


def test_large_multi_write(fs: FileLevelFilesystem):
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)

    data = [b"abcdef" * 3 * 2 ** 10 for _ in range(8)]
    for i, x in enumerate(data):
        wi.write(x, flush=bool(i % 2))
    wi.write(None, flush=True)

    data = b"abcdef" * 3 * 2 ** 13

    assert fs.get_file_header(file_id, 0)[1].size == len(data)

    data_pos = 0
    for i in range(fs.num_file_blocks(file_id)):
        bdata = fs.blockfs.read_block(file_id + i)
        if i % fs.FILE_HEADER_INTERVAL == 0:
            if i == 0:
                bdata = bdata[fs.FILE_HEADER_SIZE:]
            else:
                bdata = bdata[fs.FILE_CONTINUATION_HEADER_SIZE:]

        corresponding_data = data[data_pos:data_pos + len(bdata)]
        assert bdata[:len(corresponding_data)] == corresponding_data
        assert bdata[len(corresponding_data):].count(b"\0") == len(bdata[len(corresponding_data):])
        data_pos += len(bdata)


def test_seek(fs: FileLevelFilesystem):
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    wi = WriteIterator(fs, file_id, 0)
    wi.write(b"abcdef" * 10)

    wi.seek(0)
    wi.write(b"123456" * 5, flush=True)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, b"", len(b"abcdef" * 10), 0, []))

    assert token != token2
    assert data_after.index(b"1") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"123456" * 5 + b"abcdef" * 5
    assert header + b"123456" * 5 + b"abcdef" * 5 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
