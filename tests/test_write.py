from test_filelevelfilesystem import fs  # noqa E401
from plaraefs.filelevelfilesystem import FileLevelFilesystem, FileHeader


def test_small_single_write(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)
    writes_before = fs.blockfs.block_writes

    fs.write(file_id, b"abcdef" * 10)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, len(b"abcdef" * 10), 0, [], 0, b""))

    assert token != token2
    assert data_after.index(b"a") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert header + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
    assert fs.blockfs.block_writes == writes_before + 1


def test_small_single_overwrite(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    fs.write(file_id, b"abcdef" * 10)

    fs.write(file_id, b"123456" * 5)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, len(b"abcdef" * 10), 0, [], 0, b""))

    assert token != token2
    assert data_after.index(b"1") == fs.FILE_HEADER_SIZE
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"123456" * 5 + b"abcdef" * 5
    assert header + b"123456" * 5 + b"abcdef" * 5 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after


def test_small_multi_write(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)
    writes_before = fs.blockfs.block_writes

    fs.write(file_id, b"abcdef" * 1)
    fs.write(file_id, b"abcdef" * 8, 6)
    fs.write(file_id, b"abcdef" * 1, 6 * 9)

    data_after, token2 = fs.blockfs.read_block(file_id, with_token=True)
    header = fs.pack_file_header(FileHeader(0, len(b"abcdef" * 10), 0, [], 0, b""))

    assert token != token2
    assert data_after[fs.FILE_HEADER_SIZE:fs.FILE_HEADER_SIZE + 60] == b"abcdef" * 10
    assert header + b"abcdef" * 10 + data_before[fs.FILE_HEADER_SIZE + 60:] == data_after
    assert fs.blockfs.block_writes == writes_before + 3


def test_large_single_write(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)
    writes_before = fs.blockfs.block_writes

    data = b"abcdef" * 2 ** 20
    fs.write(file_id, data)

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


def test_large_multi_write(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data_before, token = fs.blockfs.read_block(file_id, with_token=True)

    data = [b"abcdef" * 3 * 2 ** 10 for _ in range(8)]
    for i, x in enumerate(data):
        fs.write(file_id, x, i * len(x))

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
