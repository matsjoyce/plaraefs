import pytest
import pathlib
import os
import random
import bitarray

from plaraefs.blockfilesystem import BlockFileSystem
from plaraefs.filesystem import FileSystem, FileHeader, FileContinuationHeader


@pytest.fixture()
def fs():
    key = os.urandom(32)
    location = pathlib.Path("test_bfs.plaraefs")
    if location.exists():
        location.unlink()
    BlockFileSystem.initialise(location)
    bfs = BlockFileSystem(location, key)
    FileSystem.initialise(bfs)
    yield FileSystem(bfs)
    bfs.close()
    location.unlink()


def test_file_header(fs: FileSystem):
    header = FileHeader(0, b"stuff", 0, 0, [])
    assert header == fs.unpack_file_header(fs.pack_file_header(header))

    header = FileHeader(1, b"stuff", random.randrange(2 ** 64), random.randrange(2 ** 64),
                        [random.randrange(2 ** 64) for _ in range(32)])
    assert header == fs.unpack_file_header(fs.pack_file_header(header))


def test_file_continuation_header(fs: FileSystem):
    header = FileContinuationHeader(0, 0, [])
    assert header == fs.unpack_file_continuation_header(fs.pack_file_continuation_header(header))

    header = FileContinuationHeader(random.randrange(2**64), random.randrange(2**64),
                                    [random.randrange(2**64) for _ in range(32)])
    assert header == fs.unpack_file_continuation_header(fs.pack_file_continuation_header(header))


def test_data_in_block(fs: FileSystem):
    assert fs.file_data_in_block(0) == fs.FILE_HEADER_DATA_SIZE
    assert fs.file_data_in_block(1) == fs.blockfs.LOGICAL_BLOCK_SIZE
    assert fs.file_data_in_block(fs.BLOCK_IDS_PER_HEADER) == fs.blockfs.LOGICAL_BLOCK_SIZE
    assert fs.file_data_in_block(fs.FILE_HEADER_INTERVAL) == fs.FILE_CONTINUATION_HEADER_DATA_SIZE
    assert fs.file_data_in_block(fs.FILE_HEADER_INTERVAL + 1) == fs.blockfs.LOGICAL_BLOCK_SIZE


def test_allocate_blocks(fs: FileSystem):
    n = fs.number_free_blocks(0)

    assert n == fs.blockfs.LOGICAL_BLOCK_SIZE * 8 - 1

    bitmap = fs.read_superblock(0)
    bitmap_comp = bitarray.bitarray(fs.blockfs.LOGICAL_BLOCK_SIZE * 8)
    bitmap_comp.setall(False)
    bitmap_comp[0] = True

    assert bitmap == bitmap_comp

    blocks = fs.allocate_blocks(10)

    assert list(blocks) == list(range(1, 11))

    bitmap = fs.read_superblock(0)
    bitmap_comp[1:11] = True

    assert bitmap == bitmap_comp

    fs.deallocate_blocks(blocks)

    n = fs.number_free_blocks(0)

    assert n == fs.blockfs.LOGICAL_BLOCK_SIZE * 8 - 1

    bitmap = fs.read_superblock(0)
    bitmap_comp = bitarray.bitarray(fs.blockfs.LOGICAL_BLOCK_SIZE * 8)
    bitmap_comp.setall(False)
    bitmap_comp[0] = True

    assert bitmap == bitmap_comp


def test_create_new_file(fs: FileSystem):
    file_id = fs.create_new_file()

    assert file_id == 1
    assert fs.unpack_file_header(fs.blockfs.read_block(file_id)) == FileHeader(0, b"", 0, 0, [])


def test_extend_file_blocks(fs: FileSystem):
    file_id = fs.create_new_file()
    assert file_id == 1

    norm_headers = [FileContinuationHeader(1 + (i + 1) * fs.FILE_HEADER_INTERVAL,
                                           1 + (i - 1) * fs.FILE_HEADER_INTERVAL,
                                           list(range(i * fs.FILE_HEADER_INTERVAL + 2,
                                                      i * fs.FILE_HEADER_INTERVAL + fs.BLOCK_IDS_PER_HEADER + 2)))
                    for i in range(1, 10)]
    norm_headers.insert(0, FileHeader(0, b"", 0,
                                      1 + fs.FILE_HEADER_INTERVAL,
                                      list(range(2, 2 + fs.BLOCK_IDS_PER_HEADER))))

    fs.extend_file_blocks(file_id, 10)

    assert fs.num_file_blocks(file_id) == 10
    assert fs.get_file_header(file_id, 0)[1] == FileHeader(0, b"", 0, 0, list(range(2, 11)))

    # Aim for a header with no blocks
    fs.extend_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 3 + 1)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 3 + 1
    for i in range(3):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]
    assert (fs.get_file_header(file_id, 3)[1] ==
            FileContinuationHeader(0, 1 + fs.FILE_HEADER_INTERVAL * 2, []))

    # Aim for a header with one block
    fs.extend_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 4 + 2)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 4 + 2
    for i in range(4):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]
    assert (fs.get_file_header(file_id, 4)[1] ==
            FileContinuationHeader(0, 1 + fs.FILE_HEADER_INTERVAL * 3,
                                   [2 + fs.FILE_HEADER_INTERVAL * 4]))

    # Aim for a full header
    fs.extend_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 5)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 5
    x = norm_headers[4].next_header
    norm_headers[4].next_header = 0
    for i in range(5):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]
    norm_headers[4].next_header = x

    for i in range(file_id, fs.blockfs.total_blocks()):
        if (i - 1) % fs.FILE_HEADER_INTERVAL:
            assert fs.blockfs.read_block(i) is None
        else:
            assert fs.blockfs.read_block(i) is not None


def test_truncate_file_blocks(fs: FileSystem):
    file_id = fs.create_new_file()
    assert file_id == 1

    norm_headers = [FileContinuationHeader(1 + (i + 1) * fs.FILE_HEADER_INTERVAL,
                                           1 + (i - 1) * fs.FILE_HEADER_INTERVAL,
                                           list(range(i * fs.FILE_HEADER_INTERVAL + 2,
                                                      i * fs.FILE_HEADER_INTERVAL + fs.BLOCK_IDS_PER_HEADER + 2)))
                    for i in range(1, 10)]
    norm_headers.insert(0, FileHeader(0, b"", 0,
                                      1 + fs.FILE_HEADER_INTERVAL,
                                      list(range(2, 2 + fs.BLOCK_IDS_PER_HEADER))))

    fs.extend_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 5)

    fs.truncate_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 5 - 10)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 5 - 10
    for i in range(4):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]

    assert (fs.get_file_header(file_id, 4)[1] ==
            FileContinuationHeader(0, 1 + fs.FILE_HEADER_INTERVAL * 3,
                                   list(range(2 + fs.FILE_HEADER_INTERVAL * 4,
                                              2 + fs.FILE_HEADER_INTERVAL * 5 - 11))))

    # Aim for a header with one block
    fs.truncate_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 3 + 2)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 3 + 2
    for i in range(3):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]

    assert (fs.get_file_header(file_id, 3)[1] ==
            FileContinuationHeader(0, 1 + fs.FILE_HEADER_INTERVAL * 2,
                                   [2 + fs.FILE_HEADER_INTERVAL * 3]))

    # Aim for a header with no blocks
    fs.truncate_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 3 + 1)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 3 + 1
    for i in range(3):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]

    assert (fs.unpack_file_continuation_header(fs.blockfs.read_block(norm_headers[2].next_header)) ==
            FileContinuationHeader(0, 1 + fs.FILE_HEADER_INTERVAL * 2, []))

    # Aim for a full header
    fs.truncate_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 3)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 3
    for i in range(2):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]
    assert (fs.get_file_header(file_id, 2)[1] ==
            FileContinuationHeader(0, 1 + fs.FILE_HEADER_INTERVAL * 1,
                                   list(range(2 + fs.FILE_HEADER_INTERVAL * 2,
                                              2 + fs.FILE_HEADER_INTERVAL * 2 + fs.BLOCK_IDS_PER_HEADER))))

    fs.extend_file_blocks(file_id, fs.FILE_HEADER_INTERVAL * 5)

    assert fs.num_file_blocks(file_id) == fs.FILE_HEADER_INTERVAL * 5
    x = norm_headers[4].next_header
    norm_headers[4].next_header = 0
    for i in range(5):
        assert fs.get_file_header(file_id, i)[1] == norm_headers[i]
    norm_headers[4].next_header = x

    fs.truncate_file_blocks(file_id, 1)

    assert fs.num_file_blocks(file_id) == 1
    assert fs.unpack_file_header(fs.blockfs.read_block(file_id)) == FileHeader(0, b"", 0, 0, [])

    for i in range(file_id, fs.blockfs.total_blocks()):
        if i == file_id:
            assert fs.blockfs.read_block(i) is not None
        else:
            assert fs.blockfs.read_block(i) is None
