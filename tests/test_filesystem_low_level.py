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
    header = FileHeader(0, b"stuff", 0, 0, [0] * fs.BLOCK_IDS_PER_HEADER)
    assert (header, b"") == fs.read_file_header(fs.write_file_header(header, b""))

    header = FileHeader(1, b"stuff", random.randrange(2 ** 64), random.randrange(2 ** 64),
                        [random.randrange(2 ** 64) for _ in range(32)])
    assert (header, b"") == fs.read_file_header(fs.write_file_header(header, b""))


def test_file_continuation_header(fs: FileSystem):
    header = FileContinuationHeader(0, [0] * fs.BLOCK_IDS_PER_HEADER)
    assert (header, b"") == fs.read_file_continuation_header(fs.write_file_continuation_header(header, b""))

    header = FileContinuationHeader(random.randrange(2**64), [random.randrange(2**64) for _ in range(32)])
    assert (header, b"") == fs.read_file_continuation_header(fs.write_file_continuation_header(header, b""))


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
    assert fs.read_file_header(fs.blockfs.read_block(file_id))[0] == FileHeader(0, b"", 0, 0, [0] * fs.BLOCK_IDS_PER_HEADER)

