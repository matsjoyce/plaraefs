import pytest
import pathlib
import os

from plaraefs.blockfilesystem import BlockFileSystem


@pytest.fixture()
def fs():
    key = os.urandom(32)
    location = pathlib.Path("test_bfs.plaraefs")
    if location.exists():
        location.unlink()
    BlockFileSystem.initialise(location)
    fs = BlockFileSystem(location, key)
    yield fs
    fs.close()
    location.unlink()


def test_new_blocks(fs: BlockFileSystem):
    fs.new_blocks(1)

    assert fs.read_block(0) is None
    assert fs.total_blocks() == 1

    fs.new_blocks(3)

    assert fs.total_blocks() == 4
    assert [fs.read_block(i) is None for i in range(4)]

    fs.remove_blocks(4)

    assert not fs.total_blocks()


def test_wipe_block(fs: BlockFileSystem):
    fs.new_blocks(1)

    assert fs.read_block(0) is None

    fs.write_block(0, b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE)

    assert fs.read_block(0) == b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE

    fs.wipe_block(0)

    assert fs.read_block(0) is None


def test_swap_block(fs: BlockFileSystem):
    fs.new_blocks(2)

    fs.write_block(0, b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE)
    fs.write_block(1, b"b" * BlockFileSystem.LOGICAL_BLOCK_SIZE)

    assert fs.read_block(0) == b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE
    assert fs.read_block(1) == b"b" * BlockFileSystem.LOGICAL_BLOCK_SIZE

    fs.swap_blocks(0, 1)

    assert fs.read_block(0) == b"b" * BlockFileSystem.LOGICAL_BLOCK_SIZE
    assert fs.read_block(1) == b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE


def test_read_write_blocks(fs: BlockFileSystem):
    random = os.urandom(32) * (BlockFileSystem.LOGICAL_BLOCK_SIZE // 32)
    fs.new_blocks(1)
    assert fs.read_block(0) is None
    fs.write_block(0, random)
    assert fs.read_block(0) == random


def test_new_version(fs: BlockFileSystem):
    fs.new_blocks(1)
    fs.write_block(0, b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE)

    _, token = fs.block_version(0)
    reload, token = fs.block_version(0, token)

    assert not reload

    _, token = fs.read_block(0, with_token=True)
    reload, token = fs.block_version(0, token)

    assert not reload

    fs.write_block(0, b"a" * BlockFileSystem.LOGICAL_BLOCK_SIZE)
    reload, token = fs.block_version(0, token)
    assert reload
