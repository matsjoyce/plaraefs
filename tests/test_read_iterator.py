from test_filelevelfilesystem import fs  # noqa E401
from plaraefs.read_iterator import ReadIterator
from plaraefs.filelevelfilesystem import FileLevelFilesystem


def test_small_single_read(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    wi = fs.writer(file_id, 0)
    wi.write(b"abcdef" * 10, flush=True)
    fs.blockfs.block_cache.clear()

    reads_before = fs.blockfs.block_reads

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read() == b"abcdef" * 10
    assert reads_before + 1 == fs.blockfs.block_reads


def test_small_multi_read(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    wi = fs.writer(file_id, 0)
    wi.write(b"abcdef" * 10, flush=True)
    fs.blockfs.block_cache.clear()

    reads_before = fs.blockfs.block_reads

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read(6) == b"abcdef"
    assert ri.read(6 * 8) == b"abcdef" * 8
    assert ri.read() == b"abcdef"
    assert reads_before + 1 == fs.blockfs.block_reads


def test_large_single_read(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data = b"abcdef" * 2 ** 20

    wi = fs.writer(file_id, 0)
    wi.write(data, flush=True)
    fs.blockfs.block_cache.clear()

    reads_before = fs.blockfs.block_reads

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read() == data
    assert reads_before + fs.num_file_blocks(file_id) == fs.blockfs.block_reads


def test_seek(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    wi = fs.writer(file_id, 0)
    wi.write(b"abcdef" * 10, flush=True)
    fs.blockfs.block_cache.clear()

    ri = ReadIterator(fs, file_id, 0)
    assert ri.read() == b"abcdef" * 10

    ri.seek(6 * 5)

    assert ri.read() == b"abcdef" * 5
