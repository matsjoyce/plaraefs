from test_filelevelfilesystem import fs  # noqa E401
from plaraefs.filelevelfilesystem import FileLevelFilesystem


def test_small_single_read(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    fs.write(file_id, b"abcdef" * 10)
    fs.blockfs.block_cache.clear()

    reads_before = fs.blockfs.block_reads

    assert fs.read(file_id) == b"abcdef" * 10
    assert reads_before + 1 == fs.blockfs.block_reads


def test_small_multi_read(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    fs.write(file_id, b"abcdef" * 10)
    fs.blockfs.block_cache.clear()

    reads_before = fs.blockfs.block_reads

    assert fs.read(file_id, 6) == b"abcdef"
    assert fs.read(file_id, 6 * 8, 6) == b"abcdef" * 8
    assert fs.read(file_id, 6, 6 * 9) == b"abcdef"
    assert reads_before + 1 == fs.blockfs.block_reads


def test_large_single_read(fs: FileLevelFilesystem):  # noqa E811
    file_id = fs.create_new_file(0)

    data = b"abcdef" * 2 ** 20

    fs.write(file_id, data)
    fs.blockfs.block_cache.clear()

    reads_before = fs.blockfs.block_reads

    assert fs.read(file_id) == data
    assert reads_before + fs.num_file_blocks(file_id) == fs.blockfs.block_reads
