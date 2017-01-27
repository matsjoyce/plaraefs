from test_filesystem_low_level import fs
from plaraefs.write_iterator import FileIterator
from plaraefs.filesystem import FileSystem


def test_offsets(fs: FileSystem):
    fi = FileIterator(fs, 0, 0)

    block = offset = counter = 0
    for _ in range(5000):
        if offset >= fs.file_data_in_block(block):
            offset = offset - fs.file_data_in_block(block)
            block += 1


        assert fi.block_from_offset(counter) == (block, offset), counter
        offset += 255
        counter += 255

    assert fi.block_from_offset(0) == (0, 0)
    assert fi.block_from_offset(fs.FILE_HEADER_DATA_SIZE - 1) == (0, fs.blockfs.LOGICAL_BLOCK_SIZE - 1 - fs.FILE_HEADER_SIZE)
    assert fi.block_from_offset(fs.FILE_HEADER_DATA_SIZE) == (1, 0)
