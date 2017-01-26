from test_filesystem_low_level import fs
from plaraefs.write_iterator import FileIterator
from plaraefs.filesystem import FileSystem


def test_offsets(fs: FileSystem):
    fi = FileIterator(fs, 0, 0)

    assert fi.data_in_block(0, 0) == fs.FILE_HEADER_DATA_SIZE
    assert fi.data_in_block(0, 1) == fs.blockfs.LOGICAL_BLOCK_SIZE
    assert fi.data_in_block(0, fs.BLOCK_IDS_PER_HEADER) == fs.blockfs.LOGICAL_BLOCK_SIZE
    assert fi.data_in_block(1, 0) == fs.FILE_CONTINUATION_HEADER_DATA_SIZE
    assert fi.data_in_block(1, 1) == fs.blockfs.LOGICAL_BLOCK_SIZE

    header = block = offset = counter = 0
    for _ in range(5000):
        if offset >= fi.data_in_block(header, block):
            offset = offset - fi.data_in_block(header, block)
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

        assert fi.block_from_offset(counter) == (header, block, offset + header_size), counter
        offset += 255
        counter += 255
    print(header, block, offset)
    assert header > 1

    assert fi.block_from_offset(0) == (0, 0, fs.FILE_HEADER_SIZE)
    assert fi.block_from_offset(fs.FILE_HEADER_DATA_SIZE - 1) == (0, 0, fs.blockfs.LOGICAL_BLOCK_SIZE - 1)
    assert fi.block_from_offset(fs.FILE_HEADER_DATA_SIZE) == (0, 1, 0)
