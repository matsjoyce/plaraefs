import collections


class FIFileHeader:
    __slots__ = ["token", "data", "block_id"]

    def __init__(self, token, data, block_id):
        self.token = token
        self.data = data
        self.block_id = block_id


class FileIterator:
    def __init__(self, fs, file_id, start):
        self.fs = fs
        self.start = start
        self.file_id = file_id

    def block_from_offset(self, start):
        if start < self.fs.FILE_HEADER_DATA_SIZE:
            return 0, start
        start -= self.fs.FILE_HEADER_DATA_SIZE
        header, block_and_stuff = divmod(start,
                                         self.fs.blockfs.LOGICAL_BLOCK_SIZE * self.fs.BLOCK_IDS_PER_HEADER
                                         + self.fs.FILE_CONTINUATION_HEADER_DATA_SIZE)
        block, offset = divmod(block_and_stuff, self.fs.blockfs.LOGICAL_BLOCK_SIZE)
        if block == self.fs.BLOCK_IDS_PER_HEADER:
            header += 1
            block = 0
        else:
            block += 1
        return header * self.fs.FILE_HEADER_INTERVAL + block, offset

