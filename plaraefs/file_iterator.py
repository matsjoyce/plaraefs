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

        self.headers = {}
        self.current_header_id = None
        self.current_header = None

    def set_header_token(self, header_num, token):
        self.headers[header_num].token = token

    def get_header(self, header_num):
        if header_num in self.headers:
            wih = self.headers[header_num]
            reload, wih.token = self.fs.blockfs.block_version(wih.block_id, wih.token)
            if not reload:
                return wih.block_id, wih.data

        if header_num == 0:
            block_id = self.file_id
        else:
            block_id = self.get_header(header_num - 1).continuation_block_id

        data, token = self.fs.blockfs.read_block(block_id, with_token=True)
        if header_num == 0:
            hdata, _ = self.fs.read_file_header(data)
        else:
            hdata, _ = self.fs.read_file_continuation_header(data)

        self.headers[header_num] = FIFileHeader(token, hdata, block_id)
        return block_id, hdata

    def data_in_block(self, header, block_num):
        if block_num == 0:
            if header == 0:
                return self.fs.FILE_HEADER_DATA_SIZE
            return self.fs.FILE_CONTINUATION_HEADER_DATA_SIZE
        return self.fs.blockfs.LOGICAL_BLOCK_SIZE

    def block_from_offset(self, start):
        if start < self.fs.FILE_HEADER_DATA_SIZE:
            return 0, 0, start + self.fs.FILE_HEADER_SIZE
        start -= self.fs.FILE_HEADER_DATA_SIZE
        header, block_and_stuff = divmod(start,
                                         self.fs.blockfs.LOGICAL_BLOCK_SIZE * self.fs.BLOCK_IDS_PER_HEADER
                                         + self.fs.FILE_CONTINUATION_HEADER_DATA_SIZE)
        block, offset = divmod(block_and_stuff, self.fs.blockfs.LOGICAL_BLOCK_SIZE)
        if block == self.fs.BLOCK_IDS_PER_HEADER:
            header += 1
            block = 0
            offset += self.fs.FILE_CONTINUATION_HEADER_SIZE
        else:
            block += 1
        return header, block, offset

