import collections


class WIFileHeader:
    __slots__ = ["token", "data", "block_id"]

    def __init__(self, token, data, block_id):
        self.token = token
        self.data = data
        self.block_id = block_id


class WriteIterator:
    def __init__(self, fs, file_id, start):
        self.fs = fs
        self.start = start
        self.file_id = file_id

        self.headers = {}
        self.current_header_id = None
        self.current_header = None

        self.unflushed_data = []
        self.unflushed_data_length = 0
        self.unflushed_data_first_item_start = 0

    def add_unbuffered(self, data):
        if data:
            self.unflushed_data.append(data)
            self.unflushed_data_length += len(data)

    def take_unbuffered(self, length, force=False):
        if self.unflushed_data_length < length and not force:
            return

        collected_length = 0
        collected_data = []
        while collected_length < length and self.unflushed_data_length:
            wanted_length = length - collected_length
            chunk = self.unflushed_data[0]

            if len(chunk) - self.unflushed_data_first_item_start <= wanted_length:
                self.unflushed_data.pop(0)
                collected_chunk = chunk[self.unflushed_data_first_item_start:]
                self.unflushed_data_first_item_start = 0
            else:
                collected_chunk = chunk[self.unflushed_data_first_item_start:self.unflushed_data_first_item_start + wanted_length]
                self.unflushed_data_first_item_start += len(collected_chunk)
                assert self.unflushed_data_first_item_start < len(chunk)

            collected_data.append(collected_chunk)
            collected_length += len(collected_chunk)
            self.unflushed_data_length -= len(collected_chunk)

        data = b"".join(collected_data)
        assert len(data) == length or len(data) < length and force
        return data

    def get_header(self, header_id):
        if header_id in self.headers:
            wih = self.headers[header_id]
            reload, wih.token = self.fs.blockfs.block_version(wih.block_id, wih.token)
            if not reload:
                return wih.block_id, wih.data

        if header_id == 0:
            block_id = self.file_id
        else:
            block_id = self.get_header(header_id - 1).continuation_block_id

        data, token = self.fs.blockfs.read_block(block_id, with_token=True)
        if header_id == 0:
            hdata, _ = self.fs.read_file_header(data)
        else:
            hdata, _ = self.fs.read_file_continuation_header(data)

        self.headers[header_id] = WIFileHeader(token, hdata, block_id)
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

    def write(self, data, flush=False):
        self.add_unbuffered(data)
        header, block_num, offset = self.block_from_offset(self.start)
        block_size = self.data_in_block(header, block_num)
        data_to_write = self.take_unbuffered(block_size - offset, force=flush)

        if not data_to_write:
            return

        with self.fs.blockfs.lock_file(write=True):
            while data_to_write:
                # Inefficiency: getting the header twice is bad as it won't change due to the lock
                header_block_id, header_data = self.get_header(header)
                if block_num:
                    block_id = header_data.block_ids[block_num - 1]
                else:
                    block_id = header_block_id
                data_from_end = self.fs.blockfs.LOGICAL_BLOCK_SIZE - offset - len(data_to_write)

                if offset or data_from_end:
                    old_data = self.fs.blockfs.read_block(block_id)
                    if old_data is None:
                        data_to_write = b"".join((b"\0" * offset, data_to_write, b"\0" * data_from_end))
                    else:
                        data_to_write = b"".join((old_data[:offset], data_to_write, old_data[-data_from_end:]))

                assert len(data_to_write) == self.fs.blockfs.LOGICAL_BLOCK_SIZE

                self.fs.blockfs.write_block(block_id, data_to_write)
                self.start += len(data_to_write)

                header, block_num, offset = self.block_from_offset(self.start)
                block_size = self.data_in_block(header, block_num)
                data_to_write = self.take_unbuffered(block_size - offset, force=flush)

