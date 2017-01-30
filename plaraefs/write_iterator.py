from .file_iterator import FileIterator


class WriteIterator(FileIterator):
    def __init__(self, fs, file_id, start):
        super().__init__(fs, file_id, start)
        self.unflushed_data = []
        self.unflushed_data_length = 0
        self.unflushed_data_first_item_start = 0

    def add_unflushed(self, data):
        if data:
            self.unflushed_data.append(data)
            self.unflushed_data_length += len(data)

    def take_unflushed(self, length, force=False):
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
                collected_chunk = chunk[self.unflushed_data_first_item_start:
                                        self.unflushed_data_first_item_start + wanted_length]
                self.unflushed_data_first_item_start += len(collected_chunk)
                assert self.unflushed_data_first_item_start < len(chunk)

            collected_data.append(collected_chunk)
            collected_length += len(collected_chunk)
            self.unflushed_data_length -= len(collected_chunk)

        data = b"".join(collected_data)
        assert len(data) == length or len(data) < length and force
        return data

    def blocks_to_write(self, flush=False):
        block_num, offset = self.block_from_offset(self.start)
        block_size = self.fs.file_data_in_block(block_num)
        data_to_write = self.take_unflushed(block_size - offset, force=flush)

        while data_to_write:
            self.start += len(data_to_write)
            data_from_end = block_size - offset - len(data_to_write)

            yield block_num, offset, data_from_end, data_to_write

            block_num, offset = self.block_from_offset(self.start)
            block_size = self.fs.file_data_in_block(block_num)
            data_to_write = self.take_unflushed(block_size - offset, force=flush)

    def write(self, data, flush=False):
        self.add_unflushed(data)

        # TODO: For certain sizes this should be generated...
        blocks_to_write = list(self.blocks_to_write(flush=flush))

        if not blocks_to_write:
            return

        with self.fs.blockfs.lock_file(write=True):
            total_blocks = self.fs.num_file_blocks(self.file_id)
            for block_num, offset, data_from_end, data_to_write in blocks_to_write:
                if block_num >= total_blocks:
                    self.fs.extend_file_blocks(self.file_id, blocks_to_write[-1][0] + 1, total_blocks - 1)
                    total_blocks = blocks_to_write[-1][0] + 1
                if offset or data_from_end:
                    old_data = self.fs.read_file_data(self.file_id, block_num)
                    if old_data is None:
                        data_to_write = b"".join((b"\0" * offset, data_to_write, b"\0" * data_from_end))
                    else:
                        data_to_write = b"".join((old_data[:offset], data_to_write, old_data[-data_from_end:]))

                self.fs.write_file_data(self.file_id, block_num, data_to_write)
