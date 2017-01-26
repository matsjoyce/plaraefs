import collections

from .file_iterator import FileIterator


class DefaultDict2(collections.defaultdict):
    def __missing__(self, key):
        return self.default_factory(key)


class WriteIterator(FileIterator):
    def __init__(self, fs, file_id, start):
        super().__init__(fs, file_id, start)
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

    def blocks_to_write(self, flush=False):
        header, block_num, offset = self.block_from_offset(self.start)
        block_size = self.data_in_block(header, block_num)
        data_to_write = self.take_unbuffered(block_size - offset, force=flush)

        blocks_to_write = []

        while data_to_write:
            # Inefficiency: getting the header twice is bad as it won't change due to the lock
            self.start += len(data_to_write)
            data_from_end = self.fs.blockfs.LOGICAL_BLOCK_SIZE - offset - len(data_to_write)

            yield header, block_num, offset, data_from_end, data_to_write

            header, block_num, offset = self.block_from_offset(self.start)
            block_size = self.data_in_block(header, block_num)
            data_to_write = self.take_unbuffered(block_size - offset, force=flush)

    def write(self, data, flush=False):
        self.add_unbuffered(data)

        # TODO: For certain sizes this should be generated...
        blocks_to_write = list(self.blocks_to_write(flush=flush))

        if not blocks_to_write:
            return

        header_data_cache = DefaultDict2(self.get_header)

        with self.fs.blockfs.lock_file(write=True):
            for header, block_num, offset, data_from_end, data_to_write in blocks_to_write:
                header_block_id, header_data = header_data_cache[header]
                if block_num:
                    block_id = header_data.block_ids[block_num - 1]
                else:
                    block_id = header_block_id

                if offset or data_from_end:
                    old_data = self.fs.blockfs.read_block(block_id)
                    if old_data is None:
                        data_to_write = b"".join((b"\0" * offset, data_to_write, b"\0" * data_from_end))
                    else:
                        data_to_write = b"".join((old_data[:offset], data_to_write, old_data[-data_from_end:]))

                assert len(data_to_write) == self.fs.blockfs.LOGICAL_BLOCK_SIZE

                token = self.fs.blockfs.write_block(block_id, data_to_write, with_token=True)
                if not block_num:
                    self.set_header_token(header, token)
