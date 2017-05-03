from .file_iterator import FileIterator


class ReadIterator(FileIterator):
    def read_chunks(self, size):
        with self.fs.blockfs.lock_file(write=False):
            total_file_size = self.fs.get_file_header(self.file_id, 0)[1].size
            if size is None:
                size = total_file_size - self.start
            else:
                size = min(size, total_file_size - self.start)

            while size:
                block_num, offset = self.fs.block_from_offset(self.start)
                data = self.fs.read_file_data(self.file_id, block_num)
                chunk = data[offset:offset + size]
                yield chunk
                size -= len(chunk)
                self.start += len(chunk)

    def read(self, size=None):
        return b"".join(self.read_chunks(size))
