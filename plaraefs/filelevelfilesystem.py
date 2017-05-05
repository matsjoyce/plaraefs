import bitarray
import namedlist
import itertools
import struct

from .blocklevelfilesystem import BlockLevelFilesystem
from .read_iterator import ReadIterator
from .write_iterator import WriteIterator
from .utils import check_types, LRUDict


FileHeader = namedlist.namedlist("FileHeader", ("file_type", "size", "next_header",
                                                "block_ids", "xattr_block", "xattr_inline"))
FileContinuationHeader = namedlist.namedlist("FileContinuationHeader", ("next_header", "prev_header", "block_ids"))
HeaderCache = namedlist.namedlist("HeaderCache", ("block_id", "hdata", "token"))
SuperblockCache = namedlist.namedlist("SuperblockCache", ("data", "token"))

singleton_0_bitarray = bitarray.bitarray("0")
singleton_1_bitarray = bitarray.bitarray("1")


class KeyAlreadyExists(KeyError):
    pass


class KeyDoesNotExist(KeyError):
    pass


class FileLevelFilesystem:
    FILESIZE_SIZE = 8
    BLOCK_IDS_PER_HEADER = 32
    FILE_HEADER_INTERVAL = BLOCK_IDS_PER_HEADER + 1
    XATTR_INLINE_SIZE = 256

    def __init__(self, blockfs: BlockLevelFilesystem):
        self.blockfs = blockfs
        self.header_cache = LRUDict(1024)
        self.superblock_cache = LRUDict(128)

        self.FILE_HEADER_SIZE = (1 + self.FILESIZE_SIZE +
                                 (self.BLOCK_IDS_PER_HEADER + 2) * self.blockfs.BLOCK_ID_SIZE +
                                 self.XATTR_INLINE_SIZE)
        self.FILE_HEADER_DATA_SIZE = self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_HEADER_SIZE
        self.FILE_CONTINUATION_HEADER_SIZE = (self.BLOCK_IDS_PER_HEADER + 2) * self.blockfs.BLOCK_ID_SIZE
        self.FILE_CONTINUATION_HEADER_DATA_SIZE = self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_CONTINUATION_HEADER_SIZE
        self.SUPERBLOCK_INTERVAL = self.blockfs.LOGICAL_BLOCK_SIZE * 8
        self.XATTR_BLOCK_HEADER_SIZE = self.blockfs.BLOCK_ID_SIZE
        self.XATTR_BLOCK_DATA_SIZE = self.blockfs.LOGICAL_BLOCK_SIZE - self.XATTR_BLOCK_HEADER_SIZE

        self.file_header_struct = struct.Struct(f"<B{self.BLOCK_IDS_PER_HEADER + 3}Q{self.XATTR_INLINE_SIZE}s")
        self.file_continuation_header_struct = struct.Struct(f"<{self.BLOCK_IDS_PER_HEADER + 2}Q")
        self.xattr_block_header_struct = struct.Struct(f"<Q{self.XATTR_BLOCK_DATA_SIZE}s")

    @classmethod
    @check_types
    def initialise(cls, blockfs: BlockLevelFilesystem):
        blocks = blockfs.new_blocks(1)
        assert blocks == [0]
        fs = cls(blockfs)
        fs.write_new_superblock(0)

    @check_types
    def unpack_file_header(self, data: bytes):
        (file_type, size, next_header, *block_ids,
         xattr_block, xattr_inline) = self.file_header_struct.unpack(data[:self.FILE_HEADER_SIZE])
        if not block_ids[-1]:
            block_ids = block_ids[:block_ids.index(0)]
        return FileHeader(file_type, size, next_header, block_ids, xattr_block, xattr_inline)

    @check_types
    def pack_file_header(self, header: FileHeader):
        return self.file_header_struct.pack(header.file_type,
                                            header.size,
                                            header.next_header,
                                            *header.block_ids,
                                            *([0] * (self.BLOCK_IDS_PER_HEADER - len(header.block_ids))),
                                            header.xattr_block,
                                            header.xattr_inline)

    @check_types
    def unpack_file_continuation_header(self, data: bytes):
        (next_header,
         prev_header,
         *block_ids) = self.file_continuation_header_struct.unpack(data[:self.FILE_CONTINUATION_HEADER_SIZE])
        if not block_ids[-1]:
            block_ids = block_ids[:block_ids.index(0)]
        return FileContinuationHeader(next_header, prev_header, block_ids)

    @check_types
    def pack_file_continuation_header(self, header: FileContinuationHeader):
        return self.file_continuation_header_struct.pack(header.next_header,
                                                         header.prev_header,
                                                         *header.block_ids,
                                                         *([0] * (self.BLOCK_IDS_PER_HEADER - len(header.block_ids))))

    @check_types
    def read_superblock(self, superblock_id: int):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        try:
            hcache = self.superblock_cache[superblock_id]
            reload, _ = self.blockfs.block_version(block_id, hcache.token)
            if not reload:
                return hcache.data
        except KeyError:
            pass

        if block_id >= self.blockfs.total_blocks():
            new_block_id, = self.blockfs.new_blocks(1)
            assert new_block_id == block_id, (new_block_id, block_id)
            self.write_new_superblock(superblock_id)
        arr = bitarray.bitarray()
        data, token = self.blockfs.read_block(block_id, with_token=True)
        arr.frombytes(data)
        self.superblock_cache[superblock_id] = SuperblockCache(arr, token)
        return arr

    @check_types
    def write_superblock(self, superblock_id: int, bitmap: bitarray.bitarray):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        token = self.blockfs.write_block(block_id, 0, bitmap.tobytes(), with_token=True)
        self.superblock_cache[superblock_id] = SuperblockCache(bitmap, token)

    @check_types
    def write_new_superblock(self, superblock_id: int):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        self.blockfs.write_block(block_id, 0, b"\x80" + b"\0" * (self.blockfs.LOGICAL_BLOCK_SIZE - 1))

    @check_types
    def number_free_blocks(self, superblock_id: int):
        return self.read_superblock(superblock_id).count(False)

    @check_types
    def allocate_blocks(self, number: int):
        blocks = []
        new_blocks = 0
        with self.blockfs.lock_file(write=True):
            total_size = self.blockfs.total_blocks()
            for superblock_id in itertools.count():
                bitmap = self.read_superblock(superblock_id)
                if bitmap.count(1) == 0:
                    continue

                for free_block in bitmap.itersearch(singleton_0_bitarray):
                    bitmap[free_block] = True
                    block_id = superblock_id * self.SUPERBLOCK_INTERVAL + free_block
                    blocks.append(block_id)

                    new_blocks += block_id >= total_size

                    number -= 1
                    if not number:
                        break

                self.write_superblock(superblock_id, bitmap)
                if not number:
                    break
            self.blockfs.new_blocks(new_blocks)
        return blocks

    @check_types
    def deallocate_blocks(self, block_ids: list):
        superblocks = {}
        with self.blockfs.lock_file(write=True):
            for block_id in block_ids:
                self.blockfs.wipe_block(block_id)
                superblock_id, block_id = divmod(block_id, self.SUPERBLOCK_INTERVAL)
                if superblock_id not in superblocks:
                    superblocks[superblock_id] = self.read_superblock(superblock_id)
                superblocks[superblock_id][block_id] = False

            for superblock_id, bitmap in superblocks.items():
                self.write_superblock(superblock_id, bitmap)

    @check_types
    def num_file_blocks(self, file_id: int):
        last_header, _, hdata = self.get_last_file_header(file_id)
        last_block = len(hdata.block_ids)
        return last_header * self.FILE_HEADER_INTERVAL + last_block + 1

    @check_types
    def block_from_offset(self, offset: int):
        if offset < self.FILE_HEADER_DATA_SIZE:
            return 0, offset
        offset -= self.FILE_HEADER_DATA_SIZE
        header, block_and_stuff = divmod(offset,
                                         self.blockfs.LOGICAL_BLOCK_SIZE * self.BLOCK_IDS_PER_HEADER
                                         + self.FILE_CONTINUATION_HEADER_DATA_SIZE)
        block, offset = divmod(block_and_stuff, self.blockfs.LOGICAL_BLOCK_SIZE)
        if block == self.BLOCK_IDS_PER_HEADER:
            header += 1
            block = 0
        else:
            block += 1
        return header * self.FILE_HEADER_INTERVAL + block, offset

    @check_types
    def extend_file_blocks(self, file_id: int, block_num: int, last_block: int=None):
        with self.blockfs.lock_file(write=True):
            if last_block is None:
                last_header, header_block_id, hdata = self.get_last_file_header(file_id)
                last_block = len(hdata.block_ids)
            else:
                last_header, last_block = divmod(last_block, self.FILE_HEADER_INTERVAL)
                header_block_id, hdata = self.get_file_header(file_id, last_header)

            num_blocks = block_num - last_header * self.FILE_HEADER_INTERVAL - last_block - 1
            assert num_blocks > 0
            new_blocks = self.allocate_blocks(num_blocks)
            new_blocks_position = 0

            while header_block_id:
                blocks = hdata.block_ids
                num_blocks = self.BLOCK_IDS_PER_HEADER - len(blocks)
                blocks.extend(new_blocks[new_blocks_position:new_blocks_position + num_blocks])
                new_blocks_position += num_blocks

                if new_blocks_position < len(new_blocks):
                    new_header_id = new_blocks[new_blocks_position]
                    new_blocks_position += 1
                else:
                    new_header_id = 0

                hdata.next_header = new_header_id

                if header_block_id == file_id:
                    packed = self.pack_file_header(hdata)
                else:
                    packed = self.pack_file_continuation_header(hdata)

                self.blockfs.write_block(header_block_id, 0, packed)

                if new_header_id:
                    hdata = FileContinuationHeader(0, header_block_id, [])
                header_block_id = new_header_id

    @check_types
    def truncate_file_blocks(self, file_id: int, block_num: int):
        assert block_num >= 1
        last_header, last_block = divmod(block_num, self.FILE_HEADER_INTERVAL)
        if last_block:
            last_block -= 1
        else:
            last_header -= 1
            last_block = self.BLOCK_IDS_PER_HEADER

        with self.blockfs.lock_file(write=True):
            header_block_id, hdata = self.get_file_header(file_id, last_header)
            blocks_to_free = hdata.block_ids[last_block:]

            next_block = hdata.next_header
            free_header = last_header + 1
            while next_block:
                data = self.blockfs.read_block(next_block)
                data = self.unpack_file_continuation_header(data)
                blocks_to_free.append(next_block)
                blocks_to_free.extend(data.block_ids)
                next_block = data.next_header
                free_header += 1

            hdata.block_ids = hdata.block_ids[:last_block]
            hdata.next_header = 0

            self.write_file_header(file_id, last_header, hdata)
            self.deallocate_blocks(blocks_to_free)

    @check_types
    def delete_file(self, file_id: int):
        with self.blockfs.lock_file(write=True):
            header_block_id, hdata = self.get_file_header(file_id, 0)
            blocks_to_free = hdata.block_ids
            blocks_to_free.append(header_block_id)

            next_block = hdata.next_header
            free_header = 1
            while next_block:
                data = self.blockfs.read_block(next_block)
                data = self.unpack_file_continuation_header(data)
                blocks_to_free.append(next_block)
                blocks_to_free.extend(data.block_ids)
                next_block = data.next_header
                free_header += 1

            self.deallocate_blocks(blocks_to_free)

    @check_types
    def truncate_file_size(self, file_id: int, size: int):
        assert size >= 0
        last_block, _ = self.block_from_offset(size)
        with self.blockfs.lock_file(write=True):
            self.truncate_file_blocks(file_id, last_block + 1)
            _, header = self.get_file_header(file_id, 0)
            header.size = size
            self.write_file_header(file_id, 0, header)

    @check_types
    def get_file_header(self, file_id: int, header_num: int):
        try:
            hcache = self.header_cache[(file_id, header_num)]
            reload, _ = self.blockfs.block_version(hcache.block_id, hcache.token)
            if not reload:
                return hcache.block_id, hcache.hdata
        except KeyError:
            pass

        block_id = None
        for offset in range(1, header_num):
            try:
                hcache = self.header_cache[(file_id, header_num - offset)]
            except KeyError:
                pass
            else:
                reload, _ = self.blockfs.block_version(hcache.block_id, hcache.token)
                if not reload:
                    start = header_num - offset
                    hdata = hcache.hdata
                    block_id = hcache.block_id
                    break
            try:
                hcache = self.header_cache[(file_id, header_num + offset)]
            except KeyError:
                pass
            else:
                reload, _ = self.blockfs.block_version(hcache.block_id, hcache.token)
                if not reload:
                    start = header_num + offset
                    hdata = hcache.hdata
                    block_id = hcache.block_id
                    break

        with self.blockfs.lock_file(write=False):
            if block_id is None:
                start = 0
                block_id = file_id
                hdata = self.read_file_header(file_id, start, block_id)

            print(header_num, start, hdata, self.read_file_header(file_id, start, block_id))

            while start < header_num:
                block_id = hdata.next_header
                assert hdata.next_header
                start += 1
                print(start)
                hdata = self.read_file_header(file_id, start, block_id)

            while start > header_num:
                block_id = hdata.prev_header
                assert hdata.prev_header
                start -= 1
                hdata = self.read_file_header(file_id, start, block_id)

        return block_id, hdata

    @check_types
    def read_file_header(self, file_id: int, header_num: int, block_id: int):
        if header_num:
            with self.blockfs.lock_file(write=False):
                data, token = self.blockfs.read_block(block_id, with_token=True)
            data = self.unpack_file_continuation_header(data)
        else:
            with self.blockfs.lock_file(write=False):
                data, token = self.blockfs.read_block(file_id, with_token=True)
            data = self.unpack_file_header(data)
            block_id = file_id

        self.header_cache[(file_id, header_num)] = HeaderCache(block_id, data, token)
        return data

    @check_types
    def write_file_header(self, file_id: int, header_num: int, data):
        if header_num:
            packed = self.pack_file_continuation_header(data)
        else:
            packed = self.pack_file_header(data)

        with self.blockfs.lock_file(write=True):
            header_block_id, _ = self.get_file_header(file_id, header_num)
            x = self.header_cache[(file_id, header_num)]
            x.token = self.blockfs.write_block(header_block_id, 0, packed, with_token=True)
            if header_num:
                x.hdata = FileContinuationHeader(*data)
            else:
                x.hdata = FileHeader(*data)

    @check_types
    def get_last_file_header(self, file_id: int):
        for start in sorted((hnum for fid, hnum in self.header_cache.keys() if fid == file_id), reverse=True):
            hcache = self.header_cache[(file_id, start)]
            reload, _ = self.blockfs.block_version(hcache.block_id, hcache.token)
            if not reload:
                break
        else:
            start = 0

        with self.blockfs.lock_file(write=False):
            block_id, data = self.get_file_header(file_id, start)
            while data.next_header:
                start += 1
                block_id, data = self.get_file_header(file_id, start)
        return start, block_id, data

    def create_new_file(self, file_type):
        with self.blockfs.lock_file(write=True):
            block_id, = self.allocate_blocks(1)
            header = FileHeader(file_type, 0, 0, [], 0, b"")
            self.blockfs.write_block(block_id, 0, self.pack_file_header(header))
        return block_id

    @check_types
    def file_data_in_block(self, block_num: int):
        header, block_num = divmod(block_num, self.FILE_HEADER_INTERVAL)

        if block_num:
            return self.blockfs.LOGICAL_BLOCK_SIZE
        elif header:
            return self.FILE_CONTINUATION_HEADER_DATA_SIZE
        else:
            return self.FILE_HEADER_DATA_SIZE

    @check_types
    def read_file_data(self, file_id: int, block_num: int):
        header, block_num = divmod(block_num, self.FILE_HEADER_INTERVAL)

        with self.blockfs.lock_file(write=False):
            header_block_id, hdata = self.get_file_header(file_id, header)
            if block_num:
                return self.blockfs.read_block(hdata.block_ids[block_num - 1])
            elif header:
                return self.blockfs.read_block(header_block_id)[self.FILE_CONTINUATION_HEADER_SIZE:]
            else:
                return self.blockfs.read_block(header_block_id)[self.FILE_HEADER_SIZE:]

    @check_types
    def write_file_data(self, file_id: int, block_num: int, offset: int, data: bytes):
        header, block_num = divmod(block_num, self.FILE_HEADER_INTERVAL)

        with self.blockfs.lock_file(write=True):
            header_block_id, hdata = self.get_file_header(file_id, header)
            if block_num:
                self.blockfs.write_block(hdata.block_ids[block_num - 1], offset, data)
            elif header:
                # Set token as we didn't change the header part
                self.header_cache[(file_id, header)].token = self.blockfs.write_block(header_block_id,
                                                                                      self.FILE_CONTINUATION_HEADER_SIZE
                                                                                      + offset,
                                                                                      data, with_token=True)
            else:
                self.header_cache[(file_id, header)].token = self.blockfs.write_block(header_block_id,
                                                                                      self.FILE_HEADER_SIZE + offset,
                                                                                      data, with_token=True)

    @check_types
    def reader(self, file_id: int, start: int=0):
        return ReadIterator(self, file_id, start)

    @check_types
    def writer(self, file_id: int, start: int=0):
        return WriteIterator(self, file_id, start)

    @check_types
    def pack_xattr_block(self, next_block: int, data: bytes):
        return self.xattr_block_header_struct.pack(next_block, data)

    @check_types
    def unpack_xattr_block(self, data: bytes):
        return self.xattr_block_header_struct.unpack(data)

    @check_types
    def write_xattrs(self, file_id: int, attrs: dict):
        data = b"\0".join(b"\0".join(x) for x in attrs.items())
        initial_data, data = data[:self.XATTR_INLINE_SIZE], data[self.XATTR_INLINE_SIZE:]

        with self.blockfs.lock_file(write=True):
            _, hdata = self.get_file_header(file_id, 0)
            hdata.xattr_inline = initial_data
            next_block = hdata.xattr_block
            blocks = []
            while next_block:
                blocks.append(next_block)
                raw_data = self.blockfs.read_block(next_block)
                next_block, _ = self.unpack_xattr_block(raw_data)

            block_data = []
            blocks_needed = len(data) // self.XATTR_BLOCK_DATA_SIZE + bool(len(data) % self.XATTR_BLOCK_DATA_SIZE)
            for i in range(blocks_needed):
                block_data.append(data[i * self.XATTR_BLOCK_DATA_SIZE:(i + 1) * self.XATTR_BLOCK_DATA_SIZE])

            if len(blocks) < blocks_needed:
                blocks.extend(self.allocate_blocks(len(block_data) - len(blocks)))
            elif blocks_needed < len(blocks):
                rm_blocks, blocks = blocks[blocks_needed:], blocks[:blocks_needed]
                self.deallocate_blocks(rm_blocks)

            for i, (block, data) in enumerate(zip(blocks, block_data)):
                raw_data = self.pack_xattr_block(0 if i + 1 == len(blocks) else blocks[i + 1], data)
                self.blockfs.write_block(block, 0, raw_data)

            hdata.xattr_block = blocks[0] if blocks else 0
            self.write_file_header(file_id, 0, hdata)

    @check_types
    def read_xattrs(self, file_id: int):
        with self.blockfs.lock_file(write=False):
            _, hdata = self.get_file_header(file_id, 0)
            data = [hdata.xattr_inline]
            next_block = hdata.xattr_block
            while next_block:
                raw_data = self.blockfs.read_block(next_block)
                next_block, data_part = self.unpack_xattr_block(raw_data)
                data.append(data_part)
        data = b"".join(data).rstrip(b"\0")
        if data:
            iter_items = iter(data.split(b"\0"))
            return {i: next(iter_items) for i in iter_items}
        return {}

    @check_types
    def lookup_xattr(self, file_id: int, key: bytes):
        return self.read_xattrs(file_id)[key]

    @check_types
    def set_xattr(self, file_id: int, key: bytes, value: bytes, create_only: bool=False, replace_only: bool=False):
        data = self.read_xattrs(file_id)
        if create_only and key in data:
            raise KeyAlreadyExists()
        if replace_only and key not in data:
            raise KeyDoesNotExist()
        data[key] = value
        self.write_xattrs(file_id, data)

    @check_types
    def delete_xattr(self, file_id: int, key: bytes):
        data = self.read_xattrs(file_id)
        del data[key]
        self.write_xattrs(file_id, data)
