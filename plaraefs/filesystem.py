import bitarray
import recordclass
import itertools
import lru
import struct


from .blockfilesystem import BlockFileSystem
from .utils import check_types


FileHeader = recordclass.recordclass("FileHeader", ("mode", "group_tag", "size", "next_header", "block_ids"))
FileContinuationHeader = recordclass.recordclass("FileContinuationHeader", ("next_header", "prev_header", "block_ids"))
HeaderCache = recordclass.recordclass("HeaderCache", ("block_id", "hdata", "token"))

singleton_0_bitarray = bitarray.bitarray("0")
singleton_1_bitarray = bitarray.bitarray("1")


class FileSystem:
    FILENAME_SIZE = 256
    GROUP_TAG_SIZE = 16
    FILESIZE_SIZE = 8
    BLOCK_IDS_PER_HEADER = 32
    FILE_HEADER_INTERVAL = BLOCK_IDS_PER_HEADER + 1

    def __init__(self, blockfs: BlockFileSystem):
        self.blockfs = blockfs
        self.header_cache = lru.LRU(1024)

        self.FILE_HEADER_SIZE = (1 + self.GROUP_TAG_SIZE + self.FILESIZE_SIZE +
                                 (self.BLOCK_IDS_PER_HEADER + 1) * self.blockfs.BLOCK_ID_SIZE)
        self.FILE_HEADER_DATA_SIZE = self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_HEADER_SIZE
        self.FILE_CONTINUATION_HEADER_SIZE = (self.BLOCK_IDS_PER_HEADER + 2) * self.blockfs.BLOCK_ID_SIZE
        self.FILE_CONTINUATION_HEADER_DATA_SIZE = self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_CONTINUATION_HEADER_SIZE
        self.SUPERBLOCK_INTERVAL = self.blockfs.LOGICAL_BLOCK_SIZE * 8

        self.file_header_struct = struct.Struct(f"<B{self.GROUP_TAG_SIZE}s{self.BLOCK_IDS_PER_HEADER + 2}Q")
        self.file_continuation_header_struct = struct.Struct(f"<{self.BLOCK_IDS_PER_HEADER + 2}Q")

    @classmethod
    @check_types
    def initialise(cls, blockfs: BlockFileSystem):
        blocks = blockfs.new_blocks(2)
        assert blocks == [0, 1]
        fs = cls(blockfs)
        fs.write_new_superblock(0)

    @check_types
    def unpack_file_header(self, data: bytes):
        mode, group_tag, size, next_header, *block_ids = self.file_header_struct.unpack(data[:self.FILE_HEADER_SIZE])
        if not block_ids[-1]:
            block_ids = block_ids[:block_ids.index(0)]
        return FileHeader(mode, group_tag.rstrip(b"\0"), size, next_header, block_ids)

    @check_types
    def pack_file_header(self, header: FileHeader):
        return self.file_header_struct.pack(header.mode,
                                            header.group_tag,
                                            header.size,
                                            header.next_header,
                                            *header.block_ids,
                                            *([0] * (self.BLOCK_IDS_PER_HEADER - len(header.block_ids))))

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
        arr = bitarray.bitarray()
        arr.frombytes(self.blockfs.read_block(block_id))
        return arr

    @check_types
    def write_superblock(self, superblock_id: int, bitmap: bitarray.bitarray):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        self.blockfs.write_block(block_id, 0, bitmap.tobytes())

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
                block_id, data = self.get_file_header(file_id, free_header)
                blocks_to_free.append(block_id)
                blocks_to_free.extend(data.block_ids)
                next_block = data.next_header
                free_header += 1

            hdata.block_ids = hdata.block_ids[:last_block]
            hdata.next_header = 0

            self.write_file_header(file_id, last_header, hdata)
            self.deallocate_blocks(blocks_to_free)

    @check_types
    def get_file_header(self, file_id: int, header_num: int):
        try:
            hcache = self.header_cache[(file_id, header_num)]
            reload, _ = self.blockfs.block_version(hcache.block_id, hcache.token)
            if not reload:
                return hcache.block_id, hcache.hdata
        except KeyError:
            pass

        if header_num:
            with self.blockfs.lock_file(write=False):
                block_id = self.get_file_header(file_id, header_num - 1)[1].next_header
                data, token = self.blockfs.read_block(block_id, with_token=True)
            data = self.unpack_file_continuation_header(data)
        else:
            with self.blockfs.lock_file(write=False):
                data, token = self.blockfs.read_block(file_id, with_token=True)
            data = self.unpack_file_header(data)
            block_id = file_id

        self.header_cache[(file_id, header_num)] = HeaderCache(block_id, data, token)
        return block_id, data

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

    def create_new_file(self):
        with self.blockfs.lock_file(write=True):
            block_id, = self.allocate_blocks(1)
            header = FileHeader(0, b"", 0, 0, [])
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
