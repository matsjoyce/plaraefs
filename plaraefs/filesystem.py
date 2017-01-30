import array
import bitarray
import recordclass
import itertools
import sys
import lru


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

    @property
    def FILE_HEADER_SIZE(self):
        return (1 + self.GROUP_TAG_SIZE + self.FILESIZE_SIZE +
                (self.BLOCK_IDS_PER_HEADER + 1) * self.blockfs.BLOCK_ID_SIZE)

    @property
    def FILE_HEADER_DATA_SIZE(self):
        return self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_HEADER_SIZE

    @property
    def FILE_CONTINUATION_HEADER_SIZE(self):
        return (self.BLOCK_IDS_PER_HEADER + 2) * self.blockfs.BLOCK_ID_SIZE

    @property
    def FILE_CONTINUATION_HEADER_DATA_SIZE(self):
        return self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_CONTINUATION_HEADER_SIZE

    @property
    def FILE_HEADER_INTERVAL(self):
        return self.BLOCK_IDS_PER_HEADER + 1

    @property
    def SUPERBLOCK_INTERVAL(self):
        return self.blockfs.LOGICAL_BLOCK_SIZE * 8

    def __init__(self, blockfs: BlockFileSystem):
        self.blockfs = blockfs
        self.header_cache = lru.LRU(1024)

    @classmethod
    @check_types
    def initialise(cls, blockfs: BlockFileSystem):
        blocks = blockfs.new_blocks(2)
        assert blocks == [0, 1]
        fs = cls(blockfs)
        fs.write_new_superblock(0)

    def make_block_id_container(self, default=()):
        if self.blockfs.BLOCK_ID_SIZE == 8:
            return array.array("Q", default)
        else:  # pragma: no cover
            raise RuntimeError("No array size")

    @check_types
    def unpack_block_ids(self, ids: bytes):
        arr = self.make_block_id_container()
        arr.frombytes(ids)
        if sys.byteorder != "little":  # pragma: no cover
            arr.byteswap()
        return arr

    @check_types
    def pack_block_ids(self, ids: array.array):
        if sys.byteorder != "little":  # pragma: no cover
            arr = ids.copy()
            arr.byteswap()
        else:
            arr = ids
        return arr.tobytes()

    @check_types
    def unpack_file_header(self, data: bytes):
        start = 0
        mode = data[0]
        start += 1
        group_tag = data[start:start + self.GROUP_TAG_SIZE].strip(b"\0")
        start += self.GROUP_TAG_SIZE
        size = int.from_bytes(data[start:start + self.FILESIZE_SIZE], "little", signed=False)
        start += self.FILESIZE_SIZE
        ids_data = data[start:start + self.blockfs.BLOCK_ID_SIZE * (self.BLOCK_IDS_PER_HEADER + 1)]
        block_ids = self.unpack_block_ids(ids_data)
        next_header = block_ids.pop(0)
        if not block_ids[-1]:
            block_ids = block_ids[:block_ids.index(0)]
        return FileHeader(mode, group_tag, size, next_header, block_ids)

    @check_types
    def pack_file_header(self, header: FileHeader):
        arr = self.make_block_id_container()
        arr.append(header.next_header)
        arr.extend(header.block_ids)
        arr.extend([0] * (self.BLOCK_IDS_PER_HEADER + 1 - len(arr)))
        block_ids = self.pack_block_ids(arr)

        return b"".join((bytes((header.mode,)),
                         header.group_tag.ljust(self.GROUP_TAG_SIZE, b"\0"),
                         header.size.to_bytes(self.FILESIZE_SIZE, "little", signed=False),
                         block_ids))

    @check_types
    def unpack_file_continuation_header(self, data: bytes):
        block_ids = self.unpack_block_ids(data[:self.blockfs.BLOCK_ID_SIZE * (self.BLOCK_IDS_PER_HEADER + 2)])
        next_header = block_ids.pop(0)
        prev_header = block_ids.pop(0)
        if not block_ids[-1]:
            block_ids = block_ids[:block_ids.index(0)]
        return FileContinuationHeader(next_header, prev_header, block_ids)

    @check_types
    def pack_file_continuation_header(self, header: FileContinuationHeader):
        arr = self.make_block_id_container()
        arr.append(header.next_header)
        arr.append(header.prev_header)
        arr.extend(header.block_ids)
        arr.extend([0] * (self.BLOCK_IDS_PER_HEADER + 2 - len(arr)))
        return self.pack_block_ids(arr)

    @check_types
    def read_superblock(self, superblock_id: int):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        data = self.blockfs.read_block(block_id)
        arr = bitarray.bitarray()
        arr.frombytes(data)
        return arr

    @check_types
    def write_superblock(self, superblock_id: int, bitmap: bitarray.bitarray):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        self.blockfs.write_block(block_id, bitmap.tobytes())

    @check_types
    def write_new_superblock(self, superblock_id: int):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        self.blockfs.write_block(block_id, b"\x80" + b"\0" * (self.blockfs.LOGICAL_BLOCK_SIZE - 1))

    @check_types
    def number_free_blocks(self, superblock_id: int):
        bitmap = self.read_superblock(superblock_id)
        return bitmap.count(False)

    @check_types
    def allocate_blocks(self, number: int):
        blocks = self.make_block_id_container()
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
    def deallocate_blocks(self, block_ids: array.array):
        superblocks = {}
        with self.blockfs.lock_file(write=True):
            for block_id in block_ids:
                self.blockfs.wipe_block(block_id)
                superblock_id = block_id // self.SUPERBLOCK_INTERVAL
                if superblock_id not in superblocks:
                    superblocks[superblock_id] = self.read_superblock(superblock_id)
                superblocks[superblock_id][block_id % self.SUPERBLOCK_INTERVAL] = False

            for superblock_id, bitmap in superblocks.items():
                self.write_superblock(superblock_id, bitmap)

    def num_file_blocks(self, file_id):
        last_header, header_block_id, hdata = self.get_last_file_header(file_id)
        last_block = len(hdata.block_ids)
        return last_header * self.FILE_HEADER_INTERVAL + last_block + 1

    @check_types
    def extend_file_blocks(self, file_id: int, block_num: int, last_block: int=None):
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

            old_data = self.blockfs.read_block(header_block_id)
            if old_data is None:
                data = b"".join((packed, b"\0" * (self.blockfs.LOGICAL_BLOCK_SIZE - len(packed))))
            else:
                data = b"".join((packed, old_data[len(packed):]))

            self.blockfs.write_block(header_block_id, data)

            if new_header_id:
                hdata = FileContinuationHeader(0, header_block_id, self.make_block_id_container())
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

        if header_block_id == file_id:
            packed = self.pack_file_header(hdata)
        else:
            packed = self.pack_file_continuation_header(hdata)

        old_data = self.blockfs.read_block(header_block_id)
        data = b"".join((packed, old_data[len(packed):]))

        self.blockfs.write_block(header_block_id, data)

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
            block_id = file_id
            with self.blockfs.lock_file(write=False):
                data, token = self.blockfs.read_block(file_id, with_token=True)
            data = self.unpack_file_header(data)

        self.header_cache[(file_id, header_num)] = HeaderCache(block_id, data, token)
        return block_id, data

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
            header = FileHeader(0, b"", 0, 0, self.make_block_id_container())
            data = b"".join((self.pack_file_header(header), b"\0" * self.FILE_HEADER_DATA_SIZE))
            self.blockfs.write_block(block_id, data)
        return block_id

    @check_types
    def file_data_in_block(self, block_num: int):
        header, block_num = divmod(block_num, self.FILE_HEADER_INTERVAL)
        if block_num == 0:
            if header == 0:
                return self.FILE_HEADER_DATA_SIZE
            return self.FILE_CONTINUATION_HEADER_DATA_SIZE
        return self.blockfs.LOGICAL_BLOCK_SIZE

    @check_types
    def read_file_data(self, file_id: int, block_num: int):
        data_len = self.file_data_in_block(block_num)
        header, block_num = divmod(block_num, self.FILE_HEADER_INTERVAL)

        with self.blockfs.lock_file(write=False):
            header_block_id, hdata = self.get_file_header(file_id, header)
            if not block_num:
                return self.blockfs.read_block(header_block_id)[-data_len:]
            else:
                return self.blockfs.read_block(hdata.block_ids[block_num - 1])

    @check_types
    def write_file_data(self, file_id: int, block_num: int, data: bytes):
        assert len(data) == self.file_data_in_block(block_num)
        header, block_num = divmod(block_num, self.FILE_HEADER_INTERVAL)

        with self.blockfs.lock_file(write=True):
            header_block_id, hdata = self.get_file_header(file_id, header)
            if block_num:
                self.blockfs.write_block(hdata.block_ids[block_num - 1], data)
            else:
                old_data = self.blockfs.read_block(header_block_id)
                if header:
                    data = b"".join((old_data[:self.FILE_CONTINUATION_HEADER_SIZE], data))
                else:
                    data = b"".join((old_data[:self.FILE_HEADER_SIZE], data))
                token = self.blockfs.write_block(header_block_id, data, with_token=True)
                try:
                    # Set token as we didn't change the header part
                    self.header_cache[(file_id, header)].token = token
                except KeyError:
                    pass
