import array
import bitarray
import collections
import itertools
import sys


from .blockfilesystem import BlockFileSystem


FileHeader = collections.namedtuple("FileHeader", ("mode", "group_tag", "size", "continuation_block_id", "block_ids"))
FileContinuationHeader = collections.namedtuple("FileContinuationHeader", ("continuation_block_id", "block_ids"))

singleton_0_bitarray = bitarray.bitarray("0")
singleton_1_bitarray = bitarray.bitarray("1")


class FileSystem:
    FILENAME_SIZE = 256
    GROUP_TAG_SIZE = 16
    FILESIZE_SIZE = 8
    BLOCK_IDS_PER_HEADER = 32

    @property
    def FILE_HEADER_SIZE(self):
        return 1 + self.GROUP_TAG_SIZE + self.FILESIZE_SIZE + (self.BLOCK_IDS_PER_HEADER + 1) * self.blockfs.BLOCK_ID_SIZE

    @property
    def FILE_HEADER_DATA_SIZE(self):
        return self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_HEADER_SIZE

    @property
    def FILE_CONTINUATION_HEADER_SIZE(self):
        return (self.BLOCK_IDS_PER_HEADER + 1) * self.blockfs.BLOCK_ID_SIZE

    @property
    def FILE_CONTINUATION_HEADER_DATA_SIZE(self):
        return self.blockfs.LOGICAL_BLOCK_SIZE - self.FILE_CONTINUATION_HEADER_SIZE

    @property
    def SUPERBLOCK_INTERVAL(self):
        return self.blockfs.LOGICAL_BLOCK_SIZE * 8

    def __init__(self, blockfs: BlockFileSystem):
        self.blockfs = blockfs

    @classmethod
    def initialise(cls, blockfs: BlockFileSystem):
        blocks = blockfs.new_blocks(2)
        assert blocks == [0, 1]
        fs = cls(blockfs)
        fs.write_new_superblock(0)

    def make_block_id_container(self):
        if self.blockfs.BLOCK_ID_SIZE == 8:
            return array.array("Q")
        else:
            raise RuntimeError("No array size")

    def unpack_block_ids(self, ids):
        arr = self.make_block_id_container()
        arr.frombytes(ids)
        if sys.byteorder != "little":
            arr.byteswap()
        return arr

    def pack_block_ids(self, ids):
        if sys.byteorder != "little":
            arr = ids.copy()
            arr.byteswap()
        else:
            arr = ids
        return arr.tobytes()

    def read_file_header(self, data):
        start = 0
        mode = data[0]
        start += 1
        group_tag = data[start:start + self.GROUP_TAG_SIZE].strip(b"\0")
        start += self.GROUP_TAG_SIZE
        size = int.from_bytes(data[start:start + self.FILESIZE_SIZE], "little", signed=False)
        start += self.FILESIZE_SIZE
        ids_data = data[start:start + self.blockfs.BLOCK_ID_SIZE * (self.BLOCK_IDS_PER_HEADER + 1)]
        continuation_block_id, *block_ids = self.unpack_block_ids(ids_data)
        start += self.blockfs.BLOCK_ID_SIZE * (self.BLOCK_IDS_PER_HEADER + 1)
        remaining_data = data[start:]
        return FileHeader(mode, group_tag, size, continuation_block_id, block_ids), remaining_data

    def write_file_header(self, header, remaining_data):
        arr = self.make_block_id_container()
        arr.append(header.continuation_block_id)
        arr.extend(header.block_ids)
        block_ids = self.pack_block_ids(arr)

        return b"".join((bytes((header.mode,)),
                         header.group_tag.ljust(self.GROUP_TAG_SIZE, b"\0"),
                         header.size.to_bytes(self.FILESIZE_SIZE, "little", signed=False),
                         block_ids,
                         remaining_data))

    def read_file_continuation_header(self, data):
        start = 0
        ids_data = data[start:start + self.blockfs.BLOCK_ID_SIZE * (self.BLOCK_IDS_PER_HEADER + 1)]
        continuation_block_id, *block_ids = self.unpack_block_ids(ids_data)
        start += self.blockfs.BLOCK_ID_SIZE * (self.BLOCK_IDS_PER_HEADER + 1)
        remaining_data = data[start:]
        return FileContinuationHeader(continuation_block_id, block_ids), remaining_data

    def write_file_continuation_header(self, header, remaining_data):
        arr = self.make_block_id_container()
        arr.append(header.continuation_block_id)
        arr.extend(header.block_ids)
        return b"".join((self.pack_block_ids(arr),
                         remaining_data))

    def read_superblock(self, superblock_id):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        data = self.blockfs.read_block(block_id)
        arr = bitarray.bitarray()
        arr.frombytes(data)
        return arr

    def write_superblock(self, superblock_id, bitmap):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        self.blockfs.write_block(block_id, bitmap.tobytes())

    def write_new_superblock(self, superblock_id):
        block_id = superblock_id * self.SUPERBLOCK_INTERVAL
        self.blockfs.write_block(block_id, b"\x80" + b"\0" * (self.blockfs.LOGICAL_BLOCK_SIZE - 1))

    def number_free_blocks(self, superblock_id):
        bitmap = self.read_superblock(superblock_id)
        return bitmap.count(False)

    def allocate_blocks(self, number):
        blocks = self.make_block_id_container()
        with self.blockfs.lock_file(write=True):
            for superblock_id in itertools.count():
                bitmap = self.read_superblock(superblock_id)
                for free_block in bitmap.itersearch(singleton_0_bitarray):
                    bitmap[free_block] = True
                    blocks.append(superblock_id * self.SUPERBLOCK_INTERVAL + free_block)
                    number -= 1
                    if not number:
                        break
                self.write_superblock(superblock_id, bitmap)
                if not number:
                    break
        return blocks

    def deallocate_blocks(self, block_ids):
        superblocks = {}
        with self.blockfs.lock_file(write=True):
            for block_id in block_ids:
                superblock_id = block_id // self.SUPERBLOCK_INTERVAL
                if superblock_id not in superblocks:
                    superblocks[superblock_id] = self.read_superblock(superblock_id)
                superblocks[superblock_id][block_id % self.SUPERBLOCK_INTERVAL] = False

            for superblock_id, bitmap in superblocks.items():
                self.write_superblock(superblock_id, bitmap)

    def create_new_file(self):
        with self.blockfs.lock_file(write=True):
            block_id, = self.allocate_blocks(1)
            header = FileHeader(0, b"", 0, 0, [0] * self.BLOCK_IDS_PER_HEADER)
            data = self.write_file_header(header, b"\0" * self.FILE_HEADER_DATA_SIZE)
            self.blockfs.write_block(block_id, data)
        return block_id

    def write_file_iter(self, start: int=0):
        unflushed_data = []
        unflushed_data_length = 0
        unflushed_data_first_item_start = 0

        while True:
            data = yield
            if data is None:
                flush_now = True
            else:
                unflushed_data.append(data)
                unflushed_data_length += len(data)



