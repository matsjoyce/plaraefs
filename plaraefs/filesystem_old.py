import array
import collections
import msgpack
import os
import sqlite3
import sys

from .blockfilesystem import BlockFileSystem




class FileSystem:
    FILE_ID_SIZE = 8
    WRITE_ITER_BUFFER_SIZE = 8

    def __init__(self, fname, key):
        self.blockfs = BlockFileSystem(fname, key)



    def allocate_blocks(self, number):
        free_blocks = self.free_blocks()
        allocated_blocks, free_blocks = free_blocks[:number], free_blocks[number:]
        if len(allocated_blocks) != number:
            allocated_blocks.extend(self.new_blocks(number - len(allocated_blocks)))
        self.set_free_blocks(free_blocks)
        return allocated_blocks

    def deallocate_blocks(self, block_ids):
        free_blocks = self.free_blocks()
        for block_id in block_ids:
            free_blocks.append(block_id)
            self.wipe_block(block_id)
        self.set_free_blocks(free_blocks)
    @check_types
    def file_blocks(self, file_id: int):
        cipher_raw_blocks, = self.conn.execute("select blocks from Files where id = ?", (file_id,)).fetchone()
        raw_blocks = self.decrypt(cipher_raw_blocks)
        return self.decode_block_ids(raw_blocks)

    @check_types
    def set_file_blocks(self, file_id: int, blocks):
        raw_blocks = self.encode_block_ids(blocks)
        cipher_raw_blocks = self.encrypt(raw_blocks)
        self.conn.execute("update Files set blocks = ? where id = ?", (cipher_raw_blocks, file_id))
        self.increment_version(b"files")

    def free_blocks(self):
        cipher_raw_blocks, = self.conn.execute("select value from Util where key = ?", (b"free_blocks",)).fetchone()
        if cipher_raw_blocks == b"":
            return self.make_block_id_container()
        raw_blocks = self.decrypt(cipher_raw_blocks)
        assert len(raw_blocks) % BLOCK_ID_SIZE == 0
        return self.decode_block_ids(raw_blocks)

    def set_free_blocks(self, blocks):
        raw_blocks = self.encode_block_ids(blocks)
        cipher_raw_blocks = self.encrypt(raw_blocks)
        self.conn.execute("update Util set value = ? where key = ?", (cipher_raw_blocks, b"free_blocks"))

    @check_types
    def __init__(self, fname, key: bytes):
        self.lock = threading.RLock()
        self.key = key
        assert len(self.key) == KEY_SIZE

        self.fname = pathlib.Path(fname)
        self.thread_to_file = {}
        assert self.fname.suffix == FS_EXT
        assert self.fname.stat().st_size % TOTAL_BLOCK_SIZE == 0

        self.db_fname = self.fname.with_suffix(FS_DB_EXT)
        self.conn = self.db_thread = None

        self.file_name_cache = self.reverse_file_name_cache = None
        self.file_name_version = -1
        self.versions_to_increment = set()
        self.number_of_active_write_iterators = 0

        self.is_in_exclusive_transaction = False

    @staticmethod
    def db_connect(fname):
        conn = sqlite3.connect(str(fname), check_same_thread=False, isolation_level=None)
        conn.execute("pragma foreign_keys = ON")
        return conn

    @staticmethod
    def generate_file_id():
        return int.from_bytes(os.urandom(FILE_ID_SIZE), "little", signed=True)

    @staticmethod
    def make_block_id_container():
        return array.array("Q")

    @classmethod
    @check_types
    def decode_block_ids(cls, block_ids: bytes):
        assert len(block_ids) % BLOCK_ID_SIZE == 0
        arr = cls.make_block_id_container()
        arr.frombytes(block_ids)
        if sys.byteorder != "little":
            arr.byteswap()
        return arr

    @staticmethod
    @check_types
    def encode_block_ids(block_ids: array.array):
        if sys.byteorder != "little":
            arr = block_ids.copy()
            arr.byteswap()
        else:
            arr = block_ids
        return arr.tobytes()

    @classmethod
    def initialise(cls, fname):
        fname = pathlib.Path(fname)
        assert fname.suffix == FS_EXT
        with open(fname, "x") as f:
            pass
        conn = cls.db_connect(fname.with_suffix(FS_DB_EXT))
        conn.executescript(SCHEMA)
        conn.execute("insert into Util (key, value) values (?, ?)",
                     (b"free_blocks", b""))
        conn.executemany("insert into Versions (key, version) values (?, ?)",
                         [(b"names", 0),
                          (b"files", 0),
                          (b"data", 0),
                          ])
        conn.commit()

    @contextlib.contextmanager
    def db_connected(self):
        # make sure the database is opened in the current thread
        with self.lock:
            if threading.current_thread() != self.db_thread:
                if self.conn is not None:
                    if self.conn.in_transaction:
                        raise RuntimeError("Cannot replace db in new thead, db busy")
                    self.conn.close()
                self.conn = self.db_connect(self.db_fname)
                self.db_thread = threading.current_thread()
            yield

    @contextlib.contextmanager
    def exclusive_transaction(self):
        # make sure no one else can read or write to the db during this CM
        if self.is_in_exclusive_transaction:
            yield
            return

        with self.db_connected():
            self.conn.execute("begin exclusive transaction")
            self.is_in_exclusive_transaction = True
            try:
                yield
                self.flush_versions()
            except:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()
            finally:
                self.is_in_exclusive_transaction = False

    def with_db_connected(func):
        @functools.wraps(func)
        def wdc_wrapper(self, *args, **kwargs):
            with self.db_connected():
                return func(self, *args, **kwargs)
        return wdc_wrapper

    def with_exclusive_transaction(func):
        @functools.wraps(func)
        def wet_wrapper(self, *args, **kwargs):
            with self.exclusive_transaction():
                return func(self, *args, **kwargs)
        return wet_wrapper

    @contextlib.contextmanager
    def main_file(self):
        with self.lock:
            thread = threading.current_thread()
            try:
                f = self.thread_to_file[thread]
            except KeyError:
                f = self.thread_to_file[thread] = open(self.fname, "r+b")
            try:
                yield f
            finally:
                f.flush()

    @check_types
    @with_db_connected
    def new_version(self, name: bytes, old_value: int):
        current = self.conn.execute("select version from Versions where key = ?", (name,)).fetchone()[0]
        return old_value != current, current

    @check_types
    def increment_version(self, name: bytes):
        assert self.is_in_exclusive_transaction
        self.versions_to_increment.add(name)

    def flush_versions(self):
        for name in self.versions_to_increment:
            current = self.conn.execute("select version from Versions where key = ?",
                                        (name,)).fetchone()[0]
            self.conn.execute("update Versions set version = ? where key = ?",
                              ((current + 1) % 2 ** 64, name,))
        self.versions_to_increment.clear()

    # file level operations

    @with_db_connected
    def reload_file_name_cache(self):
        reload, self.file_name_version = self.new_version(b"names", self.file_name_version)
        if reload:
            self.file_name_cache = {}
            self.reverse_file_name_cache = collections.defaultdict(set)
            for file_id, ciphername in self.conn.execute("select id, name from Names"):
                name = self.decrypt(ciphername)
                self.file_name_cache[name] = file_id
                self.reverse_file_name_cache[file_id].add(name)

    @check_types
    @with_db_connected
    def file_id_from_name(self, name: bytes):
        # returns the `file_id` for `name`
        self.reload_file_name_cache()
        try:
            return self.file_name_cache[name]
        except:
            raise FileNotFoundError(name)

    @with_db_connected
    def list_file_ids(self):
        self.reload_file_name_cache()
        return set(self.reverse_file_name_cache)

    @with_db_connected
    def list_file_names(self):
        self.reload_file_name_cache()
        return set(self.file_name_cache)

    @check_types
    @with_db_connected
    def file_names(self, file_id: int):
        return self.reverse_file_name_cache[file_id]

    @check_types
    @with_exclusive_transaction
    def remove_file_name(self, file_id: int, name: bytes):
        cipher_names = self.conn.execute("select name from Names where id = ?", (file_id,))
        for cipher_name, in cipher_names:
            if self.decrypt(cipher_name) == name:
                self.conn.execute("delete from Names where id = ? and name = ?",
                                  (file_id, cipher_name))
                self.increment_version(b"names")
                return
        raise FileNotFoundError(name)

    @check_types
    @with_exclusive_transaction
    def add_file_name(self, file_id: int, name: bytes):
        try:
            self.file_id_from_name(name)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError()

        cipher_name = self.encrypt(name)

        self.conn.execute("insert into Names (name, id) values (?, ?)", (cipher_name, file_id))
        self.increment_version(b"names")

    @check_types
    @with_exclusive_transaction
    def create_file(self, name: bytes, exclusive=True):
        # create a new file, and raise FileExistsError if `name` already exists
        try:
            file_id = self.file_id_from_name(name)
        except FileNotFoundError:
            pass
        else:
            if not exclusive:
                return file_id
            raise FileExistsError()

        current_ids = {cid for cid, in self.conn.execute("select id from Files")}
        file_id = self.generate_file_id()
        # just in case we have a collision...
        while file_id in current_ids:
            file_id = self.generate_file_id()
        cipher_name = self.encrypt(name)
        cipher_blocks = self.encrypt(b"")
        metadata = {"size": 0
                    }
        serialised_metadata = msgpack.dumps(metadata)
        cipher_metadata = self.encrypt(serialised_metadata)

        self.conn.execute("insert into Files (id, metadata, blocks) values (?, ?, ?)",
                          (file_id, cipher_metadata, cipher_blocks))
        self.increment_version(b"files")

        self.conn.execute("insert into Names (name, id) values (?, ?)", (cipher_name, file_id))
        self.increment_version(b"names")

        return file_id

    @check_types
    @with_exclusive_transaction
    def delete_file(self, file_id: int):
        file_blocks = self.file_blocks(file_id)
        free_blocks = self.free_blocks()
        free_blocks.extend(file_blocks)
        self.set_free_blocks(free_blocks)

        self.conn.execute("delete from Names where id = ?", (file_id,))
        self.increment_version(b"names")
        self.conn.execute("delete from Files where id = ?", (file_id,))
        self.increment_version(b"files")

        for block_id in file_blocks:
            self.wipe_block(block_id)

    @check_types
    @with_db_connected
    def file_metadata(self, file_id: int):
        cipher_metadata, = self.conn.execute("select metadata from Files where id = ?", (file_id,)).fetchone()
        serialised_metadata = self.decrypt(cipher_metadata)
        return msgpack.loads(serialised_metadata, use_list=False, encoding="utf-8")

    @check_types
    @with_exclusive_transaction
    def set_file_metadata(self, file_id: int, metadata):
        serialised_metadata = msgpack.dumps(metadata, use_bin_type=True)
        cipher_metadata = self.encrypt(serialised_metadata)
        self.conn.execute("update Files set metadata = ? where id = ?", (cipher_metadata, file_id))
        self.increment_version(b"files")

    @check_types
    def write_file_iter(self, file_id: int, start: int=0):
        self.number_of_active_write_iterators += 1
        files_version = data_version = -1

        unflushed_data = []
        unflushed_data_length = 0
        unflushed_data_first_item_start = 0
        flush_now = False

        next_block_length = ceildiv(start, DATA_BLOCK_SIZE) * DATA_BLOCK_SIZE - start
        if next_block_length == 0:
            next_block_length = DATA_BLOCK_SIZE

        while True:
            if flush_now:
                self.number_of_active_write_iterators -= 1
                return

            data = yield
            if data is None:
                flush_now = True
            else:
                unflushed_data.append(data)
                unflushed_data_length += len(data)

            unflushed_end = start + unflushed_data_length
            buffer_size = next_block_length + (WRITE_ITER_BUFFER_SIZE - 1) * DATA_BLOCK_SIZE
            if unflushed_data_length < buffer_size and not (flush_now and unflushed_data_length):
                continue

            with self.exclusive_transaction():
                reload, files_version = self.new_version(b"files", files_version)
                if reload:
                    current_blocks = self.file_blocks(file_id)
                    metadata = self.file_metadata(file_id)

                while unflushed_data_length >= next_block_length or flush_now and unflushed_data_length:
                    block_id, offset = divmod(start, DATA_BLOCK_SIZE)
                    collected_length = 0
                    collected_data = []
                    while collected_length < next_block_length and unflushed_data_length:
                        wanted_length = next_block_length - collected_length
                        chunk = unflushed_data[0]

                        if len(chunk) - unflushed_data_first_item_start <= wanted_length:
                            unflushed_data.pop(0)
                            collected_chunk = chunk[unflushed_data_first_item_start:]
                            unflushed_data_first_item_start = 0
                        else:
                            collected_chunk = chunk[unflushed_data_first_item_start:unflushed_data_first_item_start + wanted_length]
                            unflushed_data_first_item_start += len(collected_chunk)
                            assert unflushed_data_first_item_start < len(chunk)

                        collected_data.append(collected_chunk)
                        collected_length += len(collected_chunk)
                        unflushed_data_length -= len(collected_chunk)

                    block_data = b"".join(collected_data)
                    assert len(block_data) == next_block_length or flush_now
                    assert offset + next_block_length == DATA_BLOCK_SIZE or flush_now

                    # since we are allocating, allocate all the blocks we need to flush all current data
                    extra_blocks_needed = ceildiv(unflushed_end, DATA_BLOCK_SIZE) - len(current_blocks)
                    if extra_blocks_needed > 0:
                        current_blocks.extend(self.allocate_blocks(extra_blocks_needed))
                        self.set_file_blocks(file_id, current_blocks)
                    assert len(current_blocks) >= block_id + 1
                    start += next_block_length

                    if offset != 0 or len(block_data) != next_block_length:
                        old_block_data = self.read_block(current_blocks[block_id])
                        add_on_end = DATA_BLOCK_SIZE - offset - len(block_data)
                        start -= add_on_end
                        if old_block_data is None:
                            block_data = b"".join([b"\0" * offset, block_data, b"\0" * add_on_end])
                        else:
                            block_data = b"".join([old_block_data[:offset], block_data, old_block_data[-add_on_end:]])

                    self.write_block(current_blocks[block_id], block_data)
                    if start > metadata["size"]:
                        metadata["size"] = start
                        self.set_file_metadata(file_id, metadata)

                    next_block_length = ceildiv(start, DATA_BLOCK_SIZE) * DATA_BLOCK_SIZE - start
                    if next_block_length == 0:
                        next_block_length = DATA_BLOCK_SIZE

    @check_types
    @with_db_connected
    def read_file_iter(self, file_id: int, start: int=0):
        files_version = data_version = -1

        data = b""
        data_chunks = []
        while True:
            amount = yield data
            if amount < 1:
                yield b""
                continue

            reload, files_version = self.new_version(b"files", files_version)
            if reload:
                current_blocks = self.file_blocks(file_id)
                metadata = self.file_metadata(file_id)

            reload, data_version = self.new_version(b"data", data_version)
            if reload:
                current_block = block_data = None

            end = min(start + amount, metadata["size"])
            if start == end:
                yield b""
                continue

            first_block, first_block_start = divmod(start, DATA_BLOCK_SIZE)
            last_block, last_block_end = divmod(end, DATA_BLOCK_SIZE)
            if last_block_end == 0:
                last_block -= 1
                last_block_end = DATA_BLOCK_SIZE

            assert first_block <= last_block < len(current_blocks)
            assert end - start == DATA_BLOCK_SIZE * (last_block - first_block - 1) + DATA_BLOCK_SIZE - first_block_start + last_block_end

            for block_index in range(first_block, last_block + 1):
                block_data_start = 0
                block_data_end = DATA_BLOCK_SIZE
                if block_index == first_block:
                    block_data_start = first_block_start
                if block_index == last_block:
                    block_data_end = last_block_end

                if current_blocks[block_index] != current_block:
                    current_block = current_blocks[block_index]
                    block_data = self.read_block(current_blocks[block_index])
                if block_data is not None:
                    data_chunks.append(block_data[block_data_start:block_data_end])
                else:
                    data_chunks.append(b"\0" * (block_data_end - block_data_start))
            data = b"".join(data_chunks)
            assert len(data) == end - start
            data_chunks.clear()
            start = end

    @check_types
    @with_exclusive_transaction
    def write_file(self, file_id: int, data: bytes, start: int=0):
        writer = self.write_file_iter(file_id, start)
        next(writer)
        writer.send(data)
        try:
            writer.send(None)
        except StopIteration:
            pass
        else:
            raise RuntimeError("Writer did not finish")

    @check_types
    @with_db_connected
    def read_file(self, file_id: int, amount: int, start: int=0):
        reader = self.read_file_iter(file_id, start)
        next(reader)
        return reader.send(amount)

    @check_types
    @with_exclusive_transaction
    def truncate_file(self, file_id: int, end: int=0):
        current_blocks = self.file_blocks(file_id)
        metadata = self.file_metadata(file_id)

        needed_blocks = ceildiv(end, DATA_BLOCK_SIZE)
        if needed_blocks < current_blocks:
            current_blocks, dealloc_blocks = current_blocks[:needed_blocks], current_blocks[needed_blocks:]
            self.deallocate_blocks(dealloc_blocks)
        elif needed_blocks > current_blocks:
            current_blocks.extend(self.allocate_blocks(needed_blocks - len(current_blocks)))

        self.set_file_blocks(file_id, current_blocks)
        metadata["size"] = end
        self.set_file_metadata(file_id, metadata)

    # user friendly methods

    def open(self, fname, create=False, exclusive=False):
        if isinstance(fname, str):
            fname = fname.encode()
        if isinstance(fname, bytes):
            if create:
                fname = self.create_file(fname, exclusive=exclusive)
            else:
                fname = self.file_id_from_name(fname)
        return File(self, fname)


class File:
    def __init__(self, fs, file_id, binary=False):
        self.fs = fs
        self.file_id = file_id
        self.binary = binary

    def read(self, amount, start=0):
        data = self.fs.read_file(file_id=self.file_id, amount=amount, start=start)
        if self.binary:
            return data
        return data.decode()

    def write(self, data, start=0):
        if not self.binary:
            data = data.encode()
        self.fs.write_file(file_id=self.file_id, data=data, start=start)

    def truncate(self, length=0):
        self.fs.truncate_file(file_id=self.file_id, end=length)

    def delete(self):
        self.fs.delete_file(file_id=self.file_id)
        self.file_id = None

    def add_name(self, fname):
        if isinstance(fname, str):
            fname = fname.encode()
        self.fs.add_file_name(file_id=self.file_id, name=fname)

    def remove_name(self, fname):
        if isinstance(fname, str):
            fname = fname.encode()
        self.fs.remove_file_name(file_id=self.file_id, name=fname)
