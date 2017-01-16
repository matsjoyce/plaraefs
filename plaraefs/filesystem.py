import array
import collections
import contextlib
import functools
import inspect
import msgpack
import os
import pathlib
import sqlite3
import sys
import threading

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding as symmetric_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

FS_EXT = ".plarsfs"
FS_DB_EXT = FS_EXT + "db"

TOTAL_BLOCK_SIZE = 4 * 2 ** 10
KEY_SIZE = 32  # 256 bit keys
IV_SIZE = 16  # 128 bit IV
UNINITALISED_IV = b"\0" * IV_SIZE
DATA_BLOCK_SIZE = TOTAL_BLOCK_SIZE - IV_SIZE

FILE_ID_SIZE = 8
BLOCK_ID_SIZE = 8

SCHEMA = """
create table Files (
    id                  integer primary key,
    metadata            blob,
    blocks              blob
);

create table Filenames (
    name                blob primary key,
    id                  integer not null,
    foreign key(id) references Files(id)
);

create table Util (
    key                 blob primary key,
    value               blob
);
"""

backend = default_backend()


def ceildiv(a, b):
    # http://stackoverflow.com/a/17511341/3946766
    return -(-a // b)


def check_types(func):
    if __debug__:
        sig = inspect.signature(func)

        @functools.wraps(func)
        def ct_wrapper(*args, **kwargs):
            for arg, param in zip(args, sig.parameters.values()):
                if param.annotation is not inspect._empty and not isinstance(arg, param.annotation):
                    raise ValueError(f"Argument {param.name} must be of type {param.annotation}, not {type(arg)}")
            for name, arg in kwargs.items():
                param = sig.parameters[name]
                if param.annotation is not inspect._empty and not isinstance(arg, param.annotation):
                    raise ValueError(f"Argument {param.name} must be of type {param.annotation}, not {type(arg)}")
            return func(*args, **kwargs)
        return ct_wrapper
    return func


class FileSystem:
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
        self.is_in_exclusive_transaction = False

    @staticmethod
    def db_connect(fname):
        conn = sqlite3.connect(str(fname), check_same_thread=False, isolation_level=None)
        conn.execute("pragma foreign_keys = ON")
        return conn

    @staticmethod
    def generate_file_id():
        return int.from_bytes(os.urandom(FILE_ID_SIZE), "little", signed=True)

    @check_types
    def encrypt(self, plaintext: bytes, with_padding=True):
        iv = UNINITALISED_IV
        while iv == UNINITALISED_IV:
            iv = os.urandom(IV_SIZE)

        if with_padding:
            padder = symmetric_padding.PKCS7(algorithms.AES.block_size).padder()
            paddedtext = padder.update(plaintext) + padder.finalize()
        else:
            paddedtext = plaintext
            assert len(paddedtext) % IV_SIZE == 0

        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=backend)
        encryptor = cipher.encryptor()
        return iv + encryptor.update(paddedtext) + encryptor.finalize()

    @check_types
    def decrypt(self, cyphertext: bytes, with_padding=True):
        assert len(cyphertext) >= IV_SIZE
        iv, cyphertext = cyphertext[:IV_SIZE], cyphertext[IV_SIZE:]

        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=backend)
        decryptor = cipher.decryptor()
        paddedtext = decryptor.update(cyphertext) + decryptor.finalize()

        if with_padding:
            unpadder = symmetric_padding.PKCS7(algorithms.AES.block_size).unpadder()
            plaintext = unpadder.update(paddedtext) + unpadder.finalize()
        else:
            plaintext = paddedtext
        return plaintext

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
        conn.execute("insert into Util (key, value) values (?, ?)",
                     (b"filenames_version", (0).to_bytes(4, "little")))
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
        with self.db_connected():
            if self.conn.in_transaction:
                yield
                return

            self.conn.execute("begin exclusive transaction")
            self.is_in_exclusive_transaction = True
            try:
                yield
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

    # block level operations

    @check_types
    def block_start(self, block_id: int):
        return block_id * TOTAL_BLOCK_SIZE

    @check_types
    def block_end(self, block_id: int):
        return (block_id + 1) * TOTAL_BLOCK_SIZE

    @check_types
    @with_db_connected
    def file_blocks(self, file_id: int):
        cypher_raw_blocks, = self.conn.execute("select blocks from Files where id = ?", (file_id,)).fetchone()
        raw_blocks = self.decrypt(cypher_raw_blocks)
        return self.decode_block_ids(raw_blocks)

    @check_types
    @with_db_connected
    def set_file_blocks(self, file_id: int, blocks):
        raw_blocks = self.encode_block_ids(blocks)
        cypher_raw_blocks = self.encrypt(raw_blocks)
        self.conn.execute("update Files set blocks = ? where id = ?", (cypher_raw_blocks, file_id))

    @with_db_connected
    def free_blocks(self):
        cypher_raw_blocks, = self.conn.execute("select value from Util where key = ?", (b"free_blocks",)).fetchone()
        if cypher_raw_blocks == b"":
            return self.make_block_id_container()
        raw_blocks = self.decrypt(cypher_raw_blocks)
        assert len(raw_blocks) % BLOCK_ID_SIZE == 0
        return self.decode_block_ids(raw_blocks)

    @with_db_connected
    def set_free_blocks(self, blocks):
        raw_blocks = self.encode_block_ids(blocks)
        cypher_raw_blocks = self.encrypt(raw_blocks)
        self.conn.execute("update Util set value = ? where key = ?", (cypher_raw_blocks, b"free_blocks"))

    def total_blocks(self):
        size = self.fname.stat().st_size
        assert size % TOTAL_BLOCK_SIZE == 0
        return size // TOTAL_BLOCK_SIZE

    @check_types
    def new_blocks(self, number):
        total_blocks = self.total_blocks()
        new_block_ids = list(range(total_blocks, total_blocks + number))
        with self.main_file() as f:
            f.seek(self.block_start(new_block_ids[-1]))
            f.write(b"\0" * TOTAL_BLOCK_SIZE)
        return new_block_ids

    def allocate_blocks(self, number):
        assert self.is_in_exclusive_transaction
        free_blocks = self.free_blocks()
        allocated_blocks, free_blocks = free_blocks[:number], free_blocks[number:]
        if len(allocated_blocks) != number:
            allocated_blocks.extend(self.new_blocks(number - len(allocated_blocks)))
        self.set_free_blocks(free_blocks)
        return allocated_blocks

    def deallocate_blocks(self, block_ids):
        assert self.is_in_exclusive_transaction
        free_blocks = self.free_blocks()
        for block_id in block_ids:
            free_blocks.append(block_id)
            self.wipe_block(block_id)
        self.set_free_blocks(free_blocks)

    @check_types
    def read_block(self, block_id: int):
        # return None if the block is not initialised
        with self.main_file() as f:
            f.seek(self.block_start(block_id))
            cypherdata = f.read(TOTAL_BLOCK_SIZE)

        assert len(cypherdata) == TOTAL_BLOCK_SIZE
        if cypherdata[:IV_SIZE] == UNINITALISED_IV:
            return None

        data = self.decrypt(cypherdata, with_padding=False)
        assert len(data) == DATA_BLOCK_SIZE
        return data

    @check_types
    @with_db_connected
    def write_block(self, block_id: int, data: bytes):
        assert len(data) == DATA_BLOCK_SIZE
        cypherdata = self.encrypt(data, with_padding=False)
        assert len(cypherdata) == TOTAL_BLOCK_SIZE

        with self.main_file() as f:
            f.seek(self.block_start(block_id))
            f.write(cypherdata)

    @check_types
    @with_db_connected
    def wipe_block(self, block_id: int):
        with self.main_file() as f:
            f.seek(self.block_start(block_id))
            f.write(b"\0" * TOTAL_BLOCK_SIZE)

    # file level operations

    @with_db_connected
    def reload_file_name_cache(self):
        current_file_name_version, = self.conn.execute("select value from Util where key = ?",
                                                       (b"filenames_version",)).fetchone()
        current_file_name_version = int.from_bytes(current_file_name_version, "little")
        if current_file_name_version != self.file_name_version:
            self.file_name_cache = {}
            self.reverse_file_name_cache = collections.defaultdict(set)
            for file_id, cyphername in self.conn.execute("select id, name from Filenames"):
                name = self.decrypt(cyphername)
                self.file_name_cache[name] = file_id
                self.reverse_file_name_cache[file_id].add(name)

    def increment_file_name_cache(self):
        assert self.is_in_exclusive_transaction
        current_file_name_version, = self.conn.execute("select value from Util where key = ?",
                                                       (b"filenames_version",)).fetchone()
        current_file_name_version = int.from_bytes(current_file_name_version, "little") + 1
        self.conn.execute("update Util set value = ? where key = ?",
                          (current_file_name_version.to_bytes(4, "little"), b"filenames_version"))

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
        cypher_names = self.conn.execute("select name from Filenames where id = ?", (file_id,))
        for cypher_name, in cypher_names:
            if self.decrypt(cypher_name) == name:
                self.conn.execute("delete from Filenames where id = ? and name = ?",
                                  (file_id, cypher_name))
                self.increment_file_name_cache()
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

        cypher_name = self.encrypt(name)

        self.conn.execute("insert into Filenames (name, id) values (?, ?)", (cypher_name, file_id))
        self.increment_file_name_cache()

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
        cypher_name = self.encrypt(name)
        cypher_blocks = self.encrypt(b"")
        metadata = {"size": 0
                    }
        serialised_metadata = msgpack.dumps(metadata)
        cypher_metadata = self.encrypt(serialised_metadata)

        self.conn.execute("insert into Files (id, metadata, blocks) values (?, ?, ?)",
                          (file_id, cypher_metadata, cypher_blocks))

        self.conn.execute("insert into Filenames (name, id) values (?, ?)", (cypher_name, file_id))
        self.increment_file_name_cache()

        return file_id

    @check_types
    @with_exclusive_transaction
    def delete_file(self, file_id: int):
        file_blocks = self.file_blocks(file_id)
        free_blocks = self.free_blocks()
        free_blocks.extend(file_blocks)
        self.set_free_blocks(free_blocks)

        self.conn.execute("delete from Filenames where id = ?", (file_id,))
        self.increment_file_name_cache()
        self.conn.execute("delete from Files where id = ?", (file_id,))

        for block_id in file_blocks:
            self.wipe_block(block_id)

    @check_types
    @with_db_connected
    def file_metadata(self, file_id: int):
        cypher_metadata, = self.conn.execute("select metadata from Files where id = ?", (file_id,)).fetchone()
        serialised_metadata = self.decrypt(cypher_metadata)
        return msgpack.loads(serialised_metadata, use_list=False, encoding="utf-8")

    @check_types
    @with_exclusive_transaction
    def set_file_metadata(self, file_id: int, metadata):
        serialised_metadata = msgpack.dumps(metadata, use_bin_type=True)
        cypher_metadata = self.encrypt(serialised_metadata)
        self.conn.execute("update Files set metadata = ? where id = ?", (cypher_metadata, file_id))

    @check_types
    @with_exclusive_transaction
    def write_file(self, file_id: int, data: bytes, start: int=0):
        current_blocks = self.file_blocks(file_id)
        metadata = self.file_metadata(file_id)

        end = start + len(data)
        print(f"Write from {start} to {end}", ceildiv(end, DATA_BLOCK_SIZE))
        current_blocks.extend(self.allocate_blocks(ceildiv(end, DATA_BLOCK_SIZE) - len(current_blocks)))
        print("have", len(current_blocks), "blocks")

        first_block, first_block_start = divmod(start, DATA_BLOCK_SIZE)
        last_block, last_block_end = divmod(end, DATA_BLOCK_SIZE)
        data_current_index = 0

        for block_index in range(first_block, last_block + 1):
            block_data_start = 0
            block_data_end = DATA_BLOCK_SIZE
            if block_index == first_block:
                block_data_start = first_block_start
            if block_index == last_block:
                block_data_end = last_block_end

            needed_data = block_data_end - block_data_start
            block_data = data[data_current_index:data_current_index + needed_data]
            data_current_index += needed_data
            if needed_data != DATA_BLOCK_SIZE:
                old_block_data = self.read_block(current_blocks[block_index])
                if old_block_data is not None:
                    block_data = b"".join([old_block_data[:block_data_start], block_data, old_block_data[block_data_end:]])
                else:
                    block_data = b"".join([b"\0" * block_data_start, block_data, b"\0" * (DATA_BLOCK_SIZE - block_data_end)])
            assert len(block_data) == DATA_BLOCK_SIZE

            self.write_block(current_blocks[block_index], block_data)

        self.set_file_blocks(file_id, current_blocks)
        metadata["size"] = max(metadata["size"], end)
        self.set_file_metadata(file_id, metadata)

    @check_types
    @with_db_connected
    def read_file(self, file_id: int, amount: int, start: int=0):
        current_blocks = self.file_blocks(file_id)
        if len(current_blocks) == 0:
            return b""

        metadata = self.file_metadata(file_id)

        end = min(start + amount, metadata["size"])

        first_block, first_block_start = divmod(start, DATA_BLOCK_SIZE)
        last_block, last_block_end = divmod(end, DATA_BLOCK_SIZE)

        data = []

        for block_index in range(first_block, last_block + 1):
            block_data_start = 0
            block_data_end = DATA_BLOCK_SIZE
            if block_index == first_block:
                block_data_start = first_block_start
            if block_index == last_block:
                block_data_end = last_block_end

            block_data = self.read_block(current_blocks[block_index])
            if block_data is not None:
                data.append(block_data[block_data_start:block_data_end])
            else:
                data.append(b"\0" * (block_data_end - block_data_start))

        return b"".join(data)

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
