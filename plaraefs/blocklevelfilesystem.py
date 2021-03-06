import contextlib
import os
import pathlib
import threading

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from . import locking
from .utils import check_types, LRUDict


class BlockLevelFilesystem:
    KEY_SIZE = 32  # 256 bit keys
    IV_SIZE = 16  # 128 bit IV
    TAG_SIZE = 16  # 128 bit AEAD tag
    UNINITALISED_IV = b"\0" * IV_SIZE

    PHYSICAL_BLOCK_SIZE = 4 * 2 ** 10
    LOGICAL_BLOCK_SIZE = PHYSICAL_BLOCK_SIZE - IV_SIZE - TAG_SIZE

    FS_EXT = ".plaraefs"
    BLOCK_ID_SIZE = 8

    __slots__ = ["lock", "key", "offset", "fname", "_file", "backend", "block_reads",
                 "block_writes", "lock_file_locked", "lock_file_locked_write", "block_cache",
                 "unflushed_writes", "locked_tokens"]

    @check_types
    def __init__(self, fname, key: bytes, offset: int=0):
        self.lock = threading.RLock()
        self.key = key
        self.offset = offset
        assert len(self.key) == self.KEY_SIZE

        self.fname = pathlib.Path(fname)
        assert self.fname.suffix == self.FS_EXT
        assert (self.fname.stat().st_size - offset) % self.PHYSICAL_BLOCK_SIZE == 0
        self._file = open(str(self.fname), "r+b", 0)

        self.backend = default_backend()
        self.block_reads = self.block_writes = 0
        self.lock_file_locked = False
        self.lock_file_locked_write = False

        self.block_cache = LRUDict(2048)
        self.unflushed_writes = {}
        self.locked_tokens = set()

    @classmethod
    @check_types
    def initialise(cls, fname, key: bytes, offset: int=0):
        fname = pathlib.Path(fname)
        assert fname.suffix == cls.FS_EXT
        assert len(key) == cls.KEY_SIZE
        with open(str(fname), "xb") as f:
            f.write(b"\0" * offset)

    @contextlib.contextmanager
    def lock_file(self, write):
        with self.lock:
            if self.lock_file_locked_write or not write and self.lock_file_locked:
                yield self._file
                return
            if self.lock_file_locked:
                raise RuntimeError("File locked in wrong mode, locked for read and need lock for write")
            try:
                locking.lock_file(self._file, write)
                self.lock_file_locked = True
                self.lock_file_locked_write = write
                yield self._file
            finally:
                if write:
                    self.flush_writes()
                    self._file.flush()
                locking.unlock_file(self._file)
                self.lock_file_locked = False
                self.lock_file_locked_write = False
                self.locked_tokens.clear()

    def new_token(self):
        iv = self.UNINITALISED_IV
        while iv == self.UNINITALISED_IV:
            iv = os.urandom(self.IV_SIZE)
        return iv

    @check_types
    def encrypt_block(self, plaintext: bytes, iv: bytes=None):
        assert len(plaintext) == self.LOGICAL_BLOCK_SIZE

        if iv is None:
            iv = self.new_token()
        cipher = Cipher(algorithms.AES(self.key), modes.GCM(iv), backend=self.backend)
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        ciphertext = b"".join((iv, ciphertext, encryptor.tag))

        assert len(ciphertext) == self.PHYSICAL_BLOCK_SIZE
        return ciphertext

    @check_types
    def decrypt_block(self, ciphertext: bytes):
        assert len(ciphertext) == self.PHYSICAL_BLOCK_SIZE

        iv, ciphertext, tag = (ciphertext[:self.IV_SIZE],
                               ciphertext[self.IV_SIZE:-self.TAG_SIZE],
                               ciphertext[-self.TAG_SIZE:])

        cipher = Cipher(algorithms.AES(self.key), modes.GCM(iv, tag), backend=self.backend)
        decryptor = cipher.decryptor()
        plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        assert len(plaintext) == self.LOGICAL_BLOCK_SIZE
        return plaintext

    @check_types
    def block_start(self, block_id: int):
        return block_id * self.PHYSICAL_BLOCK_SIZE + self.offset

    def total_blocks(self):
        with self.lock_file(write=False):
            size = self.fname.stat().st_size - self.offset
        assert size % self.PHYSICAL_BLOCK_SIZE == 0
        return size // self.PHYSICAL_BLOCK_SIZE

    @check_types
    def new_blocks(self, number):
        if not number:
            return []
        total_blocks = self.total_blocks()
        new_block_ids = list(range(total_blocks, total_blocks + number))
        with self.lock_file(write=True) as f:
            f.seek(self.block_start(total_blocks))
            written = f.write(b"\0" * (self.PHYSICAL_BLOCK_SIZE * number))

        assert written == self.PHYSICAL_BLOCK_SIZE * number
        self.block_writes += number
        return new_block_ids

    @check_types
    def remove_blocks(self, number):
        total_blocks = self.total_blocks()
        assert number <= total_blocks

        new_total_blocks = total_blocks - number
        with self.lock_file(write=True) as f:
            for block_id in range(new_total_blocks, total_blocks):
                self.unflushed_writes.pop(block_id, None)
            f.truncate(self.block_start(new_total_blocks))

    @check_types
    def read_block(self, block_id: int, with_token: bool=False):
        # return None if the block is not initialised
        cache_data, cache_token = self.unflushed_writes.get(block_id, self.block_cache.get(block_id, (None, None)))
        if self.lock_file_locked and cache_token in self.locked_tokens:
            if cache_data is None:
                print("A")
            return (cache_data, cache_token) if with_token else cache_data

        assert block_id < self.total_blocks()
        with self.lock:
            with self.lock_file(write=False) as f:
                f.seek(self.block_start(block_id))
                token = f.read(self.IV_SIZE)
                if token == self.UNINITALISED_IV:
                    return None
                elif token == cache_token:
                    if cache_data is None:
                        print("B")
                    return (cache_data, cache_token) if with_token else cache_data
                cipher_data = token + f.read(self.PHYSICAL_BLOCK_SIZE - self.IV_SIZE)

            plain_data = self.decrypt_block(cipher_data)

            self.block_cache[block_id] = plain_data, token
            if self.lock_file_locked:
                self.locked_tokens.add(token)
        self.block_reads += 1
        if plain_data is None:
            print("A")
        return (plain_data, token) if with_token else plain_data

    def flush_writes(self, only=None):
        with self.lock_file(write=True) as f:
            rms = []
            for block_id, (data, token) in self.unflushed_writes.items():
                if only and block_id not in only:
                    continue
                cipher_data = self.encrypt_block(data, iv=token)
                f.seek(self.block_start(block_id))
                f.write(cipher_data)
                self.block_writes += 1
                rms.append(block_id)
            for r in rms:
                del self.unflushed_writes[r]

    @check_types
    def write_block(self, block_id: int, offset: int, data: bytes, with_token: bool=False):
        assert block_id < self.total_blocks()
        assert offset + len(data) <= self.LOGICAL_BLOCK_SIZE

        if len(data) != self.LOGICAL_BLOCK_SIZE:
            new_token = self.new_token()
            data_from_end = self.LOGICAL_BLOCK_SIZE - offset - len(data)
            with self.lock_file(write=True) as f:
                old_data = self.read_block(block_id)
                if old_data is None:
                    data_to_write = b"".join((b"\0" * offset, data, b"\0" * data_from_end))
                elif data_from_end:
                    data_to_write = b"".join((old_data[:offset], data, old_data[-data_from_end:]))
                else:
                    data_to_write = b"".join((old_data[:offset], data))
                self.unflushed_writes[block_id] = data_to_write, new_token
                self.locked_tokens.add(new_token)
                if with_token:
                    return new_token
                return

        cipher_data = self.encrypt_block(data)

        with self.lock:
            with self.lock_file(write=True) as f:
                f.seek(self.block_start(block_id))
                f.write(cipher_data)

            token = cipher_data[:self.IV_SIZE]
            self.block_cache[block_id] = data, token
            self.locked_tokens.add(token)

        self.block_writes += 1

        if with_token:
            return token

    @check_types
    def swap_blocks(self, block_id1: int, block_id2: int):
        assert block_id1 < self.total_blocks()
        assert block_id2 < self.total_blocks()

        if block_id1 == block_id2:
            return

        with self.lock:
            self.flush_writes([block_id1, block_id2])
            with self.lock_file(write=True) as f:
                f.seek(self.block_start(block_id1))
                block_1_data = f.read(self.PHYSICAL_BLOCK_SIZE)
                f.seek(self.block_start(block_id2))
                block_2_data = f.read(self.PHYSICAL_BLOCK_SIZE)

                f.seek(self.block_start(block_id1))
                f.write(block_2_data)
                f.seek(self.block_start(block_id2))
                f.write(block_1_data)

            (self.block_cache[block_id1],
             self.block_cache[block_id2]) = (self.block_cache[block_id2],
                                             self.block_cache[block_id1])
        self.block_writes += 2

    @check_types
    def wipe_block(self, block_id: int):
        assert block_id < self.total_blocks()
        with self.lock:
            self.unflushed_writes.pop(block_id, None)
            with self.lock_file(write=True) as f:
                f.seek(self.block_start(block_id))
                f.write(b"\0" * self.PHYSICAL_BLOCK_SIZE)

            self.block_cache[block_id] = None, self.UNINITALISED_IV
        self.block_writes += 1

    @check_types
    def block_version(self, block_id: int, old_version: bytes=b""):
        if self.lock_file_locked and old_version in self.locked_tokens:
            return False, old_version

        assert block_id < self.total_blocks()

        with self.lock:
            with self.lock_file(False) as f:
                f.seek(self.block_start(block_id))
                iv = f.read(self.IV_SIZE)

            if self.lock_file_locked:
                self.locked_tokens.add(iv)

            return old_version != iv, iv

    def close(self):
        self._file.flush()
        self._file.close()
