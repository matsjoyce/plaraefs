import contextlib
import os
import pathlib
import threading

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from . import locking
from .utils import check_types

BITMAP_LENGTH = 8
MAX_NUMBER_OF_BLOCKS = 2 ** BITMAP_LENGTH


class BlockFileSystem:
    KEY_SIZE = 32  # 256 bit keys
    IV_SIZE = 16  # 128 bit IV
    TAG_SIZE = 16  # 128 bit AEAD tag
    UNINITALISED_IV = b"\0" * IV_SIZE

    PHYSICAL_BLOCK_SIZE = 4 * 2 ** 10
    LOGICAL_BLOCK_SIZE = PHYSICAL_BLOCK_SIZE - IV_SIZE - TAG_SIZE

    FS_EXT = ".plaraefs"
    BLOCK_ID_SIZE = 8

    def __init__(self, fname, key: bytes):
        # TODO: add block caching
        self.lock = threading.RLock()
        self.key = key
        assert len(self.key) == self.KEY_SIZE

        self.fname = pathlib.Path(fname)
        assert self.fname.suffix == self.FS_EXT
        assert self.fname.stat().st_size % self.PHYSICAL_BLOCK_SIZE == 0
        self._file = open(self.fname, "r+b")

        self.backend = default_backend()
        self.block_reads = self.block_writes = 0
        self.file_locked = False
        self.file_locked_write = False

    @classmethod
    def initialise(cls, fname):
        fname = pathlib.Path(fname)
        assert fname.suffix == cls.FS_EXT
        with open(fname, "x"):
            pass

    @contextlib.contextmanager
    def lock_file(self, write):
        with self.lock:
            if self.file_locked:
                if write == self.file_locked_write or self.file_locked_write:
                    yield
                    return
                raise RuntimeError("File locked in wrong mode")
            try:
                locking.lock_file(self._file, write)
                self.file_locked = True
                self.file_locked_write = write
                yield
            finally:
                locking.unlock_file(self._file)
                self.file_locked = False

    @contextlib.contextmanager
    def file(self, write):
        with self.lock_file(write):
            if write:
                self.block_writes += 1
            else:
                self.block_reads += 1
            try:
                yield self._file
            finally:
                self._file.flush()

    @check_types
    def encrypt_block(self, plaintext: bytes):
        assert len(plaintext) == self.LOGICAL_BLOCK_SIZE

        iv = self.UNINITALISED_IV
        while iv == self.UNINITALISED_IV:
            iv = os.urandom(self.IV_SIZE)

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
        return block_id * self.PHYSICAL_BLOCK_SIZE

    @check_types
    def block_end(self, block_id: int):
        return (block_id + 1) * self.PHYSICAL_BLOCK_SIZE

    def total_blocks(self):
        size = self.fname.stat().st_size
        assert size % self.PHYSICAL_BLOCK_SIZE == 0
        return size // self.PHYSICAL_BLOCK_SIZE

    @check_types
    def new_blocks(self, number):
        total_blocks = self.total_blocks()
        new_block_ids = list(range(total_blocks, total_blocks + number))
        with self.file(True) as f:
            f.seek(self.block_start(total_blocks))
            written = f.write(b"\0" * (self.PHYSICAL_BLOCK_SIZE * number))

        assert written == self.PHYSICAL_BLOCK_SIZE * number
        return new_block_ids

    @check_types
    def remove_blocks(self, number):
        total_blocks = self.total_blocks()
        assert number <= total_blocks

        new_total_blocks = total_blocks - number
        with self.file(True) as f:
            f.truncate(new_total_blocks * self.PHYSICAL_BLOCK_SIZE)

    @check_types
    def read_block(self, block_id: int, with_token: bool=False):
        # return None if the block is not initialised
        assert block_id < self.total_blocks()

        with self.file(False) as f:
            f.seek(self.block_start(block_id))
            cipher_data = f.read(self.PHYSICAL_BLOCK_SIZE)

        if cipher_data.startswith(self.UNINITALISED_IV):
            return None

        plain_data = self.decrypt_block(cipher_data)
        if with_token:
            return plain_data, cipher_data[:self.IV_SIZE]
        return plain_data

    @check_types
    def write_block(self, block_id: int, data: bytes, with_token: bool=False):
        assert block_id < self.total_blocks()

        cipher_data = self.encrypt_block(data)

        with self.file(True) as f:
            f.seek(self.block_start(block_id))
            f.write(cipher_data)

        if with_token:
            return cipher_data[:self.IV_SIZE]

    @check_types
    def swap_blocks(self, block_id1: int, block_id2: int):
        assert block_id1 < self.total_blocks()
        assert block_id2 < self.total_blocks()

        if block_id1 == block_id2:
            return

        with self.file(True) as f:
            f.seek(self.block_start(block_id1))
            block_1_data = f.read(self.PHYSICAL_BLOCK_SIZE)
            f.seek(self.block_start(block_id2))
            block_2_data = f.read(self.PHYSICAL_BLOCK_SIZE)

            f.seek(self.block_start(block_id1))
            f.write(block_2_data)
            f.seek(self.block_start(block_id2))
            f.write(block_1_data)

    @check_types
    def wipe_block(self, block_id: int):
        assert block_id < self.total_blocks()
        with self.file(True) as f:
            f.seek(self.block_start(block_id))
            f.write(b"\0" * self.PHYSICAL_BLOCK_SIZE)

    @check_types
    def block_version(self, block_id: int, old_version: bytes=b""):
        assert block_id < self.total_blocks()

        with self.file(False) as f:
            f.seek(self.block_start(block_id))
            iv = f.read(self.IV_SIZE)

        return old_version != iv, iv

    def close(self):
        self._file.flush()
        self._file.close()
