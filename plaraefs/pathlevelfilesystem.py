import enum
import attr
import struct

from .filelevelfilesystem import FileLevelFilesystem
from .utils import check_types


@attr.s(slots=True)
class DirectoryEntry:
    name = attr.ib()
    file_id = attr.ib()


class FileType(enum.Enum):
    file = 0
    dir = 1


class PathLevelFilesystem:
    ROOT_FILE_ID = 1
    FILENAME_SIZE = 256

    __slots__ = ["filefs", "DIRECTORY_ENTRY_SIZE", "directory_entry_struct"]

    def __init__(self, filefs: FileLevelFilesystem):
        self.filefs = filefs
        self.DIRECTORY_ENTRY_SIZE = self.FILENAME_SIZE + self.filefs.blockfs.BLOCK_ID_SIZE
        self.directory_entry_struct = struct.Struct(f"<{self.FILENAME_SIZE}sQ")

    @classmethod
    @check_types
    def initialise(cls, filefs: FileLevelFilesystem):
        filefs.create_new_file(FileType.dir.value)

    @check_types
    def unpack_directory_entry(self, data: bytes, offset: int=0):
        name, file_id = self.directory_entry_struct.unpack_from(data, offset)
        return DirectoryEntry(name.rstrip(b"\0"), file_id)

    @check_types
    def pack_directory_entry(self, entry: DirectoryEntry):
        return self.directory_entry_struct.pack(entry.name, entry.file_id)

    @check_types
    def search_directory(self, file_id: int, name: bytes):
        _, header = self.filefs.get_file_header(file_id, 0)
        assert header.file_type == FileType.dir.value
        assert header.size % self.DIRECTORY_ENTRY_SIZE == 0
        if not header.size:
            return None, 0
        start = 0
        end = header.size // self.DIRECTORY_ENTRY_SIZE
        # Binary search
        while True:
            middle = (start + end) // 2
            entry = self.unpack_directory_entry(self.filefs.read(file_id, self.DIRECTORY_ENTRY_SIZE, middle * self.DIRECTORY_ENTRY_SIZE))
            if entry.name == name:
                return entry, middle * self.DIRECTORY_ENTRY_SIZE
            elif entry.name < name:
                start = middle + 1
            else:
                end = middle
            if start == end:
                return None, start * self.DIRECTORY_ENTRY_SIZE

    @check_types
    def add_directory_entry(self, file_id: int, entry: DirectoryEntry, overwrite: bool=False):
        existing_entry, position = self.search_directory(file_id, entry.name)
        if existing_entry:
            if not overwrite:
                raise FileExistsError()
            self.filefs.write(file_id, self.pack_directory_entry(entry), position)
            return
        entries_after = self.filefs.read(file_id, -1, position)
        self.filefs.write(file_id, self.pack_directory_entry(entry) + entries_after, position)

    @check_types
    def remove_directory_entry(self, file_id: int, name: bytes):
        existing_entry, position = self.search_directory(file_id, name)
        if not existing_entry:
            raise FileNotFoundError()
        entries_after = self.filefs.read(file_id, -1, position + self.DIRECTORY_ENTRY_SIZE)
        self.filefs.truncate_file_size(file_id, position + len(entries_after))
        self.filefs.write(file_id, entries_after, position)

    @check_types
    def directory_entries(self, file_id: int):
        _, header = self.filefs.get_file_header(file_id, 0)
        assert header.file_type == FileType.dir.value
        assert header.size % self.DIRECTORY_ENTRY_SIZE == 0
        num_entries = header.size // self.DIRECTORY_ENTRY_SIZE
        data = self.filefs.read(file_id)
        for i in range(num_entries):
            yield self.unpack_directory_entry(data, i * self.DIRECTORY_ENTRY_SIZE)
