import pytest
import pathlib
import os

from plaraefs.blocklevelfilesystem import BlockLevelFilesystem
from plaraefs.filelevelfilesystem import FileLevelFilesystem
from plaraefs.pathlevelfilesystem import PathLevelFilesystem, DirectoryEntry
from plaraefs.read_iterator import ReadIterator


@pytest.fixture()
def fs():
    key = os.urandom(32)
    location = pathlib.Path("test_bfs.plaraefs")
    if location.exists():
        location.unlink()
    BlockLevelFilesystem.initialise(location, key)
    bfs = BlockLevelFilesystem(location, key)
    FileLevelFilesystem.initialise(bfs)
    ffs = FileLevelFilesystem(bfs)
    PathLevelFilesystem.initialise(ffs)
    yield PathLevelFilesystem(ffs)
    bfs.close()
    location.unlink()


def test_directory_lookup_simple(fs: PathLevelFilesystem):
    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0] is None
    assert fs.search_directory(fs.ROOT_FILE_ID, b"b")[0] is None
    assert fs.search_directory(fs.ROOT_FILE_ID, b"c")[0] is None

    de = DirectoryEntry(b"a", fs.ROOT_FILE_ID + 1)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de)

    assert ReadIterator(fs.filefs, fs.ROOT_FILE_ID, 0).read() == fs.pack_directory_entry(de)

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0].file_id == fs.ROOT_FILE_ID + 1
    assert fs.search_directory(fs.ROOT_FILE_ID, b"b")[0] is None
    assert fs.search_directory(fs.ROOT_FILE_ID, b"c")[0] is None

    de2 = DirectoryEntry(b"b", fs.ROOT_FILE_ID + 2)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de2)

    assert (ReadIterator(fs.filefs, fs.ROOT_FILE_ID, 0).read()
            == fs.pack_directory_entry(de) + fs.pack_directory_entry(de2))

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0].file_id == fs.ROOT_FILE_ID + 1
    assert fs.search_directory(fs.ROOT_FILE_ID, b"b")[0].file_id == fs.ROOT_FILE_ID + 2
    assert fs.search_directory(fs.ROOT_FILE_ID, b"c")[0] is None


def test_directory_lookup_overwrite(fs: PathLevelFilesystem):
    de = DirectoryEntry(b"a", fs.ROOT_FILE_ID + 1)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de)

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0].file_id == fs.ROOT_FILE_ID + 1

    de.file_id = fs.ROOT_FILE_ID + 2

    with pytest.raises(FileExistsError):
        fs.add_directory_entry(fs.ROOT_FILE_ID, de)

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0].file_id == fs.ROOT_FILE_ID + 1

    fs.add_directory_entry(fs.ROOT_FILE_ID, de, overwrite=True)

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0].file_id == fs.ROOT_FILE_ID + 2


def test_directory_remove_simple(fs: PathLevelFilesystem):
    de = DirectoryEntry(b"a", fs.ROOT_FILE_ID + 1)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de)

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0].file_id == fs.ROOT_FILE_ID + 1

    fs.remove_directory_entry(fs.ROOT_FILE_ID, de.name)

    assert fs.search_directory(fs.ROOT_FILE_ID, b"a")[0] is None
    assert ReadIterator(fs.filefs, fs.ROOT_FILE_ID, 0).read() == b""


def test_directory_remove(fs: PathLevelFilesystem):
    de_a = DirectoryEntry(b"a", fs.ROOT_FILE_ID + 1)
    de_b = DirectoryEntry(b"b", fs.ROOT_FILE_ID + 2)
    de_c = DirectoryEntry(b"c", fs.ROOT_FILE_ID + 3)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de_a)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de_b)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de_c)

    fs.remove_directory_entry(fs.ROOT_FILE_ID, b"b")

    assert fs.search_directory(fs.ROOT_FILE_ID, b"b")[0] is None
    assert (ReadIterator(fs.filefs, fs.ROOT_FILE_ID, 0).read()
            == fs.pack_directory_entry(de_a) + fs.pack_directory_entry(de_c))

    with pytest.raises(FileNotFoundError):
        fs.remove_directory_entry(fs.ROOT_FILE_ID, b"b")

    fs.remove_directory_entry(fs.ROOT_FILE_ID, b"c")

    assert ReadIterator(fs.filefs, fs.ROOT_FILE_ID, 0).read() == fs.pack_directory_entry(de_a)


def test_list_directory(fs: PathLevelFilesystem):
    de_a = DirectoryEntry(b"a", fs.ROOT_FILE_ID + 1)
    de_b = DirectoryEntry(b"b", fs.ROOT_FILE_ID + 2)
    de_c = DirectoryEntry(b"c", fs.ROOT_FILE_ID + 3)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de_a)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de_b)
    fs.add_directory_entry(fs.ROOT_FILE_ID, de_c)

    assert list(fs.directory_entries(fs.ROOT_FILE_ID)) == [de_a, de_b, de_c]
