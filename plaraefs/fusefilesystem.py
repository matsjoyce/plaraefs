import fuse
import pathlib
import getpass
import logging
import stat
import os
import bcrypt
import hashlib

from .blocklevelfilesystem import BlockLevelFilesystem
from .filelevelfilesystem import FileLevelFilesystem
from .pathlevelfilesystem import PathLevelFilesystem, FileType, DirectoryEntry

ST_RDONLY = 1
ST_NOSUID = 2
ST_NODEV = 4
ST_NOEXEC = 8
ST_SYNCHRONOUS = 16
ST_MANDLOCK = 64
ST_WRITE = 128
ST_APPEND = 256
ST_NOATIME = 1024
ST_NODIRATIME = 2048
ST_RELATIME = 4096


logger = logging.getLogger(__name__)


class FUSEFilesystem(fuse.LoggingMixIn, fuse.Operations):
    def __init__(self, fname):
        self.fname = pathlib.Path(fname)
        self.salt = None
        self.password = getpass.getpass().encode()
        self.key = None

    def allow(self, fh, pid, write):
        _, header = self.filefs.get_file_header(fh, 0)
        logger.debug("Access permission for {}, process {}, write {}, tag {}", fh, os.readlink("/proc/{}/exe".format(pid)), write, header.group_tag)

    def lookup_and_check(self, path=None, fh=None, write=False):
        gid, uid, pid = fuse.fuse_get_context()
        allow = lambda f: self.allow(f, pid, write)
        if fh:
            allow(fh)
            return fh
        fh = self.pathfs.lookup(tuple(i.encode() for i in pathlib.PurePosixPath(path).parts)[1:],
                                checker=allow)
        if fh is None:
            raise fuse.FuseOSError(fuse.ENOENT)
        return fh

    def access(self, path, amode):
        self.lookup_and_check(path, write=amode & os.W_OK)
        return 0

    def chmod(self, path, mode):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def chown(self, path, uid, gid):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def create(self, path, mode):
        path = pathlib.PurePosixPath(path)
        parent = self.lookup_and_check(path.parent, write=True)
        file_id = self.filefs.create_new_file(FileType.file.value)
        self.pathfs.add_directory_entry(parent, DirectoryEntry(path.name.encode(), file_id))
        return file_id

    def destroy(self, path):
        self.blockfs.close()

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def fsyncdir(self, path, datasync, fh):
        return 0

    def getattr(self, path, fh=None):
        fh = self.lookup_and_check(path, fh)
        _, header = self.filefs.get_file_header(fh, 0)
        if header.file_type == FileType.file.value:
            mode = stat.S_IFREG
        elif header.file_type == FileType.dir.value:
            mode = stat.S_IFDIR

        return {"st_atime": 0,
                "st_ctime": 0,
                "st_gid": 0,
                "st_mode": mode | stat.S_IRUSR | stat.S_IWUSR,
                "st_mtime": 0,
                "st_nlink": 1,
                "st_size": header.size,
                "st_uid": 0}

    def getxattr(self, path, name, position=0):
        raise fuse.FuseOSError(fuse.ENOTSUP)

    def init(self, path):
        initialise = not self.fname.exists()
        if initialise:
            self.salt = bcrypt.gensalt(16)
        elif self.salt is None:
            with self.fname.open("rb") as f:
                self.salt = f.read(32).rstrip(b"\0")
        if self.key is None:
            prehash = hashlib.sha256(self.password).digest()
            hash = bcrypt.hashpw(prehash, self.salt)
            self.key = hashlib.sha256(hash).digest()[:BlockLevelFilesystem.KEY_SIZE]

        if initialise:
            BlockLevelFilesystem.initialise(self.fname, self.key, offset=32)
            with self.fname.open("r+b") as f:
                f.write(self.salt)

        self.blockfs = BlockLevelFilesystem(self.fname, self.key, offset=32)
        if initialise:
            FileLevelFilesystem.initialise(self.blockfs)
        self.filefs = FileLevelFilesystem(self.blockfs)
        if initialise:
            PathLevelFilesystem.initialise(self.filefs)
        self.pathfs = PathLevelFilesystem(self.filefs)

    def link(self, target, source):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def listxattr(self, path):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def mkdir(self, path, mode):
        path = pathlib.PurePosixPath(path)
        parent = self.lookup_and_check(path.parent, write=True)
        file_id = self.filefs.create_new_file(FileType.dir.value)
        self.pathfs.add_directory_entry(parent, DirectoryEntry(path.name.encode(), file_id))
        return 0

    def mknod(self, path, mode, dev):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def open(self, path, flags):
        file_id = self.lookup_and_check(path)  # FIXME write=...
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.file.value:
            raise fuse.FuseOSError(fuse.EISDIR)
        return file_id

    def opendir(self, path):
        file_id = self.lookup_and_check(path)  # FIXME write=...
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.dir.value:
            raise fuse.FuseOSError(fuse.ENOTDIR)
        return file_id

    def read(self, path, size, offset, fh):
        fh = self.lookup_and_check(fh=fh)
        return self.filefs.reader(fh, offset).read(size)

    def readdir(self, path, fh):
        fh = self.lookup_and_check(fh=fh)
        return [".", ".."] + [entry.name.decode() for entry in self.pathfs.directory_entries(fh)]

    def readlink(self, path):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def release(self, path, fh):
        return 0

    def releasedir(self, path, fh):
        return 0

    def removexattr(self, path, name):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def rename(self, old, new):
        old = pathlib.PurePosixPath(old)
        new = pathlib.PurePosixPath(new)
        file_id = self.lookup_and_check(old)  # XXX do we need write permission?
        old_parent = self.lookup_and_check(old.parent, write=True)
        new_parent = self.lookup_and_check(new.parent, write=True)
        self.pathfs.add_directory_entry(new_parent, DirectoryEntry(new.name.encode(), file_id))
        self.pathfs.remove_directory_entry(old_parent, old.name.encode())
        return 0

    def rmdir(self, path):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def setxattr(self, path, name, value, options, position=0):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def statfs(self, path):
        return {#"f_bavail",
                #"f_bfree",
                "f_blocks": self.blockfs.total_blocks(),
                "f_bsize": self.blockfs.PHYSICAL_BLOCK_SIZE,
                #"f_favail",
                #"f_ffree",
                #"f_files",
                "f_flag": ST_NOATIME | ST_NODEV | ST_NODIRATIME | ST_NOEXEC | ST_NOSUID | ST_SYNCHRONOUS,
                "f_frsize": self.blockfs.PHYSICAL_BLOCK_SIZE,
                #"f_namemax": self.pathfs.FILENAME_SIZE
                }

    def symlink(self, target, source):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def truncate(self, path, length, fh=None):
        file_id = self.lookup_and_check(path, fh, write=True)
        self.filefs.truncate_file_size(file_id, length)

    def unlink(self, path):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def utimens(self, path, times=None):
        raise fuse.FuseOSError(fuse.ENOSYS)

    def write(self, path, data, offset, fh):
        fh = self.lookup_and_check(fh=fh, write=True)
        self.filefs.writer(fh, offset).write(data, flush=True)
        return len(data)
