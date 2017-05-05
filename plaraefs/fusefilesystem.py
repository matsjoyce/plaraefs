import fuse
import pathlib
import getpass
import logging
import stat
import os
import bcrypt
import hashlib
import time

from .blocklevelfilesystem import BlockLevelFilesystem
from .filelevelfilesystem import FileLevelFilesystem, KeyAlreadyExists, KeyDoesNotExist
from .pathlevelfilesystem import PathLevelFilesystem, FileType, DirectoryEntry
from .accesscontroller import AccessController

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

XATTR_CREATE = 1
XATTR_REPLACE = 2


logger = logging.getLogger(__name__)


class FUSEFilesystem(fuse.Operations):
    def __init__(self, fname, accesscontroller: AccessController):
        self.fname = pathlib.Path(fname)
        self.salt = None
        self.password = getpass.getpass().encode()
        self.key = None
        self.accesscontroller = accesscontroller

    def __call__(self, op, *args):
        logger.debug(f"-> {op} {repr(args)}")
        try:
            t = time.time()
            ret = getattr(self, op)(*args)
            disp = repr(ret)
            if len(disp) > 100:
                disp = disp[:100] + "..."
            logger.debug(f"<- {op} {disp} in {time.time() - t} seconds")
            return ret
        except PermissionError as e:
            val = e.args[0] if e.args else fuse.EACCES
            logger.warning(f"<- {op} [Permission denied with {fuse.FuseOSError(val)}]")
            raise fuse.FuseOSError(val) from e
        except OSError as e:
            logger.debug(f"<- {op} {str(e)}")
            raise
        except Exception as e:
            logger.error(f"<- {op} [Unhandled exception]", exc_info=True)
            raise fuse.FuseOSError(fuse.EACCES) from e

    def access_violation(self, allowed):
        if not allowed:
            raise PermissionError()

    def lookup(self, path=None, fh=None, parent=None):
        if fh:
            return fh
        path = pathlib.PurePosixPath(path)
        if not path.name:
            return self.pathfs.ROOT_FILE_ID
        if not parent:
            parent = self.lookup(path.parent)
        data, _ = self.pathfs.search_directory(parent, path.name.encode())
        if data:
            if not self.accesscontroller.dir_lookup(dir=parent, name=path.name, file=data.file_id):
                if self.accesscontroller.dir_list(dir=parent):
                    raise PermissionError(fuse.ENOENT)
                else:
                    raise PermissionError(fuse.EACCES)
            return data.file_id
        elif self.accesscontroller.dir_list(dir=parent):
            raise fuse.FuseOSError(fuse.ENOENT)
        else:
            raise fuse.FuseOSError(fuse.EACCES)

    def access(self, path, amode):
        file_id = self.lookup(path)
        _, header = self.filefs.get_file_header(file_id, 0)
        if header.file_type == FileType.file.value:
            if amode & os.R_OK:
                self.access_violation(self.accesscontroller.file_read(file=file_id))
            if amode & os.W_OK:
                self.access_violation(self.accesscontroller.file_write(file=file_id))
            if amode & os.X_OK:
                raise fuse.FuseOSError(fuse.EACCES)
        elif header.file_type == FileType.dir.value:
            if amode & os.R_OK:
                self.access_violation(self.accesscontroller.dir_list(dir=file_id))
            if amode & os.W_OK:
                self.access_violation(self.accesscontroller.dir_add_file(dir=file_id, name=None))
                self.access_violation(self.accesscontroller.dir_remove_file(dir=file_id, name=None, file=None))
            if amode & os.X_OK:
                self.access_violation(self.accesscontroller.dir_lookup(dir=file_id, name=None, file=None))

    def chmod(self, path, mode):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def chown(self, path, uid, gid):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def create(self, path, mode):
        path = pathlib.PurePosixPath(path)
        parent = self.lookup(path.parent)
        self.access_violation(self.accesscontroller.dir_add_file(dir=parent, name=path.name))
        if self.pathfs.search_directory(parent, path.name.encode())[0] is not None:
            raise fuse.FuseOSError(fuse.EEXIST)
        file_id = self.filefs.create_new_file(FileType.file.value)
        self.pathfs.add_directory_entry(parent, DirectoryEntry(path.name.encode(), file_id))
        return file_id

    def destroy(self, path):
        self.blockfs.close()

    def flush(self, path, fh):
        pass

    def fsync(self, path, datasync, fh):
        pass

    def fsyncdir(self, path, datasync, fh):
        pass

    def getattr(self, path, fh=None):
        fh = self.lookup(path, fh)
        self.access_violation(self.accesscontroller.file_read(file=fh))
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

    def getxattr(self, path, name):
        file_id = self.lookup(path)
        self.access_violation(self.accesscontroller.xattr_get(file=file_id, name=name))
        try:
            return self.filefs.lookup_xattr(file_id, name.encode())
        except KeyError:
            raise fuse.FuseOSError(fuse.ENODATA)

    def init(self, path):
        initialise = not self.fname.exists()
        if initialise:
            self.salt = bcrypt.gensalt(15)
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

    def link(self, target, source):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def listxattr(self, path):
        file_id = self.lookup(path)
        self.access_violation(self.accesscontroller.xattr_list(file=file_id))
        return [i.decode() for i in self.filefs.read_xattrs(file_id)
                if self.accesscontroller.xattr_lookup(file=file_id, name=i.decode())]

    def mkdir(self, path, mode):
        path = pathlib.PurePosixPath(path)
        parent = self.lookup(path.parent)
        self.access_violation(self.accesscontroller.dir_add_file(dir=parent))
        file_id = self.filefs.create_new_file(FileType.dir.value)
        self.pathfs.add_directory_entry(parent, DirectoryEntry(path.name.encode(), file_id))

    def mknod(self, path, mode, dev):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def open(self, path, flags):
        file_id = self.lookup(path)
        if flags & 3 == os.O_RDONLY:
            self.access_violation(self.accesscontroller.file_read(file=file_id))
        elif flags & 3 == os.O_WRONLY:
            self.access_violation(self.accesscontroller.file_write(file=file_id))
        elif flags & 3 == os.O_RDWR:
            self.access_violation(self.accesscontroller.file_read(file=file_id))
            self.access_violation(self.accesscontroller.file_write(file=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.file.value:
            raise fuse.FuseOSError(fuse.EISDIR)
        return file_id

    def opendir(self, path):
        file_id = self.lookup(path)
        self.access_violation(self.accesscontroller.dir_list(dir=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.dir.value:
            raise fuse.FuseOSError(fuse.ENOTDIR)
        return file_id

    def read(self, path, size, offset, fh):
        file_id = self.lookup(path, fh)
        self.access_violation(self.accesscontroller.file_read(file=file_id))
        return self.filefs.reader(file_id, offset).read(size)

    def readdir(self, path, fh):
        file_id = self.lookup(path, fh)
        self.access_violation(self.accesscontroller.dir_list(dir=file_id))
        return [".", ".."] + [entry.name.decode() for entry in self.pathfs.directory_entries(file_id)
                              if self.accesscontroller.dir_lookup(dir=file_id,
                                                                  name=entry.name.decode(),
                                                                  file=entry.file_id)]

    def readlink(self, path):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def release(self, path, fh):
        pass

    def releasedir(self, path, fh):
        pass

    def removexattr(self, path, name):
        file_id = self.lookup(path)
        self.access_violation(self.accesscontroller.xattr_remove(file=file_id))
        try:
            self.filefs.delete_xattr(file_id, name.encode())
        except KeyError:
            raise fuse.FuseOSError(fuse.ENODATA)

    def rename(self, old, new):
        old = pathlib.PurePosixPath(old)
        new = pathlib.PurePosixPath(new)
        old_parent = self.lookup(old.parent)
        new_parent = self.lookup(new.parent)
        file_id = self.lookup(old)
        try:
            self.lookup(new)
        except OSError:
            pass
        else:
            raise fuse.FuseOSError(fuse.EEXIST)
        self.access_violation(self.accesscontroller.dir_remove_file(dir=old_parent, name=old.name, file=file_id))
        self.access_violation(self.accesscontroller.dir_add_file(dir=new_parent, name=new.name))
        self.pathfs.add_directory_entry(new_parent, DirectoryEntry(new.name.encode(), file_id))
        self.pathfs.remove_directory_entry(old_parent, old.name.encode())

    def rmdir(self, path):
        path = pathlib.PurePosixPath(path)
        parent = self.lookup(path.parent)
        self.access_violation(self.accesscontroller.dir_remove_file(dir=parent))
        file_id = self.lookup(path, parent=parent)
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.dir.value:
            raise fuse.FuseOSError(fuse.ENOTDIR)
        self.access_violation(self.accesscontroller.dir_delete(file=file_id))
        if self.pathfs.directory_entries(file_id):
            raise fuse.FuseOSError(fuse.ENOTEMPTY)
        self.pathfs.remove_directory_entry(parent, path.name.encode())
        self.filefs.delete_file(file_id)

    def setxattr(self, path, name, value, options):
        file_id = self.lookup(path)
        self.access_violation(self.accesscontroller.xattr_set(file=file_id))
        try:
            self.filefs.set_xattr(file_id, name.encode(), value,
                                  replace_only=bool(options & XATTR_REPLACE),
                                  create_only=bool(options & XATTR_CREATE))
        except KeyAlreadyExists:
            raise fuse.FuseOSError(fuse.EEXIST)
        except KeyDoesNotExist:
            raise fuse.FuseOSError(fuse.ENODATA)

    def statfs(self, path):
        basefs_stat = os.statvfs(str(self.fname))
        return {"f_bavail": basefs_stat.f_bavail * basefs_stat.f_bsize // self.blockfs.PHYSICAL_BLOCK_SIZE,
                "f_bfree": basefs_stat.f_bavail * basefs_stat.f_bsize // self.blockfs.PHYSICAL_BLOCK_SIZE,
                "f_blocks": self.blockfs.total_blocks() + 10,
                "f_bsize": self.blockfs.LOGICAL_BLOCK_SIZE,
                # "f_favail": 1,
                # "f_ffree": 1,
                # "f_files": 1,
                "f_flag": ST_NOATIME | ST_NODEV | ST_NODIRATIME | ST_NOEXEC | ST_NOSUID | ST_SYNCHRONOUS,
                "f_frsize": self.blockfs.LOGICAL_BLOCK_SIZE,
                "f_namemax": self.pathfs.FILENAME_SIZE}

    def symlink(self, target, source):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def truncate(self, path, length, fh=None):
        file_id = self.lookup(path, fh)
        self.access_violation(self.accesscontroller.file_write(file=file_id))
        self.filefs.truncate_file_size(file_id, length)

    def unlink(self, path):
        path = pathlib.PurePosixPath(path)
        parent = self.lookup(path.parent)
        file_id = self.lookup(path, parent=parent)
        self.access_violation(self.accesscontroller.dir_remove_file(dir=parent, name=path.name, file=file_id))
        self.access_violation(self.accesscontroller.file_delete(file=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.file.value:
            raise fuse.FuseOSError(fuse.EISDIR)
        self.pathfs.remove_directory_entry(parent, path.name.encode())
        self.filefs.delete_file(file_id)

    def utimens(self, path, times=None):  # XXX: UNSUPPORTED
        raise fuse.FuseOSError(fuse.ENOSYS)

    def write(self, path, data, offset, fh):
        file_id = self.lookup(path, fh)
        self.access_violation(self.accesscontroller.file_write(file=file_id))
        self.filefs.writer(file_id, offset).write(data, flush=True)
        return len(data)
