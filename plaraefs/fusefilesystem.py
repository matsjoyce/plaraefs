from cffi import FFI
from errno import *
from signal import signal, SIGINT, SIG_DFL
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


ENOTSUP = 95

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


ffi = FFI()
sys_types = (pathlib.Path(__file__).parent / "include/types.h").read_text()
fuse_cdef = (pathlib.Path(__file__).parent / "include/fuse.h").read_text()

ffi.cdef(sys_types + fuse_cdef)

libfuse = ffi.verify(
    "#include <fuse3/fuse.h>",
    libraries=["fuse3"],
    define_macros=[("FUSE_USE_VERSION", "30"),
                   ("_FILE_OFFSET_BITS", "64")])


class FUSEFilesystem:
    def __init__(self, fname, accesscontroller: AccessController, debug=False):
        self.fname = pathlib.Path(fname)
        self.salt = None
        self.password = getpass.getpass().encode()
        self.key = None
        self.accesscontroller = accesscontroller
        self.accesscontroller.fs = self
        self.debug = debug

    def mount(self, mount_point):
        self.mount_point = mount_point
        args = ["fuse", "-f", "-o", f"fsname=plaraefs", "-o", "allow_other", str(mount_point)]
        if self.debug:
            args.append("-d")
        argv = [ffi.new("char[]", arg.encode()) for arg in args]
        fuse_ops = ffi.new("struct fuse_operations*")

        methods = [x for x in self.__class__.__dict__ if not x.startswith("_")]
        self.keep_alive = []
        for method in methods:
            if hasattr(fuse_ops, method):
                cdef = ffi.typeof(getattr(fuse_ops, method)).cname

                def w(*args, _method=method):
                    return self(_method, *args)

                w.__name__ = method + "_wrapper"
                w.__qualname__ = method + "_wrapper"

                callback = ffi.callback(cdef, w)
                self.keep_alive.append(callback)
                setattr(fuse_ops, method, callback)

        try:
            old_handler = signal(SIGINT, SIG_DFL)
        except ValueError:
            old_handler = SIG_DFL

        err = libfuse.fuse_main_real(len(args), argv, fuse_ops,
                                     ffi.sizeof("struct fuse_operations"), ffi.NULL)

        try:
            signal(SIGINT, old_handler)
        except ValueError:
            pass

        if err:
            logger.critical(f"Mount failed with error [{os.strerror(err)}]")
            raise RuntimeError(err)

    def process_info(self):
        ctx = libfuse.fuse_get_context()
        return ctx.uid, ctx.gid, ctx.pid

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
            val = e.args[0] if e.args else EACCES
            logger.warning(f"<- {op} {repr(args)} Permission denied with [{os.strerror(val)}]")
            return -val
        except OSError as e:
            val = e.args[0] if e.args else EACCES
            logger.debug(f"<- {op} {repr(args)} [{os.strerror(val)}]")
            return -val
        except Exception as e:
            logger.error(f"<- {op} {repr(args)} [Unhandled exception]", exc_info=True)
            return -EACCES

    def init(self, info, config):
        initialise = not self.fname.exists()
        if initialise:
            password2 = getpass.getpass("Creating new filesystem, repeat password: ").encode()
            if self.password != password2:
                print("Passwords do not match!")
                raise RuntimeError()
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

        # config.nullpath_ok = True

        return ffi.NULL

    def access_violation(self, allowed):
        if not allowed:
            raise PermissionError()

    def lookup(self, path=None, info=None, parent=None):
        if info and info.fh:
            return info.fh
        parent_path, name = os.path.split(path)
        if not name:
            return self.pathfs.ROOT_FILE_ID
        if not parent:
            parent = self.lookup(parent_path)
        data, _ = self.pathfs.search_directory(parent, name)
        if data:
            if not self.accesscontroller.dir_lookup(dir=parent, name=name, file=data.file_id):
                if self.accesscontroller.dir_list(dir=parent):
                    raise PermissionError(ENOENT)
                else:
                    raise PermissionError(EACCES)
            return data.file_id
        elif self.accesscontroller.dir_list(dir=parent):
            raise OSError(ENOENT)
        else:
            raise OSError(EACCES)

    def access(self, path, amode):
        file_id = self.lookup(ffi.string(path))
        _, header = self.filefs.get_file_header(file_id, 0)
        if header.file_type == FileType.file.value:
            if amode & os.R_OK:
                self.access_violation(self.accesscontroller.file_read(file=file_id))
            if amode & os.W_OK:
                self.access_violation(self.accesscontroller.file_write(file=file_id))
            if amode & os.X_OK:
                raise OSError(EACCES)
        elif header.file_type == FileType.dir.value:
            if amode & os.R_OK:
                self.access_violation(self.accesscontroller.dir_list(dir=file_id))
            if amode & os.W_OK:
                self.access_violation(self.accesscontroller.dir_add_file(dir=file_id, name=None))
                self.access_violation(self.accesscontroller.dir_remove_file(dir=file_id, name=None, file=None))
            if amode & os.X_OK:
                self.access_violation(self.accesscontroller.dir_lookup(dir=file_id, name=None, file=None))
        return 0

    def chmod(self, path, mode, info):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def chown(self, path, uid, gid, info):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def create(self, path, mode, info):
        parent_path, name = os.path.split(ffi.string(path))
        parent = self.lookup(parent_path)
        self.access_violation(self.accesscontroller.dir_add_file(dir=parent, name=name))
        if self.pathfs.search_directory(parent, name)[0] is not None:
            raise OSError(EEXIST)
        file_id = self.filefs.create_new_file(FileType.file.value)
        self.pathfs.add_directory_entry(parent, DirectoryEntry(name, file_id))
        info.fh = file_id
        return 0

    def destroy(self, path):
        self.blockfs.close()

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def fsyncdir(self, path, datasync, fh):
        return 0

    def getattr(self, path, result, info):
        fh = self.lookup(ffi.string(path), info)
        self.access_violation(self.accesscontroller.file_read(file=fh))
        _, header = self.filefs.get_file_header(fh, 0)
        if header.file_type == FileType.file.value:
            mode = stat.S_IFREG
        elif header.file_type == FileType.dir.value:
            mode = stat.S_IFDIR

        result.st_atim.tv_sec = 0
        result.st_ctim.tv_sec = 0
        result.st_gid = 0
        result.st_mode = mode | stat.S_IRUSR | stat.S_IWUSR
        result.st_mtim.tv_sec = 0
        result.st_nlink = 1
        result.st_size = header.size
        result.st_uid = 0

        return 0

    def getxattr(self, path, name, buf, size):
        file_id = self.lookup(ffi.string(path))
        self.access_violation(self.accesscontroller.xattr_get(file=file_id, name=name))
        try:
            value = self.filefs.lookup_xattr(file_id, ffi.string(name))
        except KeyError:
            raise OSError(ENODATA)

        if size:
            if len(value) > size:
                return -ERANGE
            buf = ffi.buffer(buf, size)
            buf[:len(value)] = value

        return len(value)

    def link(self, target, source):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def listxattr(self, path, buf, size):
        file_id = self.lookup(ffi.string(path))
        self.access_violation(self.accesscontroller.xattr_list(file=file_id))
        value = b"\0".join(i for i in self.filefs.read_xattrs(file_id)
                           if self.accesscontroller.xattr_lookup(file=file_id, name=i))
        if value:
            value += b"\0"

        if size:
            if len(value) > size:
                return -ERANGE
            buf = ffi.buffer(buf, size)
            buf[:len(value)] = value

        return len(value)

    def mkdir(self, path, mode):
        parent_path, name = os.path.split(ffi.string(path))
        parent = self.lookup(parent_path)
        self.access_violation(self.accesscontroller.dir_add_file(dir=parent, name=name))
        file_id = self.filefs.create_new_file(FileType.dir.value)
        self.pathfs.add_directory_entry(parent, DirectoryEntry(name, file_id))
        return 0

    def mknod(self, path, mode, dev):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def open(self, path, info):
        file_id = self.lookup(ffi.string(path))
        if info.flags & 3 == os.O_RDONLY:
            self.access_violation(self.accesscontroller.file_read(file=file_id))
        elif info.flags & 3 == os.O_WRONLY:
            self.access_violation(self.accesscontroller.file_write(file=file_id))
        elif info.flags & 3 == os.O_RDWR:
            self.access_violation(self.accesscontroller.file_read(file=file_id))
            self.access_violation(self.accesscontroller.file_write(file=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.file.value:
            raise OSError(EISDIR)
        info.fh = file_id
        return 0

    def opendir(self, path, info):
        file_id = self.lookup(ffi.string(path))
        self.access_violation(self.accesscontroller.dir_list(dir=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.dir.value:
            raise OSError(ENOTDIR)
        info.fh = file_id
        return 0

    def read(self, path, buf, size, offset, info):
        file_id = self.lookup(ffi.string(path), info)
        self.access_violation(self.accesscontroller.file_read(file=file_id))
        data = self.filefs.reader(file_id, offset).read(size)
        buf = ffi.buffer(buf, size)
        buf[:len(data)] = data
        return len(data)

    def readdir(self, path, buf, filler, offset, info, flags):
        file_id = self.lookup(ffi.string(path), info)
        self.access_violation(self.accesscontroller.dir_list(dir=file_id))
        paths = [b".", b".."]
        paths.extend(entry.name for entry in self.pathfs.directory_entries(file_id)
                     if self.accesscontroller.dir_lookup(dir=file_id,
                                                         name=entry.name,
                                                         file=entry.file_id))
        for item in paths:
            if filler(buf, item, ffi.NULL, 0, 0) != 0:
                break

        return 0

    def readlink(self, path):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def release(self, path, info):
        return 0

    def releasedir(self, path, info):
        return 0

    def removexattr(self, path, name):
        file_id = self.lookup(ffi.string(path))
        self.access_violation(self.accesscontroller.xattr_remove(file=file_id, name=name))
        try:
            self.filefs.delete_xattr(file_id, name)
        except KeyError:
            raise OSError(ENODATA)
        return 0

    def rename(self, old, new, flags):
        if flags:
            raise OSError(EINVAL)

        old = ffi.string(old)
        new = ffi.string(new)
        old_parent_path, old_name = os.path.split(old)
        new_parent_path, new_name = os.path.split(new)
        old_parent = self.lookup(old_parent_path)
        new_parent = self.lookup(new_parent_path)
        file_id = self.lookup(old, parent=old_parent)
        try:
            existing_file_id = self.lookup(new, parent=new_parent)
        except OSError:
            pass
        else:
            _, old_header = self.filefs.get_file_header(file_id, 0)
            _, new_header = self.filefs.get_file_header(existing_file_id, 0)
            if old_header.file_type != new_header.file_type:
                raise OSError(ENOTDIR if old_header.file_type == FileType.dir.value else EISDIR)
            if new_header.file_type == FileType.dir.value:
                try:
                    next(self.pathfs.directory_entries(existing_file_id))
                except StopIteration:
                    pass
                else:
                    raise OSError(ENOTEMPTY)
            self.pathfs.remove_directory_entry(new_parent, new_name)
            self.filefs.delete_file(existing_file_id)
        self.access_violation(self.accesscontroller.dir_remove_file(dir=old_parent, name=old_name, file=file_id))
        self.access_violation(self.accesscontroller.dir_add_file(dir=new_parent, name=new_name))
        self.pathfs.add_directory_entry(new_parent, DirectoryEntry(new_name, file_id))
        self.pathfs.remove_directory_entry(old_parent, old_name)
        return 0

    def rmdir(self, path):
        path = ffi.string(path)
        parent_path, name = os.path.split(path)
        parent = self.lookup(parent_path)
        file_id = self.lookup(path, parent=parent)
        self.access_violation(self.accesscontroller.dir_remove_file(dir=parent, name=name, file=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.dir.value:
            raise OSError(ENOTDIR)
        self.access_violation(self.accesscontroller.dir_delete(dir=file_id))
        try:
            next(self.pathfs.directory_entries(file_id))
        except StopIteration:
            pass
        else:
            raise OSError(ENOTEMPTY)
        self.pathfs.remove_directory_entry(parent, name)
        self.filefs.delete_file(file_id)
        return 0

    def setxattr(self, path, name, value, size, options):
        file_id = self.lookup(ffi.string(path))
        name = ffi.string(name)
        value = ffi.buffer(value, size)[:]
        self.access_violation(self.accesscontroller.xattr_set(file=file_id, name=name, value=value))
        try:
            self.filefs.set_xattr(file_id, name, value,
                                  replace_only=bool(options & XATTR_REPLACE),
                                  create_only=bool(options & XATTR_CREATE))
        except KeyAlreadyExists:
            raise OSError(EEXIST)
        except KeyDoesNotExist:
            raise OSError(ENODATA)
        return 0

    def statfs(self, path, result):
        basefs_stat = os.statvfs(str(self.fname))
        result.f_bavail = basefs_stat.f_bavail * basefs_stat.f_bsize // self.blockfs.PHYSICAL_BLOCK_SIZE
        result.f_bfree = basefs_stat.f_bavail * basefs_stat.f_bsize // self.blockfs.PHYSICAL_BLOCK_SIZE
        result.f_blocks = self.blockfs.total_blocks() + 10
        result.f_bsize = self.blockfs.LOGICAL_BLOCK_SIZE
        result.f_favail = 1
        result.f_ffree = 1
        result.f_files = 1
        result.f_flag = ST_NOATIME | ST_NODEV | ST_NODIRATIME | ST_NOEXEC | ST_NOSUID | ST_SYNCHRONOUS
        result.f_frsize = self.blockfs.LOGICAL_BLOCK_SIZE
        result.f_namemax = self.pathfs.FILENAME_SIZE
        return 0

    def symlink(self, target, source):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def truncate(self, path, length, info):
        file_id = self.lookup(ffi.string(path), info)
        self.access_violation(self.accesscontroller.file_write(file=file_id))
        self.filefs.truncate_file_size(file_id, length)
        return 0

    def unlink(self, path):
        path = ffi.string(path)
        parent_path, name = os.path.split(path)
        parent = self.lookup(parent_path)
        file_id = self.lookup(path, parent=parent)
        self.access_violation(self.accesscontroller.dir_remove_file(dir=parent, name=name, file=file_id))
        self.access_violation(self.accesscontroller.file_delete(file=file_id))
        if self.filefs.get_file_header(file_id, 0)[1].file_type != FileType.file.value:
            raise OSError(EISDIR)
        self.pathfs.remove_directory_entry(parent, name)
        self.filefs.delete_file(file_id)
        return 0

    def utimens(self, path, times, info):  # XXX: UNSUPPORTED
        raise OSError(ENOSYS)

    def write(self, path, buf, size, offset, info):
        file_id = self.lookup(ffi.string(path), info)
        self.access_violation(self.accesscontroller.file_write(file=file_id))
        buf = ffi.buffer(buf, size)
        self.filefs.writer(file_id, offset).write(buf[:size], flush=True)
        return size
