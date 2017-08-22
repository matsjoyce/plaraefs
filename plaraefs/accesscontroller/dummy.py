import logging
import os

from . import AccessController

logger = logging.getLogger(__name__)


def wrapper(func):
    def w(self, **kwargs):
        proc = os.readlink('/proc/{}/exe'.format(self.fs.process_info()[2]))
        logger.debug(f"Access request from {proc}: {func.__name__}{kwargs}")
        ret = func(self, **kwargs)
        if not ret:
            logger.debug(f"Access denied")
        return ret
    return w


class DummyAccessController(AccessController):
    @wrapper
    def file_read(self, file):
        return True

    @wrapper
    def file_write(self, file):
        return True

    @wrapper
    def file_delete(self, file):
        return True

    @wrapper
    def dir_list(self, dir):
        return True

    @wrapper
    def dir_lookup(self, dir, name, file):
        return True

    @wrapper
    def dir_add_file(self, dir, name):
        return True

    @wrapper
    def dir_remove_file(self, dir, name, file):
        return True

    @wrapper
    def dir_delete(self, dir):
        return True

    @wrapper
    def xattr_set(self, file, name, value):
        return True

    @wrapper
    def xattr_get(self, file, name):
        return True

    @wrapper
    def xattr_list(self, file):
        return True

    @wrapper
    def xattr_lookup(self, file, name):
        return True

    @wrapper
    def xattr_remove(self, file, name):
        return True
