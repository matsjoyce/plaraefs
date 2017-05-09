import abc


class AccessController(abc.ABC):
    def __init__(self):
        self.fs = None

    @abc.abstractmethod
    def file_read(self, file):
        pass

    @abc.abstractmethod
    def file_write(self, file):
        pass

    @abc.abstractmethod
    def file_delete(self, file):
        pass

    @abc.abstractmethod
    def dir_list(self, dir):
        pass

    @abc.abstractmethod
    def dir_lookup(self, dir, name, file):
        pass

    @abc.abstractmethod
    def dir_add_file(self, dir, name):
        pass

    @abc.abstractmethod
    def dir_remove_file(self, dir, name, file):
        pass

    @abc.abstractmethod
    def dir_delete(self, dir):
        pass

    @abc.abstractmethod
    def xattr_set(self, file, name, value):
        pass

    @abc.abstractmethod
    def xattr_get(self, file, name):
        pass

    @abc.abstractmethod
    def xattr_list(self, file):
        pass

    @abc.abstractmethod
    def xattr_lookup(self, file, name):
        pass

    @abc.abstractmethod
    def xattr_remove(self, file, name):
        pass
