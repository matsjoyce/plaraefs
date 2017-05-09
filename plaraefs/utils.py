import inspect
import functools
import collections


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


class LRUDict(collections.OrderedDict):
    def __init__(self, maxsize):
        super().__init__()
        self.maxsize = maxsize

    def __getitem__(self, key):
        try:
            self.move_to_end(key)
        except KeyError:
            pass
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.move_to_end(key)
        if len(self) > self.maxsize:
            self.popitem(last=False)


class BitArray:
    __slots__ = ["data", "start_search"]

    def __init__(self, data):
        self.data = bytearray(data)
        self.start_search = 0

    def search(self):
        for i in range(self.start_search, len(self.data)):
            byte = self.data[i]
            if byte != 255:
                self.start_search = i
                for j in range(8):
                    if not byte & (128 >> j):
                        yield i * 8 + j

    def tobytes(self):
        return bytes(self.data)

    def __iter__(self):
        for byte in self.data:
            for j in range(8):
                yield bool(byte & (128 >> j))

    def __getitem__(self, position):
        i, j = divmod(position, 8)
        return bool(self.data[i] & (128 >> j))

    def __setitem__(self, position, x):
        i, j = divmod(position, 8)
        if not x:
            self.start_search = min(self.start_search, i)
        if x:
            self.data[i] |= (128 >> j)
        else:
            self.data[i] &= ~(128 >> j)

    def count(self, x):
        return sum(1 for i in self if i == x)
