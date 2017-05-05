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
