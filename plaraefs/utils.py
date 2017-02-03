import inspect
import functools


def ceildiv(a, b):
    # http://stackoverflow.com/a/17511341/3946766
    return -(-a // b)


def check_types(func):
    #if __debug__:
        #sig = inspect.signature(func)

        #@functools.wraps(func)
        #def ct_wrapper(*args, **kwargs):
            #for arg, param in zip(args, sig.parameters.values()):
                #if param.annotation is not inspect._empty and not isinstance(arg, param.annotation):
                    #raise ValueError(f"Argument {param.name} must be of type {param.annotation}, not {type(arg)}")
            #for name, arg in kwargs.items():
                #param = sig.parameters[name]
                #if param.annotation is not inspect._empty and not isinstance(arg, param.annotation):
                    #raise ValueError(f"Argument {param.name} must be of type {param.annotation}, not {type(arg)}")
            #return func(*args, **kwargs)
        #return ct_wrapper
    return func
