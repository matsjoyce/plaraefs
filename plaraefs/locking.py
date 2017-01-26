import os


if os.name == "nt":  # pragma: no cover
    import msvcrt

    # FIXME untested!

    def lock_file(file, write):
        if write:
            msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)

    def unlock_file(file):
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)


else:
    import fcntl

    def lock_file(file, write):
        if write:
            flags = fcntl.LOCK_EX
        else:
            flags = fcntl.LOCK_SH
        fcntl.lockf(file.fileno(), flags)

    def unlock_file(file):
        fcntl.lockf(file.fileno(), fcntl.LOCK_UN)
