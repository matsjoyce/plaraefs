"""
Usage:
    test.py [--withinit]
"""

import docopt
import time
import pathlib

from plaraefs.filesystem import FileSystem

args = docopt.docopt(__doc__)

big_test_times = 25
data_multiplier = 2**20
chunk_size = 5 * 2 ** 10 - 3

#big_test_times = 1
#data_multiplier = 2**10
#chunk_size = 500

if args["--withinit"]:
    FileSystem.initialise("sandbox/a.plaraefs")


def print_general_info(fs):
    print()
    print(f"Free blocks: {len(fs.free_blocks())}")
    print(f"Total blocks: {fs.total_blocks()}")
    print(f"File ids: {', '.join(map(str, fs.list_file_ids()))}")
    print(f"File names: {', '.join(map(str, fs.list_file_names()))}")
    print()

sandbox = pathlib.Path() / "sandbox"
if not sandbox.exists():
    sandbox.mkdir()

fs = FileSystem(sandbox / "a.plaraefs", b"a" * 32)

print_general_info(fs)

print("Creating file `a`...")
a = fs.open("a", create=True)
print("File `a` created")

print_general_info(fs)

print("Renaming file `a` to `b`...")
a.add_name("b")

print_general_info(fs)

a.remove_name("a")
print("File `a` renamed")

print_general_info(fs)

print("Writing abc to file `b`...")
a.write("abc")
print("File `b` written to")

print_general_info(fs)

print("Reading  file `b`...")
print(a.read(100))
print("File `b` read")

print_general_info(fs)

print("Deleting file `b`...")
a.delete()
print("File `b` deleted")

print_general_info(fs)

persist = fs.open("persist", create=True)
if args["--withinit"]:
    persist.write("0")
old = persist.read(100)
print("persist contains", old)
print("updating...")
persist.write(str(int(old) + 1))


big = fs.open("big", create=True)
data = "asdfghjkl" * data_multiplier

print("Writing", len(data) * big_test_times / data_multiplier, "MiB")
t = time.time()
for i in range(big_test_times):
    big.write(data)
diff = time.time() - t
print("Complete", diff, "seconds", len(data) / diff / data_multiplier * big_test_times, "MiB/sec")

print("Reading", len(data) * big_test_times / data_multiplier, "MiB")
t = time.time()
for i in range(big_test_times):
    rdata = big.read(len(data))
    assert rdata == data
diff = time.time() - t
print("Complete", diff, "seconds", len(data) / diff / data_multiplier * big_test_times, "MiB/sec")

data = "ASDFGHJKL" * data_multiplier

print("Writing chunk test")
before = fs.block_writes
writer = fs.write_file_iter(big.file_id)
next(writer)
position = 0

t = time.time()
while position < len(data):
    writer.send(data[position:position + chunk_size].encode())
    position += chunk_size
try:
    writer.send(None)
except StopIteration:
    pass
else:
    err_writer_did_not_finish
diff = time.time() - t
print("Complete", diff, "seconds", len(data) / diff / data_multiplier, "MiB/sec")
assert fs.block_writes == len(fs.file_blocks(big.file_id)) + before

print("Reading chunk test")
reader = fs.read_file_iter(big.file_id)
next(reader)
ds = []
before = fs.block_reads
d = True
t = time.time()
while d:
    d = reader.send(chunk_size)
    ds.append(d)
diff = time.time() - t
print("Complete", diff, "seconds", len(data) / diff / data_multiplier, "MiB/sec")
ds = b"".join(ds).decode()

assert ds == data
assert fs.block_reads == len(fs.file_blocks(big.file_id)) + before


big.delete()
print("Done...")

print_general_info(fs)
