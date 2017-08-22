"""
Usage:
    plaraefs mount <fname> <path> [<accesscontroller>] [--debug] [--fuse-debug]
    plaraefs check <fname> [--fix-unreferenced] [--fix-unused-data] [--remove-corrupted] [--list-found] [--fix-nonexistent-entry]
    plaraefs prune <fname>
"""

import logging
import itertools
import pathlib

from .fusefilesystem import FUSEFilesystem
from .accesscontroller.dummy import DummyAccessController

logger = logging.getLogger(__name__)


def main(args=None):
    import importlib
    import sys
    import docopt

    try:
        import iridescence
    except ImportError:
        iridescence = None

    args = docopt.docopt(__doc__, argv=sys.argv[1:] if args is None else args)

    if iridescence:
        iridescence.quick_setup(level=logging.DEBUG if args.get("--debug", True) else logging.INFO)

    if args["mount"]:
        if args["<accesscontroller>"] is None:
            accesscontroller = "mark1.Mark1AccessController"
        else:
            accesscontroller = args["<accesscontroller>"]
        module, name = accesscontroller.split(".")
        cls = getattr(importlib.import_module(".accesscontroller." + module, package=__package__), name)
        print("Using accesscontroller", cls)
    else:
        cls = DummyAccessController

    fs = FUSEFilesystem(pathlib.Path(args["<fname>"]).absolute(), cls(), debug=args.get("--fuse-debug", False))

    if args["mount"]:
        fs.mount(pathlib.Path(args["<path>"]).resolve())

    if args["check"]:
        fs.init(object(), object())

        with fs.blockfs.lock_file(write=True):
            files_found = {fs.pathfs.ROOT_FILE_ID: ((), fs.pathfs.ROOT_FILE_ID)}
            files_unchecked = {fs.pathfs.ROOT_FILE_ID}
            used_blocks = {}
            unused_blocks = set()

            for i in itertools.count():
                if i * fs.filefs.SUPERBLOCK_INTERVAL >= fs.blockfs.total_blocks():
                    superblocks = i
                    break
                bitmap = fs.filefs.read_superblock(i)
                for bid, used in enumerate(bitmap):
                    block_id = bid + i * fs.filefs.SUPERBLOCK_INTERVAL
                    if used:
                        used_blocks[block_id] = None if bid else f"super {i}"
                        if block_id >= fs.blockfs.total_blocks():
                            print(f"Block {block_id} is marked as used but does not exist")
                    elif not bid:
                        print(f"Superblock {i} is not marked as used")
                    else:
                        if block_id < fs.blockfs.total_blocks():
                            data = fs.blockfs.read_block(block_id)
                            if data is not None:
                                print(f"Block {block_id} is unused but contains data")
                                if args["--fix-unused-data"]:
                                    fs.blockfs.wipe_block(block_id)
                                else:
                                    print("Use --fix-unused-data")
                            unused_blocks.add(block_id)

            if fs.blockfs.total_blocks() != len(used_blocks) + len(unused_blocks):
                print("Total blocks does not equal found blocks:",
                      f"{fs.blockfs.total_blocks()} != {len(used_blocks)} + {len(unused_blocks)}")

            while files_unchecked:
                file_id = files_unchecked.pop()
                try:
                    _, header = fs.filefs.get_file_header(file_id, 0)
                except:
                    print("Corrupted file header for ", files_found[file_id][0],
                          "file id", file_id)
                    if args["--remove-corrupted"]:
                        fs.pathfs.remove_directory_entry(files_found[file_id][1], files_found[file_id][0][-1])
                        fs.filefs.deallocate_blocks([file_id])
                    else:
                        print("Use --remove-corrupted")
                    continue
                if args["--list-found"]:
                    print("Found", files_found[file_id][0],
                          "file id", file_id,
                          "size", header.size,
                          "blocks", fs.filefs.num_file_blocks(file_id))
                if header.file_type == 1:
                    for entry in fs.pathfs.directory_entries(file_id):
                        files_found[entry.file_id] = files_found[file_id][0] + (entry.name,), file_id
                        files_unchecked.add(entry.file_id)
                        if entry.file_id not in used_blocks:
                            print("Directory entry", files_found[entry.file_id][0], "does not point to a used block")
                            if args["--fix-nonexistent-entry"]:
                                fs.pathfs.remove_directory_entry(file_id, entry.name)
                            else:
                                print("Use --fix-nonexistent-entry")

                header_num = 0
                header_block_id = file_id
                total_file_blocks = 0

                while header_block_id:
                    header = fs.filefs.read_file_header(file_id, header_num, header_block_id)
                    total_file_blocks += len(header.block_ids) + 1
                    for block_id in header.block_ids + [header_block_id]:
                        if block_id not in used_blocks:
                            if block_id > fs.blockfs.total_blocks():
                                print("File", files_found[file_id][0], "points to block", block_id,
                                      "but block does not exist")
                            else:
                                print("File", files_found[file_id][0], "points to block", block_id,
                                      "but block is not marked as used")
                                data = fs.blockfs.read_block(block_id)
                                if data is None:
                                    print("Block is empty")
                                else:
                                    print("Block data:", data[:100])
                        else:
                            data = fs.blockfs.read_block(block_id)
                            if data is None:
                                print("File", files_found[file_id][0], "points to block", block_id,
                                      "but block is empty")
                            used_blocks[block_id] = file_id

                    header_num += 1
                    header_block_id = header.next_header

                if fs.filefs.num_file_blocks(file_id) != total_file_blocks:
                    print("File blocks mismatch, has", total_file_blocks, "should be",
                          fs.filefs.num_file_blocks(file_id))

            for block_id, file_id in used_blocks.items():
                if file_id is None:
                    print("Block", block_id, "is marked as used but no file points to it")
                    if args["--fix-unreferenced"]:
                        bitmap = fs.filefs.read_superblock(block_id // fs.filefs.SUPERBLOCK_INTERVAL)
                        bitmap[block_id % fs.filefs.SUPERBLOCK_INTERVAL] = False
                        fs.filefs.write_superblock(block_id // fs.filefs.SUPERBLOCK_INTERVAL, bitmap)
                        fs.blockfs.wipe_block(block_id)
                    else:
                        print("Use --fix-unreferenced")

            print(f"Found {len(used_blocks)} used blocks")
            print(f"Found {len(unused_blocks)} unused blocks")
            print(f"Found {len(files_found)} files")
            print(f"Found {superblocks} super blocks")

    if args["prune"]:
        fs.init(object(), object())

        with fs.blockfs.lock_file(write=True) as f:
            last_used = 0
            for i in itertools.count():
                if i * fs.filefs.SUPERBLOCK_INTERVAL >= fs.blockfs.total_blocks():
                    superblocks = i
                    break
                free = 0
                bitmap = fs.filefs.read_superblock(i)
                for bid, used in enumerate(bitmap):
                    block_id = bid + i * fs.filefs.SUPERBLOCK_INTERVAL
                    if used:
                        if bid:
                            last_used = block_id
                    else:
                        free += 1
                print(f"Superblock {i}: {free} free blocks")
            print(f"Last used block is {last_used}, pruning {fs.blockfs.total_blocks() - last_used + 1} blocks")
            f.truncate((last_used + 1) * fs.blockfs.PHYSICAL_BLOCK_SIZE + fs.blockfs.offset)
