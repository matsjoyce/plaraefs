"""
Usage:
    plaraefs mount <fname> <path>
    plaraefs check <fname>
"""


import fuse
import logging
import itertools

from .fusefilesystem import FUSEFilesystem
from .accesscontroller.dummy import DummyAccessController

logger = logging.getLogger(__name__)


def main(args=None):
    import docopt
    try:
        import iridescence
    except ImportError:
        iridescence = None

    import sys

    if iridescence:
        iridescence.quick_setup(level=logging.INFO)

    args = docopt.docopt(__doc__, argv=sys.argv[1:] if args is None else args)

    fs = FUSEFilesystem(args["<fname>"], DummyAccessController())

    if args["mount"]:
        fuse.FUSE(fs, args["<path>"],
                  foreground=True,
                  nothreads=True)
    if args["check"]:
        fs.init("")

        with fs.blockfs.lock_file(write=True):
            files_found = {fs.pathfs.ROOT_FILE_ID: ""}
            files_unchecked = {fs.pathfs.ROOT_FILE_ID}
            used_blocks = {}
            unused_blocks = set()

            for i in itertools.count():
                if i * fs.filefs.SUPERBLOCK_INTERVAL >= fs.blockfs.total_blocks():
                    break
                bitmap = fs.filefs.read_superblock(i)
                for bid, used in enumerate(bitmap):
                    block_id = bid + i * fs.filefs.SUPERBLOCK_INTERVAL
                    if used:
                        used_blocks[block_id] = None if bid else f"super {i}"
                    elif not bid:
                        print(f"Superblock {i} is not marked as used")
                    else:
                        if block_id < fs.blockfs.total_blocks():
                            data = fs.blockfs.read_block(block_id)
                            if data is not None:
                                print(f"Block {block_id} is unused but contains data")
                            unused_blocks.add(block_id)

            if fs.blockfs.total_blocks() != len(used_blocks) + len(unused_blocks):
                print("Total blocks does not equal found blocks:",
                      f"{fs.blockfs.total_blocks()} != {len(used_blocks)} + {len(unused_blocks)}")

            while files_unchecked:
                file_id = files_unchecked.pop()
                _, header = fs.filefs.get_file_header(file_id, 0)
                print("Found", files_found[file_id], "size", header.size, "blocks", fs.filefs.num_file_blocks(file_id))
                if header.file_type == 1:
                    for name, subfile in fs.pathfs.directory_entries(file_id):
                        files_found[subfile] = f"{files_found[file_id]}/{name.decode()}"
                        files_unchecked.add(subfile)

                header_num = 0
                header_block_id = file_id
                total_file_blocks = 0

                while header_block_id:
                    header = fs.filefs.read_file_header(file_id, header_num, header_block_id)
                    total_file_blocks += len(header.block_ids) + 1
                    for block_id in header.block_ids + [header_block_id]:
                        if block_id not in used_blocks:
                            if block_id > fs.blockfs.total_blocks():
                                print("File", files_found[subfile], "points to block", block_id,
                                      "but block does not exist")
                            else:
                                print("File", files_found[subfile], "points to block", block_id,
                                      "but block is not marked as used")
                                data = fs.blockfs.read_block(block_id)
                                if data is None:
                                    print("Block is empty")
                                else:
                                    print("Block data:", data[:100])
                        else:
                            used_blocks[block_id] = file_id

                    header_num += 1
                    header_block_id = header.next_header

                if fs.filefs.num_file_blocks(file_id) != total_file_blocks:
                    print("File blocks mismatch, has", total_file_blocks, "should be",
                          fs.filefs.num_file_blocks(file_id))

            for block_id, file_id in used_blocks.items():
                if file_id is None:
                    print("Block", block_id, "is marked as used but no file points to it")

            print(f"Found {len(used_blocks)} used blocks")
            print(f"Found {len(unused_blocks)} unused blocks")
            print(f"Found {len(files_found)} files")
            print(f"Found {list(used_blocks.values()).count('super')} super blocks")
