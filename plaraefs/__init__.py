"""
Usage:
    plaraefs mount <fname> <path>
"""


import fuse
import logging

from .fusefilesystem import FUSEFilesystem

logger = logging.getLogger(__name__)


def main(args=None):
    import docopt
    try:
        import iridescence
    except ImportError:
        iridescence = None

    import sys

    if iridescence:
        iridescence.quick_setup()

    args = docopt.docopt(__doc__, argv=sys.argv[1:] if args is None else args)

    if args["mount"]:
        fuse.FUSE(FUSEFilesystem(args["<fname>"]), args["<path>"], foreground=True)
