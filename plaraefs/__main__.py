try:
    from . import main
except SystemError:
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    from plaraefs import main

exit(main())
