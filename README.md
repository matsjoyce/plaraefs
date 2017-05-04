plarae-fs - Process Level Access Restricted And Encrypted File System
=====================================================================

An attempt to make a virtual file system that can control file permissions on a process level.

Warning!
--------

This code is in active development and will break / change API without blinking. Seriously, don't use this!

Requirements
============

 - [Python 3.5+](https://www.python.org/) or [PyPy 3.5-v5.7.1+](http://pypy.org/)
 - [bitarray](https://pypi.python.org/pypi/bitarray) (PyPy requires that patch found [here](https://github.com/ilanschnell/bitarray/pull/47))
 - [bcrypt](https://pypi.python.org/pypi/bcrypt)
 - [cryptography](https://pypi.python.org/pypi/cryptography)
 - [docopt](https://pypi.python.org/pypi/docopt)
 - [fusepy](https://pypi.python.org/pypi/fusepy)
 - [namedlist](https://pypi.python.org/pypi/namedlist)
 - [pylru](https://pypi.python.org/pypi/pylru)

Optional:

 - [coverage](https://pypi.python.org/pypi/coverage)
 - [flake8](https://pypi.python.org/pypi/flake8)
 - [iridescence](https://github.com/matsjoyce/iridescence)
 - [pytest](https://pypi.python.org/pypi/pytest)


Filesystem structure
====================

Constants
---------

All numbers in bytes

 - `PHYSICAL_BLOCK_SIZE`
     - The size of the encrypted blocks written by the cryptographic block system
     - Default: 4096 (4 KiB)
 - `IV_SIZE`
     - The size of the initialisation vector for AES
     - Default: 16
 - `TAG_SIZE`
     - The size of the AEAD tag used for authentication
     - Default: 16
 - `LOGICAL_BLOCK_SIZE`
     - The block size exposed by the cryptographic block system
     - Value: `PHYSICAL_BLOCK_SIZE - IV_SIZE - TAG_SIZE`
     - Default: 4064
 - `BLOCK_ID_SIZE`
     - The address size for addressing blocks
     - Default: 8 (64 bit address)
 - `FILENAME_SIZE`
     - The length of filenames (path components)
     - Default: 256
 - `FILESIZE_SIZE`
     - The largest possible file
     - Default: 8 (max file size of 2**64)
 - `XATTR_INLINE_SIZE`
     - The length of the embedded xattr field
     - Default: 256

Superblock
----------

 - Positioned every `LOGICAL_BLOCK_SIZE * 8` blocks
 - Contains bitmap of free blocks

File header block
-----------------

 - Starts with mode byte
 - Followed by `FILESIZE_SIZE` 64-bit int representing the file size.
 - Followed by `BLOCK_ID_SIZE` block id for the next file continuation block, 0 if there isn't one
 - Followed by 32 `BLOCK_ID_SIZE` block ids indicating the next blocks
 - Followed by `BLOCK_ID_SIZE` block id for xattr additional space
 - Followed by `XATTR_INLINE_SIZE` xattr storage
 - Followed by data

File header continuation block
------------------------------

 - Starts `BLOCK_ID_SIZE` block id for the next file continuation block, 0 if there isn't one
 - Followed by `BLOCK_ID_SIZE` block id for the previous file continuation block
 - Followed by 32 `BLOCK_ID_SIZE` block ids indicating the next blocks
 - Followed by data

Normal file
-----------

 - Mode byte is 0
 - File data is contained in data section

Directory
---------

 - Mode byte is 1
 - Data is subfiles encoded in the following way:
     - `FILENAME_SIZE` subfile name
     - Followed by `BLOCK_ID_SIZE` block id to the file header block

Maximums
--------

 - Maximum file system size is 2**76 bytes (4 ZiB)
 - Maximum file size is 2**64 bytes (16 EiB)
