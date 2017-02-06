plarae-fs - Process Level Access Restricted And Encrypted File System
=====================================================================

An attempt to make a virtual file system that can control file permissions on a process level.

Warning!
--------

This code is in active development and will break / change API without blinking. Seriously, don't use this!

Requirements
============

 - [Python 3.6](https://www.python.org/)
 - [cryptography](https://pypi.python.org/pypi/cryptography/1.7.1)
 - [fusepy](https://pypi.python.org/pypi/fusepy/2.0.4)
 - [msgpack](https://pypi.python.org/pypi/msgpack-python)


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
 - `GROUP_TAG_SIZE`
     - The length of the group tag (used for access restriction)
     - Default: 16

Superblock
----------

 - Positioned every `LOGICAL_BLOCK_SIZE * 8` blocks
 - Contains bitmap of free blocks

File header block
-----------------

 - Starts with mode byte
 - Followed by group tag (`GROUP_TAG_SIZE` long)
 - Followed by `FILESIZE_SIZE` 64-bit int representing the file size.
 - Followed by `BLOCK_ID_SIZE` block id for the next file continuation block, 0 if there isn't one
 - Followed by 32 `BLOCK_ID_SIZE` block ids indicating the next blocks
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


For reference
=============

 - With -O

V1 stats:

```
Writing 225.0 MiB
Complete 5.214540004730225 seconds 43.14858066021116 MiB/sec
Reading 225.0 MiB
Complete 3.341447114944458 seconds 67.3360948894563 MiB/sec
Writing chunk test
Complete 2.2614645957946777 seconds 3.9797218213081966 MiB/sec
Reading chunk test
Complete 0.27364468574523926 seconds 32.88936518350266 MiB/sec
```

V2 stats:
```
Writing 360.0 MiB
Complete in 7.5445098876953125 seconds 47.71681730938421 MiB/sec
Reading 360.0 MiB
Complete 5.980628728866577 seconds 60.19434014728175 MiB/sec
Writing chunk test
Complete 4.950728178024292 seconds 27.26871586269861 MiB/sec
Reading chunk test
Complete 3.2899115085601807 seconds 41.03453836029843 MiB/sec
```
