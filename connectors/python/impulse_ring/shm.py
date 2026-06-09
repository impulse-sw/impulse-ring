"""POSIX shared-memory segments via /dev/shm files + mmap.

Linux maps a POSIX shm name "/foo" to the file /dev/shm/foo, so opening that
path directly is equivalent to shm_open and needs no librt binding.
"""

import mmap
import os

SHM_DIR = "/dev/shm"


def _path(name: str) -> str:
    return SHM_DIR + name


def create(name: str, size: int) -> mmap.mmap:
    """Create (or truncate) a segment of `size` bytes and map it shared RW."""
    fd = os.open(_path(name), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.ftruncate(fd, size)
        return mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
    finally:
        os.close(fd)


def open_segment(name: str) -> mmap.mmap:
    """Open an existing segment created by another process and map it RW."""
    fd = os.open(_path(name), os.O_RDWR)
    try:
        size = os.fstat(fd).st_size
        if size == 0:
            raise OSError("segment has zero size")
        return mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
    finally:
        os.close(fd)


def unlink(name: str) -> None:
    try:
        os.unlink(_path(name))
    except FileNotFoundError:
        pass
