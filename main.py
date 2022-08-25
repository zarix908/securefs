from __future__ import print_function, absolute_import, division
import argparse

import logging
import os

from errno import EACCES
from multiprocessing import Process, Queue
from os.path import realpath
from threading import Lock

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from handler import handle
from fssignal import Signal, NodeType


class SignalFs(LoggingMixIn, Operations):
    def __init__(self, root, signals):
        self.root = realpath(root)
        self.__signals = signals
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        return super(SignalFs, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode, **kwargs):
        self.__signals.put(Signal(path, NodeType.FILE))
        return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
            return os.fdatasync(fh)
        else:
            return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
            'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(self.root + source, target)

    listxattr = None
    mknod = os.mknod
    open = os.open

    def mkdir(self, path, mode):
        self.__signals.put(Signal(path, NodeType.DIRECTORY))
        os.mkdir(path, mode)

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        self.__signals.put(Signal(self.root + new, NodeType.UNKNOWN))
        return os.rename(old, self.root + new)

    def rmdir(self, path):
        self.__signals.put(Signal(path, NodeType.DIRECTORY))
        os.rmdir(path)

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in (
            'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
            'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        self.__signals.put(Signal(path, NodeType.FILE))
        with open(path, 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        with self.rwlock:
            self.__signals.put(Signal(path, NodeType.FILE))
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('root')
    parser.add_argument('mount')
    args = parser.parse_args()

    signals = Queue()
    p = Process(target=handle, args=(signals,))
    p.start()

    logging.basicConfig(level=logging.DEBUG)
    FUSE(SignalFs(args.root, signals), args.mount, foreground=True)

    p.join()


if __name__ == '__main__':
    main()
