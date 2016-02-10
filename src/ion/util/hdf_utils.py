""" Utility functions wrapping various HDF5 binaries """

__author__ = 'Christopher Mueller, Michael Meisinger'

import os
import re
import shutil
import subprocess
import fcntl
import gevent
from gevent.lock import RLock
from pyon.public import log
try:
    import h5py
except ImportError:
    h5py = None


def repack(infile_path, outfile_path=None):
    if not os.path.exists(infile_path):
        raise IOError("Input file does not exist: '{0}'".format(infile_path))

    replace = False
    if outfile_path is None:
        replace = True
        outfile_path = infile_path + '_out'

    try:
        subprocess.check_output(['h5repack', infile_path, outfile_path])
        if replace:
            os.remove(infile_path)
            shutil.move(outfile_path, infile_path)
    except subprocess.CalledProcessError:
        if os.path.exists(outfile_path):
            os.remove(outfile_path)
        raise


def space_ratio(infile_path):
    if not os.path.exists(infile_path):
        raise IOError("Input file does not exist: '{0}'".format(infile_path))

    #_metadata = r'File metadata: (\d*) bytes'
    _unaccounted = r'Unaccounted space: (\d*) bytes'
    #_raw_data = r'Raw data: (\d*) bytes'
    _total = r'Total space: (\d*) bytes'
    try:
        output = subprocess.check_output(['h5stat', '-S', infile_path])
        #meta = float(re.search(_metadata, output).group(1))
        unaccounted = float(re.search(_unaccounted, output).group(1))
        #raw = float(re.search(_raw_data, output).group(1))
        total = float(re.search(_total, output).group(1))

        return unaccounted/total
    except subprocess.CalledProcessError:
        raise


def dump(infile_path):
    if not os.path.exists(infile_path):
        raise IOError("Input file does not exist: '{0}'".format(infile_path))

    try:
        subprocess.check_output(['h5dump', '-BHA', infile_path], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        raise


def has_corruption(infile_path):
    if not os.path.exists(infile_path):
        raise IOError("Input file does not exist: '{0}'".format(infile_path))

    try:
        # Try dumping the file - most read corruptions can be found this way
        dump(infile_path)
    except subprocess.CalledProcessError:
        return True

    # Other mechanisms for detecting corruption?
    return False


class HDFLockingFile(h5py.File if h5py else object):
    __sh_locks = {}
    __ex_locks = {}
    __rlock = RLock()

    def __init__(self, *args, **kwargs):
        """ See also: https://github.com/theochem/horton/blob/master/horton/io/lockedh5.py """
        if not h5py:
            raise Exception("Requires h5py")
        retry_count = kwargs.pop("retry_count", 10)
        retry_wait = kwargs.pop("retry_wait", 1.0)

        # Try to open file (h5py may detect multiple writes)
        for num in xrange(retry_count):
            try:
                h5py.File.__init__(self, *args, **kwargs)
                break
            except IOError:
                if num == retry_count-1:
                    raise
                else:
                    gevent.sleep(retry_wait)

        # Try to acquire a lock
        for num in xrange(retry_count):
            try:
                self.lock()
                break
            except IOError:
                if num == retry_count-1:
                    raise
                else:
                    gevent.sleep(retry_wait)

    def lock(self):
        with self.__rlock:
            if self.driver != 'sec2':
                raise ValueError("Invalid h5py File driver")

            # Have to use a cache because the if the flock call comes from the same pid,
            # it will be ignored instead of setting errno
            if self.mode == "r":
                if self.filename in self.__ex_locks:
                    raise IOError('[Errno 11] Resource temporarily unavailable, cached lock on %s' % self.filename)

                if self.filename in self.__sh_locks:
                    self.__sh_locks[self.filename] += 1
                else:
                    self.__sh_locks[self.filename] = 1

                fd = self.fid.get_vfd_handle()
                try:
                    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except IOError:
                    del self.__sh_locks[self.filename] # Eject it from the cache, the filesystem has it
                    raise

            else:
                if self.filename in self.__sh_locks or self.filename in self.__ex_locks:
                    raise IOError('[Errno 11] Resource temporarily unavailable, cached lock on %s' % self.filename)

                self.__ex_locks[self.filename] = 1

                # Using sec2 and not reading
                fd = self.fid.get_vfd_handle()
                # Lock the file
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except IOError:
                    del self.__ex_locks[self.filename] # Eject it from the cache, the filesystem has it
                    raise

    def unlock(self):
        with self.__rlock:
            if self.mode == 'r':
                fd = self.fid.get_vfd_handle()
                fcntl.flock(fd, fcntl.LOCK_UN | fcntl.LOCK_NB)
                if self.__sh_locks[self.filename] > 1:
                    self.__sh_locks[self.filename] -= 1
                else:
                    del self.__sh_locks[self.filename]
            else:
                fd = self.fid.get_vfd_handle()
                fcntl.flock(fd, fcntl.LOCK_UN | fcntl.LOCK_NB)
                del self.__ex_locks[self.filename]

    @classmethod
    def force_unlock(cls, path):
        with cls.__rlock:
            fd = os.open(path, os.O_RDONLY)
            fcntl.flock(fd, fcntl.LOCK_UN | fcntl.LOCK_NB)
            del cls.__ex_locks[path]
            del cls.__sh_locks[path]

    def close(self):
        self.unlock()

        h5py.File.close(self)
