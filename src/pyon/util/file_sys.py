""" Utility for managing relative file system paths """

__author__ = "Luke Campbell, Michael Meisinger"

import errno
import StringIO
import tempfile
import shutil
import os
import re
import random
import string

from pyon.core.bootstrap import CFG as bootcfg, get_sys_name
from pyon.util.log import log
from pyon.util.containers import DotDict


class FileSystemError(Exception):
    """
    Client filesystem request failed
    """
    status_code = 411

    def get_status_code(self):
        return self.status_code

    def get_error_message(self):
        return self.message

    def __str__(self):
        return str(self.get_status_code()) + " - " + str(self.get_error_message())


class FileSystem(object):
    # These are static, and shared throughout the container, do not operate on a per-instance basis.
    FS_DIRECTORY_LIST = ['TEMP', 'CACHE', 'FILESTORE', 'SCIDATA']
    FS_DIRECTORY = DotDict(zip(FS_DIRECTORY_LIST, FS_DIRECTORY_LIST))

    FS = DotDict(zip(FS_DIRECTORY_LIST, FS_DIRECTORY_LIST))
    root = ''
    _instance = None


    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(FileSystem, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    @staticmethod
    def _get_fs_root(config):
        # Return the root dir for the container FS
        # Note that if different users run scion, be aware of common root file permissions
        return os.path.join(config.get_safe('container.filesystem.root', '/tmp/scion'), "scion_" + get_sys_name())

    @classmethod 
    def _clean(cls, config):
        """ Force cleans the FS root, but not any other mappings """
        if not cls.root:
            cls.root = cls._get_fs_root(config)
        log.info('Removing %s', cls.root)
        if os.path.exists(cls.root):
            shutil.rmtree(cls.root)

    def __init__(self, CFG):
        if not FileSystem.root:
            FileSystem.root = self._get_fs_root(CFG)

        fs_cfg = CFG.get_safe('container.filesystem') or {}

        for k, v in FileSystem.FS_DIRECTORY.iteritems():
            fsname = v.lower()  # Lower case string
            fsdir = fs_cfg.get(fsname, None)
            if fsdir:
                FileSystem.FS_DIRECTORY[k] = fsdir
            else:
                FileSystem.FS_DIRECTORY[k] = os.path.join(FileSystem.root, fsname)
            # Check to make sure you're within your rights to access this
            if not FileSystem._sandbox(FileSystem.FS_DIRECTORY[k]):
                raise OSError('Invalid FileSystem location. (%s is set to \'%s\')' % (k, FileSystem.FS_DIRECTORY[k]))
            if not os.path.exists(FS_DIRECTORY[k]):
                log.debug('Making path: %s', FS_DIRECTORY[k])
                self.__makedirs(FS_DIRECTORY[k])

        except_list = ["ROOT"]
        except_list.extend(FileSystem.FS_DIRECTORY.keys())
        for fsname, fsdir in fs_cfg.iteritems():
            fsalias = fsname.upper()
            if fsalias in except_list:
                continue
            if not fsdir:
                FileSystem.FS_DIRECTORY[fsalias] = os.path.join(FileSystem.root, fsname)
                FileSystem.FS[fsalias] = fsalias
                if not os.path.exists(FS_DIRECTORY[fsalias]):
                    log.debug('Making path: %s', FS_DIRECTORY[fsalias])
                    self.__makedirs(FS_DIRECTORY[fsalias])
            elif os.path.exists(fsdir):
                FileSystem.FS_DIRECTORY[fsalias] = fsdir
                FileSystem.FS[fsalias] = fsalias
            else:
                log.warn("FileSystem alias %s maps to non-existing dir: '%s'", fsalias, fsdir)

    @classmethod
    def __makedirs(cls,path):
        try:
            os.makedirs(path)
        except OSError as ose:
            if ose.errno != errno.EEXIST:
                raise

    @staticmethod
    def get(path):
        if path.startswith('/'): # Like it should
            path = path[1:]  # Strip the beginning /
        # Determine root
        tree = path.split('/')
        if tree[0].upper() not in FS:
            return None
        root = FileSystem.FS_DIRECTORY[tree.pop(0).upper()]
        fullpath = '/'.join([root] + tree)
        return fullpath


    @staticmethod
    def is_safe(path):
        """
        Ensure that the path is within the sandbox roots.
        """
        for root in FileSystem.FS_DIRECTORY.itervalues():
            if path.startswith(root):
                return True
        return False

    @staticmethod
    def _sandbox(path):
        """
        If the path is in a bad place return false.
        """
        # List of bad places
        black_list = [
            # Standard Unix
            '/',
            '/bin',
            '/sbin',
            '/usr/bin',
            '/usr/sbin'
            '/usr/local/sbin',
            '/etc',
            '/usr/etc',
            '/home',
            '/var',
            '/lib',
            '/usr/lib',
            # Linux
            '/lost+found',
            '/boot',
            '/dev',
            '/media',
            '/proc',
            '/sys',
            '/root',
            '/selinux',
            '/srv',
            '/mnt'
            # Darwin
            '/Application',
            '/Developer',
            '/Library',
            '/Network',
            '/System',
            '/Users',
            '/Volumes',
            '/include'
            '/private',
            '/cores'

        ]
        if path in black_list:
            return False

        return True


    @staticmethod
    def _parse_filename(file):
        # Remove whitespace
        ret = re.sub(r'\s', '_', file)

        # Remove non alphanumeric
        ret = re.sub(r'[~!@#$%^&*()-+,/\'\";:`<>?\\\]\[\}\{=]+', '', ret)

        # Limit 64 chars
        return ret[:64]

    @classmethod
    def get_url(cls, fs, filename, ext=''):
        """
        @param fs The file system enumeration for the resource where this file belongs. 'TEMP', 'LIBRARY' etc.
        @param file The filename to be used
        @param ext Optional: guarantees the file will have the extension specified
        @return The full path to the desired resource on the file system
        """
        path =  os.path.join(FS_DIRECTORY[fs], '%s%s' % (FileSystem._parse_filename(filename), ext))
        cls.__makedirs(path)
        return path

    @classmethod
    def get_hierarchical_url(cls, fs, filename, ext=''):
        """
        @param fs The file system enumeration for the resource where this file belongs. 'TEMP', 'LIBRARY' etc.
        @param file The filename to be turned into a path and name
        @param ext Optional: guarantees the file will have the extension specified
        @return The full path to the desired resource on the file system
        """
        clean_name = FileSystem._parse_filename(filename)

        if len(clean_name) < 6:
            return os.path.join(FS_DIRECTORY[fs], '%s%s' % (clean_name, ext))

        else:
            path = os.path.join(FS_DIRECTORY[fs], "%s/%s" % (clean_name[0:2], clean_name[2:4]))
            cls.__makedirs(path)

            return os.path.join(path, '%s%s' % (clean_name[4:], ext))

    @classmethod
    def get_extended_url(cls, path):
        if ':' in path:
            s = path.split(':')
            base = FileSystem.FS_DIRECTORY[s[0]]
            path = os.path.join(base, s[1])
            cls.__makedirs(path)
        return path

    @staticmethod
    def mktemp(filename='', ext=''):
        """
        @description Creates a temporary file that is semi-persistent
        @param filename Desired filename to use, if empty a random name is generated
        @param ext the optional file extension to use
        @return an open file to the desired temporary file
        """
        if filename:
            return open(FileSystem.get_url(fs=FS.TEMP, filename=filename, ext=ext), 'w+b')
        else:
            rand_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(24))
            return open(FileSystem.get_url(fs=FS.TEMP, filename=rand_str), 'w+b')

    # This needs some work getting this right in the mean-time use mktemp its ok....

    @staticmethod
    def mkstemp():
        """
        Obtain a secure file
        """
        return tempfile.TemporaryFile(mode='w+b', dir=FS_DIRECTORY[FS.TEMP])

    @staticmethod
    def unlink(filepath):
        """
        @description Removes a specified file or symlink
        @param filepath The absolute path to the file.
        @throws NotFound, BadRequest
        """
        if not FileSystem.is_safe(filepath):
            raise FileSystemError('It is not safe to remove %s, it is outside the scope of your permission.' % filepath)
        if not os.path.exists(filepath):
            raise FileSystemError('%s does not exist.' % filepath)
        try:
            os.unlink(filepath)
        except OSError as e:
            raise OSError('%s: %s' % (filepath, e.message))


    @staticmethod
    def memory_file():
        """
        Very fast file IO, great for temporary files and fast mechanisms, avoid arbitrarily large strings, will cause thrashing!
        """
        return StringIO.StringIO()

    @staticmethod
    def secure_file():
        """
        A method for secure file I/O, the file is immediately unlinked after creation
        """
        f = FileSystem.mktemp()
        FileSystem.unlink(f.name)
        return f

    @staticmethod
    def atomic_file(filename):
        """
        @description Create an atomic filename
        @param filename The desired (destination) file
        @return An AtomicFile
        """
        return AtomicFile(fname=filename)


class AtomicFile(object):
    """
    A write-only atomic file. Writing is performed to a temporary file and on close,
    the file is moved to the desired destination.

    This is an atomic action.

    This is ideal for threads, concurrency, crashes and saving state.

    """
    def __init__(self, fname):
        self.filename = fname
        self.file = FileSystem.mktemp()

    def write(self, text):
        self.file.write(text)

    def close(self):
        tmp_filename = self.file.name
        self.file.close()
        os.rename(tmp_filename, self.filename)


# Clients should either import this directory
#
FS_DIRECTORY = FileSystem.FS_DIRECTORY
FS = FileSystem.FS
