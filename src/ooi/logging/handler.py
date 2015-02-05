
import logging.handlers
import StringIO
import threading
import sys
import os

try:
    unicode
    _unicode = True
except NameError:
    _unicode = False

BLOCK_SIZE = 512

# instead of asking OS for size of file after each log message,
# this handler keeps track of the original size and how much was appended.
# when DEBUGGING_POSITION is True, it verifies that this matches the OS size.
# set to False to reduce OS and possibly very slow HW calls.
DEBUG_POSITION=False

class BlockIOFileHandler(logging.handlers.RotatingFileHandler):
    """ standard RotatingFileHandler except
        - writes for most messages are accumulated and written as full 512-byte blocks
        - writes for severe messages (>= configurable level) cause buffer to be written immediately
        - rollover occurs after writing message, not before
    """
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0):
        super(BlockIOFileHandler,self).__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self._buffer = StringIO.StringIO()
        self._lock = threading.Lock()
        self._write_level = logging.ERROR
        self._first_write = True

    def emit(self, record):
        with self._lock:
            self._format_into_buffer(record)
            if self._should_write(record):
                self._write_buffer(record)
                if self.shouldRollover():
                    self.doRollover()

    def _format_into_buffer(self, record):
        # format and add to text buffer
        msg = self.format(record)
        stream = self._buffer
        if self._first_write:
            self._write_first(msg)
            self._first_write = False
        else:
            self._write(msg)

    def _write_first(self, msg):
        """ go through logic from logging.StreamHandler.emit() once,
            but remember result to re-apply to other log messages
        """
        if not _unicode:
            self._write_default(msg)
            self._write = self._write_default
        else:
            try:
                if isinstance(msg, unicode) and getattr(self.stream, 'encoding', None):
                    self._ufs = '%s\n'.decode(self.stream.encoding)
                    try:
                        self._write_unicode_format(msg)
                        self._write = self._write_unicode_format
                    except UnicodeEncodeError:
                        self._write_encoded_string(msg)
                        self._write = self._write_encoded_string
                else:
                    self._write_default(msg)
                    self._write = self._write_default
            except UnicodeEncodeError:
                self._write_utf(msg)
                self._write = self._write_utf


    def _write_default(self, msg):
        self._buffer.write('%s\n' % msg)

    def _write_unicode_format(self, msg):
        self._buffer.write(self._ufs % msg)

    def _write_encoded_string(self, msg):
        unicode = (self._ufs % msg).encode(self.stream.encoding)
        self._buffer.write(unicode)

    def _write_utf(self, msg):
        self._write_default(msg.encode("UTF-8"))

    def _should_write(self, record):
        return self._buffer.len>=512 or record.levelno>=self._write_level

    def _write_buffer(self, record):
        # determine how much to write so file is on block boundary (unless severe message)
        len = self._buffer.len
        if record.levelno<self._write_level:
            bytes_beyond_next_block = (self._file_size+len) % BLOCK_SIZE
            write_len = len - bytes_beyond_next_block
        else:
            write_len = len
        # write accumulated messages
        if write_len>0:
            all_text = self._buffer.getvalue()
            write_text = all_text[:write_len]
            remaining_text = all_text[write_len:]
            try:
                self.stream.write(write_text)
                self.flush()
                self._file_size += write_len
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                self.handleError(record)

    def _open(self):
        stream = super(BlockIOFileHandler,self)._open()
        stream.seek(0, 2)
        self._file_size = stream.tell()
        return stream

    def shouldRollover(self):
        """
        overrides RotatingFileHandler.shouldRollover
        except this is called after the write completes, so uses file size instead of adding message size
        """
        if self.stream is None:                 # delay was set...
            self.stream = self._open()
        if self.maxBytes > 0:                   # are we rolling over?
            if DEBUG_POSITION:
                self.stream.seek(0, 2)  #due to non-posix-compliant Windows feature
                position = self.stream.tell()
                if position <> self._file_size:
                    print >> sys.stderr, 'BLOCK LOGGER FILE SIZE ERROR: calculated %d, filesystem reports %d bytes' % (self._file_size, position)
            else:
                position = self._file_size
            if position >= self.maxBytes:
                    return 1
        return 0


class PIDFileHandler(logging.handlers.RotatingFileHandler):
    """ standard RotatingFileHandler except
        the filename should contain one '%d'
        which will be substituted with the PID
    """
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0):
        super(BlockIOFileHandler,self).__init__(filename % os.getpid(), mode, maxBytes, backupCount, encoding, delay)
