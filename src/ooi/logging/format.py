
import logging
import traceback
import ooi.exception
import sys

class StackFormatter(logging.Formatter):
    """ logging formatter that:
        - displays exception stack traces one line per frame, with aligned columns
        - displays multiple chained stack traces and labels for ApplicationException objects that have them

        a subclass may customize the display by:
        - overriding format_stack, which is called for each stack found;
        - overriding filter_frames, which is called by the format_stack to limit display to a subset of frames

        USAGE

        Example lines from logging.yml:

            formatters:
              stacky:
                (): 'ooi.logging.format.StackFormatter'
                format: '%(asctime)s %(levelname)-8s %(threadName)s %(name)-15s:%(lineno)d %(message)s'
            handlers:
              console:
                class: logging.StreamHandler
                level: DEBUG
                stream: ext://sys.stdout
                formatter: stacky

    """
    def __init__(self,*a,**b):
        super(StackFormatter,self).__init__(*a,**b)
        self.set_filename_width(40)

    def set_filename_width(self, width):
        self._filename_width = width
        self._format_string = '%%%ds:%%-7d%%s'%width #ie- "%40s:%-7d%s" --> .../path/to/filename.py:123    line.of_code()

    def formatException(self, record):
        try:
            type,ex,tb = sys.exc_info()
            # use special exception logging only for IonExceptions with more than one saved stack
            if isinstance(ex, ooi.exception.ApplicationException):
                stacks = ex.get_stacks()
            else:
                stacks = [ ('exception: '+str(ex), traceback.extract_tb(tb) if tb else None) ]
            lines = []
            for label,stack in stacks:
                lines += self.format_stack(label, stack)
            return '\n'.join(lines)
        except Exception,e:
            #print >> sys.stderr, 'WARNING: StackFormatter could not dislay stack: %s (submitting to default formatter)' % (e,)
            return super(StackFormatter,self).formatException(record)

    def format_stack(self, label, stack):
        first_stack = label=='__init__'  # skip initial label -- start output with first stack frame
        if not first_stack:
            yield '   ----- '+label+' -----'

        frames = self.filter_frames(first_stack, stack)

        # create format string to align frames evenly, limit filename to 40chars
        w=self._filename_width
        p=-w
        s=self._format_string
        for file,line,method,code in frames:
            file_part = file if len(file)<w else file[p:]
            yield s%(file_part,line,code)

    def filter_frames(self, first_stack, stack):
        """ override in subclass to display a subset of the stack,
            default is to display all frames
        """
        return stack


class RawRecordFormatter(logging.Formatter):
    """ non-readable formatter that encodes all record fields to store for later processing.
        to re-read the file later, define a function "handle_raw" and exec the log entries.
    """
    def format(self, record):
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
            record.exc_info = None
        return 'handle_record_dict(' + repr(record.__dict__) + ')\n'


class FieldFormatter(logging.Formatter):
    """ display special values added by AddField filter.
        useful for debugging extra fields added via the threadlocal mechanism
        without having to use graylog to see them.

        create with logging.yml:

          formatter:
            (): ooi.logging.format.FieldFormatter
            fields: threadID,userName,conversationID

        and set these fields in code:

          from ooi.logging import config
          config.set_logging_fields( {'call':'conversationID'}, {'userName':'bob'} )

        and set the thread-local parts

          import threading
          x = threading.local()
          x.call = 'abc'

          from ooi.logging import log
          log.warning('pop goes the weasel')

        should produce this output:

          <date,time,level,etc> threadID?? userName=bob conversationID=abc: pop goes the weasel
    """

    def __init__(self, fields,*a,**b):
        super(logging.Formatter,self).__init__(*a,**b)
        self.fields = ','.split(fields)

    def format(self, record):
        line = ''
        for field in self.fields:
            if hasattr(record,field):
                line += '%s: %s '%(field,getattr(record,field))
            else:
                line += '%s?? '%field
        line += ': ' + record.message
        record.message = line
        return super(logging.Formatter,self).format(record)
