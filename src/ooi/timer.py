"""
record time taken for code to perform several steps
collect and report statistics for timing and other metrics

NOTE: by default Timer and Accumulator will use special loggers.  for example, if you create a Timer
    in module foo/bar/baz.py:
        t = Timer()
        t.log()
"""

import time
import logging
from threading import Lock
import math
import inspect
from ooi.logging import log

def _get_calling_module(default_value=None):
    try:
        stack = inspect.stack()
        # stack[0]: call to inspect.stack() on the line above
        # stack[1]: call to _get_calling_module() below
        # stack[2] # call to _SelfLogging.__init__() by subclass
        frame=stack[3] # call to Timer() or Accumulator() by caller
        if frame and frame[0]:
            module = inspect.getmodule(frame[0])
            if module:
                return module.__name__
            elif frame[1]:
                return frame[1]
    except:
        log.warning('failed to inspect calling module', exc_info=True)
        return default_value

class _SelfLogging(object):
    """ base class provides shared logging behavior of Timer and Accumulator """
    def __init__(self, name, logger, level, prefix):
        if name:
            self.name = name
        else:
            self.name = _get_calling_module(default_value='unspecified')
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(prefix + '.' + self.name)
        self.level = level

    def is_log_enabled(self):
        return self.logger.isEnabledFor(self.level)

    def _log(self):
        if self.is_log_enabled():
            self.logger.log(self.level, str(self))


class Timer(_SelfLogging):
    def __init__(self, name=None, logger=None, level=logging.DEBUG, milliseconds=True, number_format='%f'):
        """
        @param name: override module name used for default logger with logging.getLogger('timing.'+name)
        @param logger: override default logger with this
        @param level: log times at this log level
        @param milliseconds: report time in milliseconds (vs seconds)?
        @param number_format: '%f' used by default
        """
        super(Timer, self).__init__(name, logger, level, 'timing')

        self.times = [] # list of tuples (msg, time)
        self.complete_step("start")
        self.multiplier = 1000 if milliseconds else 1
        self.lone_number_format = ' '+number_format
        self.label_number_format = ' %s='+number_format

    def complete_step(self, label=None):
        self.times.append((label, time.time()))

    def __str__(self):
        # special case if only have start time (complete_step never called)
        if len(self.times)==1:
            return 'start time: ' + self.times[0][1]

        # otherwise message has format: elapsed TOTAL unit
        message = 'elapsed' + self.lone_number_format%self.multiplier*self._elapsed() + ' s' if self.multiplier==1 else ' ms'
        if len(self.times)==2:
            return message

        # or: elapsed TOTAL unit: num1 num2 ...
        # or: elapsed TOTAL unit: step1=num1 step2=num2 ...
        message += ':'
        for pair_o_tuples in zip(self.times[1:],self.times[:-1]):
            # looping through adjacent pairs: [(labelN,timeN), (labelN+1,timeN+1)]
            delta = self.multiplier*(pair_o_tuples[0][1]-pair_o_tuples[1][1])
            label = pair_o_tuples[1][0]
            if label:
                message += self.label_number_format%(label,delta)
            else:
                message += self.lone_number_format%delta
        return message

    def _elapsed(self):
        first_step = self.times[0]
        last_step = self.times[-1]
        return last_step[1]-first_step[1]

    def log(self, min=0):
        if min and self._elapsed()>=min:
            self._log()

_persisted_accumulators = {}
def get_accumulators():
    return _persisted_accumulators

class Accumulator(_SelfLogging):
    """ calculate average, standard deviation, min and max of named values
        reported directly (with add_value) or results of a Timer (with add)

        callers can log() or query stats get_min(step), get_average(step), etc
        or use Accumulator(trigger_step='publish', trigger_frequency=500) to automatically log results periodically
        ie- With arguments above, every 500 times the "publish" step is recorded with add(Timer) or add_value('publish', value)
            the accumulator will write a log message reporting min, max, average, stddev of each step recorded.
            By default, it then clears stats (so each log message is the most recent N measurements),
            or use trigger_clear=False to keep a running average until an explicit clear().
    """
    def __init__(self, name=None, logger=None, level=logging.INFO, format='%2f', keys='all',
                 trigger_key=None, trigger_frequency=1000, trigger_clear=True, persist=False):
        """
        @param name: override module name used for default logger with logging.getLogger('stats.'+name)
        @param logger: override default logger with this
        @param level: log times at this log level
        @param format: '%2f' used by default
        @param keys: list of which values to report with str() or log(), 'total' for just total, 'all' for all keys, '!total' for all but total
        @param trigger_key: count for this key may result in logging
        @param trigger_frequency: when count of trigger_key is a multiple of this value, log results
        @param trigger_clear: reset counters after triggered report (False=cumulative average since container start, True=current average)
        @param persist: keep a reference to this Accumulator, returned by get_accumulators()
        """
        super(Accumulator,self).__init__(name, logger, level, 'stats')
        self.lock = Lock()
        self.format = '%d values: ' + format + ' min, ' + format + ' avg, ' + format + ' max, ' + format + ' dev'
        self.keys_arg = keys
        self.trigger_key = trigger_key
        self.trigger_frequency = trigger_frequency
        self.trigger_clear = trigger_clear
        self.clear()
        if persist:
            global _persisted_accumulators
            _persisted_accumulators[self.name] = self

    def keys(self):
        if self.keys_arg == 'all':
            return self.count.keys()
        elif self.keys_arg == 'total':
            return ['__total__']
        elif self.keys_arg == '!total':
            out = self.count.keys()
            out.remove('__total__')
            return out
        else:
            return self.keys_arg

    def clear(self):
        with self.lock:
            self.count = { '__total__': 0 }
            self.min = {}
            self.sum = {}
            self.sumsquares = {}
            self.min = {}
            self.max = {}

    def add(self, timer):
        new_values = []
        for pair_o_tuples in zip(timer.times[:-1], timer.times[1:], xrange(len(timer.times)-1)):
            label = pair_o_tuples[1][0] or str(pair_o_tuples[2])
            delta = pair_o_tuples[1][1]-pair_o_tuples[0][1]
            new_values.append((label,delta))
        with self.lock:
            for label,delta in new_values:
                self.add_value(label, delta, _can_trigger=False, _have_lock=True)
            self._check_trigger()

    def add_value(self, label, value, _can_trigger=True, _have_lock=False):
        """
        @param _can_trigger: when called from add(Timer), avoid trigger until whole Timer is added
        @param _have_lock: when called from add(Timer), don't need to re-acquire the lock
        """
        if not _have_lock:
            with self.lock:
                self.add_value(label, value, _can_trigger=_can_trigger, _have_lock=True)
        else:
            if label in self.sum:
                self.count[label]+=1
                self.sum[label] += value
                self.sumsquares[label] += value*value
                self.min[label] = min(value, self.min[label])
                self.max[label] = max(value, self.max[label])
            else:
                self.count[label]=1
                self.sumsquares[label] = value*value
                self.min[label] = self.max[label] = self.sum[label] = value
            if _can_trigger:
                self._check_trigger()

    def _check_trigger(self):
        """ always called while holding lock """
        if self.trigger_key and self.count[self.trigger_key] and self.count[self.trigger_key]%self.trigger_frequency==0:
            self.log()
            if self.trigger_clear:
                self.clear()

    def get_count(self, key='__total__'):
        return self.count[key] if key in self.count else 0

    def get_min(self, key='__total__'):
        return self.min[key] if key in self.min else float('nan')
    def get_max(self, key='__total__'):
        return self.max[key] if key in self.max else float('nan')

    def get_average(self, key='__total__'):
        return self.sum[key] / self.count[key]

    def get_standard_deviation(self, key='__total__'):
        if self.count[key]<2:
            return float('nan')
        avg = self.get_average(key=key)
        return math.sqrt(self.sumsquares[key]/self.count[key]-avg*avg)

    def __len__(self):
        return len(self.keys())

    def __str__(self):
        return '\n'.join(['%s: %s' % (key,self.to_string(key)) for key in self.keys()])

    def to_string(self, key='__total__'):
        count = self.get_count(key)
        if count:
            return self.format % ( count, self.get_min(key), self.get_average(key),
                                    self.get_max(key), self.get_standard_deviation(key))
        else:
            return 'no values reported'

    def log(self):
        self._log()