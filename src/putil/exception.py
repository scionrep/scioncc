"""
Base exception class for application-defined exceptions to keep the stack explicitly
so it can be caught, re-raised and inspected from a different context.

Some of this is similar to PEP3134 and implemented in python3
"""

import traceback
import sys


class ApplicationException(Exception):
    def __init__(self, *args, **kwargs):
        super(ApplicationException, self).__init__(*args)
        self._stacks = []
        self._cause, self._stack_cause = None, None

        # Save current stack
        self._stack_init = traceback.extract_stack()
        del self._stack_init[-2:]  # Remove the __init__ and extract_strack frames
        self.add_stack(self.__class__.__name__ + ': ' + str(self), self._stack_init)

        # Add stacks and labels for cause
        if 'cause' in kwargs and kwargs['cause']:
            self._cause = kwargs['cause']

            # WARNING - this may show exception even outside of an except handler
            # See http://stackoverflow.com/questions/16974489
            cause_type, cause_exc, cause_tb = sys.exc_info()  #if retain_cause else (None, None, None)

            if not isinstance(kwargs['cause'], Exception):
                # cause is not an exception - treat as boolean, use exc_info
                self._cause = cause_exc

            cause_label = 'caused by: ' + self._cause.__class__.__name__ + ': ' + str(self._cause)

            if isinstance(self._cause, ApplicationException) and self._cause._stacks:
                # If ApplicationException, copy stacks
                for i, (label, stack) in enumerate(self._cause._stacks):
                    if i == 0:
                        self.add_stack(cause_label, stack)
                    else:
                        self.add_stack(label, stack)
                self._stack_cause = self._cause._stack_init

            elif self._cause == cause_exc:
                # If cause is current exception in exc_info, use its stack
                self._stack_cause = traceback.extract_tb(cause_tb)
                self.add_stack(cause_label, self._stack_cause)

            else:
                # We got an exception but it's not current and there is no stack
                pass

    def drop_chained_init_frame(self):
        """Fixes current exception stack.
        Ideally, get_stack() should return the stack from thread start down to where
        the exception is created. But actual stack traces may include stack frames
        where a subclass __init__ method chains to its superclass __init__.
        To avoid exposing the internals of the exception classes and instead maintain
        a stack useful for application debugging, __init__ in each subclass (direct or indirect)
        should invoke this method to remove one (more) frame from the saved stack.
        """
        self._stack_init = self._stack_init[0:-2]

    def get_stack(self):
        """Return current exception stack trace"""
        return self._stack_init

    def get_cause(self):
        """Return an original cause exception, or None"""
        return self._cause

    def get_cause_stack(self):
        """Return the stack of an original cause exception, or None"""
        return self._stack_cause

    def add_stack(self, label, stack):
        """Adds a formatted stack to the current exception"""
        self._stacks.append((label, stack))

    def get_stacks(self):
        """Return a list of causing stacks or empty list"""
        return self._stacks

    @staticmethod
    def format_stack(stack, short=False, path=False, align=True):
        def extract_mod(filename):
            if path:
                return filename
            parts = filename.rsplit("src/", 1)
            if len(parts) == 2:
                return parts[1]
            parts = filename.rsplit(".egg/", 1)
            if len(parts) == 2:
                return parts[1]
            parts = filename.rsplit("lib/", 1)
            if len(parts) == 2:
                return parts[1]
            return filename.rsplit("/", 1)[-1]

        if not type(stack) in (list, tuple):
            return

        if stack and type(stack[0]) in (list, tuple) and len(stack[0]) == 2 and type(stack[0][1] in (list, tuple)):
            # It's a list of label, stack
            return "\n".join("%s\n%s" % (label, ApplicationException.format_stack(stk, short=short, path=path, align=align)) for label, stk in stack)

        if short and align:
            locs = ["%s:%s:%s" % (extract_mod(s[0]), s[1], s[2]) for s in stack]
            modalign = max(len(l) for l in locs) if stack else 0
            pattern = '  %-' + str(modalign) + 's  %s'
            return "\n".join(pattern % (l, s[3]) for (l, s) in zip(locs, stack))
        elif short:
            return "\n".join('  %s:%s:%s  %s' % (extract_mod(s[0]), s[1], s[2], s[3]) for s in stack)
        else:
            return "\n".join('  File "%s", line %s, in %s\n    %s' % (s[0], s[1], s[2], s[3]) for s in stack)
