"""
base exception class for application-defined exceptions to keep the stack explicitly
so it can be caught, re-raised and inspected from a different context

some of this is similar to PEP3134 and implemented in python3
"""

import traceback
import sys

class ApplicationException(Exception):
    def __init__(self, *a, **b):
        super(ApplicationException,self).__init__(*a,**b)
        self._stacks = []

        # save current stack
        self._stack_init = traceback.extract_stack()
        self.add_stack(self.__class__.__name__ + ': ' + str(self), self._stack_init)

        # WARNING this is unreliable!  only use if cause passed as argument
        cause_info = sys.exc_info() #if retain_cause else (None,None,None)

        # add stacks and labels for cause
        if 'cause' in b:
            if isinstance(b['cause'],Application):
                self._cause = b['cause']
                cause_label = 'caused by: ' + self._cause.__class__.__name__ + ': ' + str(self._cause)
                # if ApplicationException, get stacks from its list
                if isinstance(self._cause,ApplicationException) and len(self._cause._stacks):
                    first = True
                    for label,stack in self._cause._stacks:
                        if first:
                            self.add_stack(cause_label, stack)
                            first = False
                        else:
                            self.add_stack(label, stack)
                # otherwise if this is current exception in exc_info, use its stack
                elif self._cause==cause_info[1] and cause_info[2]:
                    self._stack_cause = traceback.extract_tb(cause_info[2])
                    self.add_stack(cause_label, self._stack_cause)
            # cause is not an exception? treat as boolean, use exc_info
            elif b['cause']:
                self._cause=cause_info[1]
                if cause_info[2]:
                    self._stack_cause = traceback.extract_tb(cause_info[2])
                    self.add_stack(cause_label, self._stack_cause)


    def drop_chained_init_frame(self):
        """ ideally, get_stack() should return the stack from thread start down to where the exception is created.
            but actual stack traces may include stack frames where a subclass __init__ method chains to its superclass __init__.
            to avoid exposing the internals of the exception classes and instead maintain a stack useful for application debugging,
            __init__ in each subclass (direct or indirect) should invoke this method to remove one (more) frame from the saved stack.
        """
        self._stack_init = self._stack_init[0:-2]

    def get_stack(self):
        return self._stack_init

    def get_cause(self):
        """ if this exception was created in an except: block, return the original exception """
        return self._cause

    def get_cause_stack(self):
        """ if this exception was created in an except: block, return the stack of the original exception """
        return self._stack_cause

    def add_stack(self, label, stack):
        self._stacks.append((label, stack))

    def get_stacks(self):
        return self._stacks