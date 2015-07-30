#!/usr/bin/env python

"""Base class for system bootstrap processes"""

__author__ = 'Michael Meisinger'

from pyon.public import log, iex, ImmediateProcess, RT


class BootstrapPlugin(object):
    """
    Base class defining the interface for a bootstrap plugin
    """
    def on_initial_bootstrap(self, process, config, **kwargs):
        """
        Perform system initializations on a first start of the system
        @param process  The bootstrap process
        @param config  The config DotDict that contains plugin specific config
        @retval  bool; if False, abort bootstrap and terminate launch
        """
        pass

    def on_restart(self, process, config, **kwargs):
        """
        Perform system initializations and cleanup on a system restart
        @param process  The bootstrap process
        @param config  The config DotDict that contains plugin specific config
        @retval  bool; if False, abort bootstrap and terminate launch
        """

class AbortBootstrap(Exception):
    pass

class BootstrapProcess(ImmediateProcess, BootstrapPlugin):
    """
    Base class for system bootstrap processes
    """
    process_type = "immediate"

    def on_start(self):
        bootmode = self.CFG.get_safe("bootmode", "initial")

        if bootmode == "initial":
            self.on_initial_bootstrap(self, self.CFG)
        elif bootmode == "restart":
            self.on_restart(self, self.CFG)
        elif bootmode == "secondary":
            # Basically ignore bootstrap actions if we are a non-primary system instance
            pass
        else:
            raise Exception("Unknown bootmode: %s", bootmode)
