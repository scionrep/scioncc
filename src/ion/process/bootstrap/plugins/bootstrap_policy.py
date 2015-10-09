#!/usr/bin/env python

"""Bootstrap process for system policy"""

__author__ = 'Stephen Henrie, Michael Meisinger'

from pyon.public import log
from ion.core.bootstrap_process import BootstrapPlugin
from ion.process.bootstrap.load_system_policy import LoadSystemPolicy


class BootstrapPolicy(BootstrapPlugin):
    """
    Bootstrap plugin for system policy
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        if config.get_safe("system.load_policy", False):
            LoadSystemPolicy.op_load_system_policies(process)
