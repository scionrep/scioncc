""" manage logging configuration

    USAGE:

    ### perform once as the application begins to initialize logging system-wide
    from ooi.logging import config
    config.add_configuration("some/path/logging.yml")           # could be normal file
    config.add_configuration("or/resource/logging.local.yml")   # or resource within egg
    # define special fields for GELF records
    config.set_logging_fields( {"username":"user", "conversation-id":"session"}, {"system":"alpha"} )

    ### now throughout codebase, can write log records
    from ooi.logging import log
    log.info("up and running now")

    ### but can also go back and change configuration later
    def oh_please_make_it_stop():
        config.set_level("pyon.net.endpoint", logging.ERROR)
        config.set_level("pyon.ion.endpoint", logging.ERROR)
"""

from logging import NOTSET
import logging.config
import errno
import yaml
import collections
from pkg_resources import resource_string
import ooi.logging
import logger
import sys
import traceback

class _LoggingConfiguration(object):

    def __init__(self):
        self.current_config = {}
        self.debug = False

    #### can't use normal logging to debug the logging!  flimsy method to use STDOUT...

    def set_debug(self, value=True):
        """ log all calls to public methods:
                True: enabled
                False: disabled
                "verbose": also log the stack to see where the call is coming from
        """
        self.debug = value

    def _debug(self, message):
        """ conditionally log the message and call stack """
        if self.debug:
            self._log(message)
        if self.debug=='verbose':
            try:
                self._log('Stack trace:\n' + '\t\n'.join(['%s:%d %s'%(f,l,c) for f,l,m,c in traceback.extract_stack()]))
            except:
                self._log('Failed to get stack information')

    def _log(self, message):
        """ print a message to STDOUT """
        print >> sys.stderr, message

    def add_configuration(self, configuration, initial=False):
        self._debug('DEBUG LOGGING: add_configuration: %r' % configuration)

        if not configuration:
            return # no config = no-op
        if isinstance(configuration, dict):
            self._add_dictionary_configuration(configuration, initial)
        elif isinstance(configuration, str):
            # is a configuration file or resource -- try both
            contents = self._read_file(configuration) or self._read_resource(configuration)
            if not contents:
                raise IOError('failed to locate logging configuration: ' + configuration)
            parsed = yaml.load(contents)
            self.add_configuration(parsed, initial)
        elif isinstance(configuration, list) or isinstance(configuration, tuple):
            for item in configuration:
                self.add_configuration(item, initial)
        else:
            raise Exception("ERROR: unable to configure logging from a %s: %s" % (configuration.__class__.__name__, repr(configuration)))

    def _add_dictionary_configuration(self, configuration, initial):
        if not initial:
            self._warn_about_supplemental_handlers(configuration)
        if 'context' in configuration:
            if 'context' in self.current_config:
                self._log("WARNING: logging context filters are additive")
            self._handle_context_entries(configuration)
        if 'disable_existing_loggers' not in configuration:
            self.current_config['disable_existing_loggers'] = False
        self._add_dictionary(self.current_config, configuration)
        logging.config.dictConfig(self.current_config)
        self._debug('DEBUG LOGGING: configuration: %r' % self.current_config)

    def _warn_about_supplemental_handlers(self, configuration):
        if not self.debug:
            return
        do_warn = 'root' in configuration and 'handlers' in configuration['root']
        if not do_warn and 'loggers' in configuration:
            logger_config = configuration['loggers']
            for key in logger_config:
                if 'handlers' in logger_config:
                    do_warn = True
                    break
        if do_warn:
            self._log('WARNING: supplemental file contains handlers (usually supplemental logging config files should just contain level overrides)')

    def _handle_context_entries(self, configuration):
        attribute_name = configuration['context']['attribute'] if 'attribute' in configuration['context'] else None
        static = configuration['context']['static'] if 'static' in configuration['context'] else {}
        dynamic = configuration['context']['thread-local'] if 'thread-local' in configuration['context'] else {}
        self.set_logging_fields(dynamic, static, attribute_name)

    def replace_configuration(self, configuration):
        self.current_config.clear()
        self.add_configuration(configuration, initial=True)

    def set_level(self, scope, level, recursive=False):
        self._debug('DEBUG LOGGING: set_level: %s: %s' % (scope,level))
        if scope:
            changes = { scope: {'level':level }}
            if recursive:
                first_part = scope + '.'
                if 'loggers' in self.current_config:
                    for name in self.current_config['loggers'].keys():
                        if name.startswith(first_part):
                            changes[name] = NOTSET
            config = { 'loggers': changes }
            self.add_configuration(config)
        else:
            config = { 'root': self.current_config['root'] }
            config['root']['level'] = level
            if recursive:
                config['loggers'] = {}
                if 'loggers' in self.current_config:
                    for name in self.current_config['loggers'].keys():
                        config['loggers'][name] = NOTSET
            self.add_configuration(config)

    def set_all_levels(self, level):
        self._debug('DEBUG LOGGING: set_all_levels: %s' % level)
        changes = {'root':{'level':level}}
        if 'loggers' in self.current_config:
            for scope in self.current_config['loggers'].keys():
                changes[scope] = {'level':'NOTSET'}
        self.add_configuration(changes)

    def get_configuration(self):
        return self.current_config

    def _read_file(self, filename):
        try:
            with open(filename, 'r') as infile:
                return infile.read()
        except IOError, e:
            if e.errno != errno.ENOENT:
                self._log('ERROR: error reading logging configuration file %r: %s' % (filename, e))
        return None

    def _read_resource(self, resource_name):
        try:
            return resource_string('', resource_name)
        except IOError, e:
            if e.errno != errno.ENOENT:
                self._log('ERROR: error reading logging configuration file %r: %s' % (resource_name, e))
        return None

    def _add_dictionary(self, current, added):
        """ from pyon.core.common, except allow recursion (logging config isn't too deep) """
        if added:
            for key in added:
                if key in current and isinstance(current[key], collections.Mapping):
                    self._add_dictionary(current[key], added[key])
                else:
                    current[key] = added[key]

    def add_filter(self, filter):
        """ add a filter to all new loggers created """
        ooi.logging.log._add_filter(filter)

    def set_logging_fields(self, thread_local_fields, constant_fields, attribute_name):
        """WARNING: calling multiple times is currently additive -- will not replace fields"""
        filter = logger.AddFields(attribute_name, thread_local_fields, constant_fields)
        self.add_filter(filter)
