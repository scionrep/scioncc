"""
test that all modules in pyon have valid syntax and can be imported successfully
"""

import os
from unittest import TestCase,main
import putil.testing
import __main__
from nose.plugins.attrib import attr

MODULES = ['ion', 'pyon', 'putil' ]

@attr('UNIT')
class TestProjectImports(putil.testing.ImportTest):

    def setUp(self):
        source_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        self.source_directory = source_dir
        self.base_package = MODULES
