
import os
import putil.testing

class TestOOIImports(putil.testing.ImportTest):
    def setUp(self):
        # for utilities project only, want to search in BASE/src
        # but this test is in BASE/test/ooi
        # so have to go up two levels, then down to src

        target_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '')
        self.source_directory = target_dir
        self.base_package = 'putil'

