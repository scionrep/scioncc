"""
testing utilities to check that modules can be imported

these are not tests directly -- see implementations in coi-services/core/test/test_project_imports.py
and pyon/core/test/test_project_imports.py

imports cannot be safely un-imported, so if you run test_can_import below, you cannot run test_can_import_from_any_dir in the same go.
so to use, run one method per command.

examples from pyon:
    bin/nosetests pyon/core/test/test_project_imports.py:TestProjectImports.test_can_import
    bin/nosetests pyon/core/test/test_project_imports.py:TestProjectImports.test_can_import_from_any_dir

examples from coi-services:
    bin/nosetests ion/core/test/test_project_imports.py:TestProjectImports.test_can_import
    bin/nosetests ion/core/test/test_project_imports.py:TestProjectImports.test_can_import_from_any_dir
"""

import unittest
import os

class ImportTest(unittest.TestCase):
    """
    unit test that attempts to import every python file beneath the base directory given.
    fail if any can not be imported
    """
    def __init__(self, source_directory, base_package,*a,**b):
        """
        @param source_directory: should be something in the PYTHONPATH
        @param base_package: top-level package to start recursive search in (or list of them)
        @param a, b: pass-through from unittest.main() to TestCase.__init__()
        """
        super(ImportTest,self).__init__(*a,**b)
        self.source_directory = source_directory
        self.base_package = base_package
        print 'source dir: %s\npkgs: %s' % (source_directory, base_package)
    def test_can_import(self):
        failures = []

        packages = self.base_package if isinstance(self.base_package,list) else [ self.base_package ]
        for pkg in packages:
            pkg_dir = pkg.replace('.','/')
            self._import_below(pkg_dir, pkg, failures)
        if failures:
            self.fail(msg='failed to import these modules:\n' + '\n'.join(failures))

    def test_can_import_from_any_dir(self):
        original_dir = os.getcwd()
        try:
            os.chdir('/tmp')
            self.test_can_import()
        finally:
            os.chdir(original_dir)

    def _import_below(self, dir, mod, failures):
        subdir = os.path.join(self.source_directory, dir)
        for entry in os.listdir(subdir):
            path = os.path.join(subdir, entry)
            try:
                # each python script (except init), try to import
                if os.path.isfile(path) and entry.endswith('.py') and entry!='__init__.py':
                    submod = mod + '.' + entry[:-3]
                    __import__(submod)
                    del submod
                # each new subpackage import and recurse
                elif os.path.isdir(path) and os.path.isfile(os.path.join(path,'__init__.py')) and entry!='test':
                    submod = mod + '.' + entry
                    __import__(submod)
                    self._import_below(os.path.join(dir,entry), submod, failures)
            except Exception,e:
                failures.append(submod)

