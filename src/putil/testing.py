"""
Testing utilities to check that modules can be imported

imports cannot be safely un-imported, so if you run test_can_import below,
you cannot run test_can_import_from_any_dir in the same go.
so to use, run one method per command.
"""

import unittest
import os


class UtilTest(unittest.TestCase):
    # override __str__ and __repr__ behavior to show a copy-pastable nosetest name for tests
    #  putil.module:TestClassName.test_function_name
    def __repr__(self):
        name = self.id()
        name = name.split('.')
        if name[0] not in ["putil"]:
            return "%s (%s)" % (name[-1], '.'.join(name[:-1]))
        else:
            return "%s ( %s )" % (name[-1], '.'.join(name[:-2]) + ":" + '.'.join(name[-2:]))

    __str__ = __repr__


class ImportTest(UtilTest):
    """
    Base class for unit test that attempts to import every python file beneath
    the base directory given. Fail if any can not be imported
    Add setUp method to child class setting base_package and source_directory.
    """

    def test_can_import(self):
        if not hasattr(self, "source_directory") or not hasattr(self, "base_package"):
            raise unittest.SkipTest("Don't execute base class")

        failures = []

        packages = self.base_package if isinstance(self.base_package, list) else [self.base_package]
        for pkg in packages:
            pkg_dir = pkg.replace('.','/')
            self._import_below(pkg_dir, pkg, failures)
        if failures:
            self.fail(msg='failed to import these modules:\n' + '\n'.join(failures))

    def test_can_import_from_any_dir(self):
        if not hasattr(self, "source_directory") or not hasattr(self, "base_package"):
            raise unittest.SkipTest("Don't execute base class")

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
            except Exception as ex:
                failures.append(submod)
