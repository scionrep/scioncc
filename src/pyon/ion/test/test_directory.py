#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
import gevent

from pyon.util.unit_test import IonUnitTestCase
from pyon.core.bootstrap import CFG
from pyon.core.exception import BadRequest, NotFound
from pyon.datastore.datastore import DatastoreManager
from pyon.ion.directory import Directory

from interface.objects import DirEntry


@attr('UNIT', group='datastore')
class TestDirectory(IonUnitTestCase):

    def test_directory(self):
        dsm = DatastoreManager()
        ds = dsm.get_datastore("resources", "DIRECTORY")
        ds.delete_datastore()
        ds.create_datastore()

        self.patch_cfg('pyon.ion.directory.CFG', {'service': {'directory': {'publish_events': False}}})

        directory = Directory(datastore_manager=dsm)
        directory.start()

        #self.addCleanup(directory.dir_store.delete_datastore)

        objs = directory.dir_store.list_objects()

        root = directory.lookup("/DIR")
        self.assert_(root is not None)

        entry = directory.lookup("/temp")
        self.assert_(entry is None)

        entry_old = directory.register("/", "temp")
        self.assertEquals(entry_old, None)

        # Create a node
        entry = directory.lookup("/temp")
        self.assertEquals(entry, {} )

        # The create case
        entry_old = directory.register("/temp", "entry1", foo="awesome")
        self.assertEquals(entry_old, None)
        entry_new = directory.lookup("/temp/entry1")
        self.assertEquals(entry_new, {"foo":"awesome"})

        # The update case
        entry_old = directory.register("/temp", "entry1", foo="ingenious")
        self.assertEquals(entry_old, {"foo": "awesome"})

        # The delete case
        entry_old = directory.unregister("/temp", "entry1")
        self.assertEquals(entry_old, {"foo": "ingenious"})
        entry_new = directory.lookup("/temp/entry1")
        self.assertEquals(entry_new, None)

        directory.register("/BranchA", "X", resource_id="rid1")
        directory.register("/BranchA", "Y", resource_id="rid2")
        directory.register("/BranchA", "Z", resource_id="rid3")
        directory.register("/BranchA/X", "a", resource_id="rid4")
        directory.register("/BranchA/X", "b", resource_id="rid5")
        directory.register("/BranchB", "k", resource_id="rid6")
        directory.register("/BranchB", "l", resource_id="rid7")
        directory.register("/BranchB/k", "m", resource_id="rid7")
        directory.register("/BranchB/k", "X")

        res_list = directory.find_by_value("/", attribute="resource_id", value="rid3")
        self.assertEquals(len(res_list), 1)
        self.assertEquals(res_list[0].org, "ION")
        self.assertEquals(res_list[0].parent, "/BranchA")
        self.assertEquals(res_list[0].key, "Z")

        res_list = directory.find_by_value("/", attribute="resource_id", value="rid34")
        self.assertEquals(len(res_list), 0)

        res_list = directory.find_by_value("/", attribute="resource_id", value="rid7")
        self.assertEquals(len(res_list), 2)

        res_list = directory.find_by_value("/BranchB", attribute="resource_id", value="rid7")
        self.assertEquals(len(res_list), 2)

        res_list = directory.find_by_value("/Branch", attribute="resource_id", value="rid7")
        self.assertEquals(len(res_list), 2)

        res_list = directory.find_by_value("/BranchB/k", attribute="resource_id", value="rid7")
        self.assertEquals(len(res_list), 1)

        res_list = directory.find_child_entries("/BranchB/k/m")
        self.assertEquals(len(res_list), 0)

        res_list = directory.find_child_entries("/BranchB")
        self.assertEquals(len(res_list), 2)

        res_list = directory.find_child_entries("/BranchB/k/m", direct_only=False)
        self.assertEquals(len(res_list), 0)

        res_list = directory.find_child_entries("/BranchB", direct_only=False)
        self.assertEquals(len(res_list), 4)

        res_list = directory.find_by_key("X")
        self.assertEquals(len(res_list), 2)

        res_list = directory.find_by_key("X", parent="/BranchB")
        self.assertEquals(len(res_list), 1)

        entry_list = directory.lookup_mult("/BranchA", ["X", "Z"])
        self.assertEquals(len(entry_list), 2)
        self.assertEquals(entry_list[0]["resource_id"], "rid1")
        self.assertEquals(entry_list[1]["resource_id"], "rid3")

        entry_list = directory.lookup_mult("/BranchA", ["Y", "FOO"])
        self.assertEquals(len(entry_list), 2)
        self.assertEquals(entry_list[0]["resource_id"], "rid2")
        self.assertEquals(entry_list[1], None)

        # Test prevent duplicate entries
        directory.register("/some", "dupentry", foo="ingenious")
        de = directory.lookup("/some/dupentry", return_entry=True)
        de1_attrs = de.__dict__.copy()
        del de1_attrs["_id"]
        del de1_attrs["_rev"]
        del de1_attrs["type_"]
        de1 = DirEntry(**de1_attrs)
        with self.assertRaises(BadRequest) as ex:
            de_id1,_ = directory.dir_store.create(de1)
            self.assertTrue(ex.message.startswith("DirEntry already exists"))

        res_list = directory.find_by_key("dupentry", parent="/some")
        self.assertEquals(1, len(res_list))

    def test_directory_lock(self):
        dsm = DatastoreManager()
        ds = dsm.get_datastore("resources", "DIRECTORY")
        ds.delete_datastore()
        ds.create_datastore()

        self.patch_cfg('pyon.ion.directory.CFG', {'service': {'directory': {'publish_events': False}}})

        directory = Directory(datastore_manager=dsm)
        directory.start()

        lock1 = directory.acquire_lock("LOCK1", lock_info=dict(process="proc1"))
        self.assertEquals(lock1, True)

        lock2 = directory.acquire_lock("LOCK1", lock_info=dict(process="proc2"))
        self.assertEquals(lock2, False)

        with self.assertRaises(BadRequest):
            directory.acquire_lock("LOCK/SOME")

        with self.assertRaises(BadRequest):
            directory.release_lock("LOCK/SOME")

        with self.assertRaises(NotFound):
            directory.release_lock("LOCK2")

        directory.release_lock("LOCK1")

        lock1 = directory.acquire_lock("LOCK1", lock_info=dict(process="proc3"))
        self.assertEquals(lock1, True)

        # TEST: With lock holders

        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc1")
        self.assertEquals(lock5, True)

        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc1")
        self.assertEquals(lock5, True)

        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc2")
        self.assertEquals(lock5, False)

        directory.release_lock("LOCK5")

        # TEST: Timeout
        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc1", timeout=0.1)
        self.assertEquals(lock5, True)

        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc2")
        self.assertEquals(lock5, False)

        res = directory.is_locked("LOCK5")
        self.assertEquals(res, True)

        gevent.sleep(0.15)

        res = directory.is_locked("LOCK5")
        self.assertEquals(res, False)

        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc2", timeout=0.1)
        self.assertEquals(lock5, True)

        gevent.sleep(0.15)

        # TEST: Holder self renew
        lock5 = directory.acquire_lock("LOCK5", lock_holder="proc2", timeout=0.1)
        self.assertEquals(lock5, True)

        directory.stop()