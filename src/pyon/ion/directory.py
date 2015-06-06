#!/usr/bin/env python

"""Directory for system config and registrations in a directory tree"""

__author__ = 'Thomas R. Lennan, Michael Meisinger'

from pyon.core import bootstrap
from pyon.core.bootstrap import CFG
from pyon.core.exception import Inconsistent, BadRequest, NotFound, Conflict
from pyon.container.cc import CCAP
from pyon.datastore.datastore import DataStore
from pyon.ion.event import EventPublisher, EventSubscriber
from pyon.ion.identifier import create_unique_directory_id
from pyon.util.log import log
from pyon.util.containers import get_ion_ts

from interface.objects import DirEntry, DirectoryModificationType


class Directory(object):
    """
    Frontend to a directory functionality backed by a datastore.
    Terms:
      directory: instance of a Directory, representing entries within one Org. A tree of entries.
      path: parent+key (= qualified name of an entry). All paths start with '/'
      entry: node in the directory tree with a name (key) and parent path, holding arbitrary attributes
      key: local name of an entry
    """

    def __init__(self, orgname=None, datastore_manager=None, events_enabled=False, container=None):
        self.container = container or bootstrap.container_instance
        # Get an instance of datastore configured as directory.
        datastore_manager = datastore_manager or self.container.datastore_manager
        self.dir_store = datastore_manager.get_datastore(DataStore.DS_DIRECTORY, DataStore.DS_PROFILE.DIRECTORY)

        self.orgname = orgname or CFG.system.root_org
        self.is_root = (self.orgname == CFG.system.root_org)

        self.events_enabled = events_enabled
        self.event_pub = None
        self.event_sub = None

    def start(self):
        # Create directory root entry (for current org) if not existing
        if CFG.system.auto_bootstrap:
            root_de = self.register("/", "DIR", sys_name=bootstrap.get_sys_name())
            if root_de is None:
                # We created this directory just now
                pass

        if self.events_enabled:
            # init change event publisher
            self.event_pub = EventPublisher()

            # Register to receive directory changes
            # self.event_sub = EventSubscriber(event_type="ContainerConfigModifiedEvent",
            #                                  origin="Directory",
            #                                  callback=self.receive_directory_change_event)

        # Create directory root entry (for current org) if not existing
        if CFG.system.auto_bootstrap:
            self.register("/", "DIR", sys_name=bootstrap.get_sys_name())

    def stop(self):
        self.close()

    def close(self):
        """
        Close directory and all resources including datastore and event listener.
        """
        if self.event_sub:
            self.event_sub.deactivate()
        self.dir_store.close()

    # -------------------------------------------------------------------------
    # Directory register, lookup and find

    def lookup(self, parent, key=None, return_entry=False):
        """
        Read directory entry by key and parent node.
        @param return_entry  If True, returns DirEntry object if found, otherwise DirEntry attributes dict
        @retval Either current DirEntry attributes dict or DirEntry object or None if not found.
        """
        path = self._get_path(parent, key) if key else parent
        direntry = self._read_by_path(path)
        if return_entry:
            return direntry
        else:
            return direntry.attributes if direntry else None

    def lookup_mult(self, parent, keys=None, return_entry=False):
        """
        Read several directory entries by keys from the same parent node.
        @param return_entry  If True, returns DirEntry object if found, otherwise DirEntry attributes dict
        @retval Either list of current DirEntry attributes dict or DirEntry object or None if not found.
        """
        direntry_list = self._read_by_path(parent, mult_keys=keys)
        if return_entry:
            return direntry_list
        else:
            return [direntry.attributes if direntry else None for direntry in direntry_list]

    def register(self, parent, key, create_only=False, return_entry=False, ensure_parents=True, **kwargs):
        """
        Add/replace an entry within directory, below a parent node or "/" root.
        Note: Replaces (not merges) the attribute values of the entry if existing. No optimistic locking exists.
        register will back off when a concurrent write was detected, meaning that the other writer wins.
        @param create_only  If True, does not change an existing entry
        @param return_entry  If True, returns DirEntry object of prior entry, otherwise DirEntry attributes dict
        @param ensure_parents  If True, make sure that parent nodes exist
        @retval  DirEntry if previously existing
        """
        if not (parent and key):
            raise BadRequest("Illegal arguments")
        if not type(parent) is str or not parent.startswith("/"):
            raise BadRequest("Illegal arguments: parent")

        dn = self._get_path(parent, key)
        log.debug("Directory.register(%s): %s", dn, kwargs)

        entry_old = None
        cur_time = get_ion_ts()
        # Must read existing entry by path to make sure to not create path twice
        direntry = self._read_by_path(dn)
        if direntry and create_only:
            # We only wanted to make sure entry exists. Do not change
            # NOTE: It is ambiguous to the caller whether we ran into this situation. Seems OK.
            return direntry if return_entry else direntry.attributes
        elif direntry:
            old_rev, old_ts, old_attr = direntry._rev, direntry.ts_updated, direntry.attributes
            direntry.attributes = kwargs
            direntry.ts_updated = cur_time
            try:
                self.dir_store.update(direntry)

                if self.events_enabled and self.container.has_capability(CCAP.EXCHANGE_MANAGER):
                    self.event_pub.publish_event(event_type="DirectoryModifiedEvent",
                                                 origin=self.orgname + ".DIR", origin_type="DIR",
                                                 key=key, parent=parent, org=self.orgname,
                                                 sub_type="REGISTER." + parent[1:].replace("/", "."),
                                                 mod_type=DirectoryModificationType.UPDATE)
            except Conflict:
                # Concurrent update - we accept that we finished the race second and give up
                log.warn("Concurrent update to %s detected. We lose: %s", dn, kwargs)

            if return_entry:
                # Reset object back to prior state
                direntry.attributes = old_attr
                direntry.ts_updated = old_ts
                direntry._rev = old_rev
                entry_old = direntry
            else:
                entry_old = old_attr
        else:
            direntry = self._create_dir_entry(parent, key, attributes=kwargs, ts=cur_time)
            if ensure_parents:
                self._ensure_parents_exist([direntry])
            try:
                self.dir_store.create(direntry, create_unique_directory_id())
                if self.events_enabled and self.container.has_capability(CCAP.EXCHANGE_MANAGER):
                    self.event_pub.publish_event(event_type="DirectoryModifiedEvent",
                                                 origin=self.orgname + ".DIR", origin_type="DIR",
                                                 key=key, parent=parent, org=self.orgname,
                                                 sub_type="REGISTER." + parent[1:].replace("/", "."),
                                                 mod_type=DirectoryModificationType.CREATE)
            except BadRequest as ex:
                if not ex.message.endswith("already exists"):
                    raise
                # Concurrent create - we accept that we finished the race second and give up
                log.warn("Concurrent create of %s detected. We lose: %s", dn, kwargs)

        return entry_old

    def register_safe(self, parent, key, **kwargs):
        """
        Use this method to protect caller from any form of directory register error
        """
        try:
            return self.register(parent, key, **kwargs)
        except Exception as ex:
            log.exception("Error registering path=%s/%s, args=%s", parent, key, kwargs)

    def register_mult(self, entries):
        """
        Registers multiple directory entries efficiently in one datastore access.
        Note: this fails of entries are currently existing, so works for create only.
        """
        if type(entries) not in (list, tuple):
            raise BadRequest("Bad entries type")
        de_list = []
        cur_time = get_ion_ts()
        for parent, key, attrs in entries:
            direntry = self._create_dir_entry(parent, key, attributes=attrs, ts=cur_time)
            de_list.append(direntry)
        pe_list = self._ensure_parents_exist(de_list, create=False)
        de_list.extend(pe_list)
        deid_list = [create_unique_directory_id() for i in xrange(len(de_list))]
        self.dir_store.create_mult(de_list, deid_list)

        if self.events_enabled and self.container.has_capability(CCAP.EXCHANGE_MANAGER):
            for de in de_list:
                self.event_pub.publish_event(event_type="DirectoryModifiedEvent",
                                             origin=self.orgname + ".DIR", origin_type="DIR",
                                             key=de.key, parent=de.parent, org=self.orgname,
                                             sub_type="REGISTER." + de.parent[1:].replace("/", "."),
                                             mod_type=DirectoryModificationType.CREATE)

    def unregister(self, parent, key=None, return_entry=False):
        """
        Remove entry from directory.
        Returns attributes of deleted DirEntry
        """
        path = self._get_path(parent, key) if key else parent
        log.debug("Removing content at path %s" % path)

        direntry = self._read_by_path(path)
        if direntry:
            self.dir_store.delete(direntry)
            if self.events_enabled and self.container.has_capability(CCAP.EXCHANGE_MANAGER):
                self.event_pub.publish_event(event_type="DirectoryModifiedEvent",
                                             origin=self.orgname + ".DIR", origin_type="DIR",
                                             key=key, parent=parent, org=self.orgname,
                                             sub_type="UNREGISTER." + parent[1:].replace("/", "."),
                                             mod_type=DirectoryModificationType.DELETE)

        if direntry and not return_entry:
            return direntry.attributes
        else:
            return direntry

    def unregister_safe(self, parent, key):
        try:
            return self.unregister(parent, key)
        except Exception as ex:
            log.exception("Error unregistering path=%s/%s", parent, key)

    def find_child_entries(self, parent='/', direct_only=True, **kwargs):
        """
        Return all child entries (ordered by path) for the given parent path.
        Does not return the parent itself. Optionally returns child of child entries.
        Additional kwargs are applied to constrain the search results (limit, descending, skip).
        @param parent  Path to parent (must start with "/")
        @param direct_only  If False, includes child of child entries
        @retval  A list of DirEntry objects for the matches
        """
        if not type(parent) is str or not parent.startswith("/"):
            raise BadRequest("Illegal argument parent: %s" % parent)
        if direct_only:
            start_key = [self.orgname, parent, 0]
            end_key = [self.orgname, parent]
            res = self.dir_store.find_by_view('directory', 'by_parent',
                start_key=start_key, end_key=end_key, id_only=True, convert_doc=True, **kwargs)
        else:
            path = parent[1:].split("/")
            start_key = [self.orgname, path, 0]
            end_key = [self.orgname, list(path) + ["ZZZZZZ"]]
            res = self.dir_store.find_by_view('directory', 'by_path',
                start_key=start_key, end_key=end_key, id_only=True, convert_doc=True, **kwargs)

        match = [value for docid, indexkey, value in res]
        return match

    def find_by_key(self, key=None, parent='/', **kwargs):
        """
        Returns a list of DirEntry for each directory entry that matches the given key name.
        If a parent is provided, only checks in this parent and all subtree.
        These entries are in the same org's directory but have different parents.
        """
        if key is None:
            raise BadRequest("Illegal arguments")
        if parent is None:
            raise BadRequest("Illegal arguments")
        start_key = [self.orgname, key, parent]
        end_key = [self.orgname, key, parent + "ZZZZZZ"]
        res = self.dir_store.find_by_view('directory', 'by_key',
            start_key=start_key, end_key=end_key, id_only=True, convert_doc=True, **kwargs)

        match = [value for docid, indexkey, value in res]
        return match

    def find_by_value(self, subtree='/', attribute=None, value=None, **kwargs):
        """
        Returns a list of DirEntry with entries that have an attribute with the given value.
        """
        if attribute is None:
            raise BadRequest("Illegal arguments")
        if subtree is None:
            raise BadRequest("Illegal arguments")
        start_key = [self.orgname, attribute, value, subtree]
        end_key = [self.orgname, attribute, value, subtree + "ZZZZZZ"]
        res = self.dir_store.find_by_view('directory', 'by_attribute',
                        start_key=start_key, end_key=end_key, id_only=True, convert_doc=True, **kwargs)

        match = [value for docid, indexkey, value in res]
        return match

    def remove_child_entries(self, parent, delete_parent=False):
        pass

    # -------------------------------------------------------------------------
    #  Concurrency Control

    def acquire_lock(self, key, lock_info=None):
        """
        Attempts to atomically acquire a lock with the given key and namespace.
        """
        if not key:
            raise BadRequest("Illegal arguments")
        if "/" in key:
            raise BadRequest("Illegal arguments: key")

        direntry = self._create_dir_entry("/System/Locks", key, attributes=lock_info)
        lock_result = True
        try:
            # This is an atomic operation. It relies on the unique key constraint of the directory service
            self.dir_store.create(direntry, create_unique_directory_id())
        except BadRequest as ex:
            if ex.message.endswith("already exists"):
                lock_result = False
            else:
                raise

        log.debug("Directory.acquire_lock(%s): %s -> %s", key, lock_info, lock_result)

        return lock_result

    def release_lock(self, key):
        """
        Releases lock identified by key.
        Raises NotFound if lock does not exist.
        """
        if not key:
            raise BadRequest("Illegal arguments")
        if "/" in key:
            raise BadRequest("Illegal arguments: key")

        log.debug("Directory.release_lock(%s)", key)

        dir_entry = self.lookup("/System/Locks", key, return_entry=True)
        if dir_entry:
            self.dir_store.delete(dir_entry._id)
        else:
            raise NotFound("Lock %s not found" % key)

    # -------------------------------------------------------------------------
    # Internal functions

    def receive_directory_change_event(self, event_msg, headers):
        # @TODO add support to fold updated config into container config
        pass


    def _get_path(self, parent, key):
        """
        Returns the qualified directory path for a directory entry.
        """
        if parent == "/":
            return parent + key
        elif parent.startswith("/"):
            return parent + "/" + key
        else:
            raise BadRequest("Illegal parent: %s" % parent)

    def _get_key(self, path):
        """
        Returns the key from a qualified directory path
        """
        parent, key = path.rsplit("/", 1)
        return key

    def _create_dir_entry(self, parent, key, orgname=None, ts=None, attributes=None):
        """
        Standard way to create a DirEntry object.
        """
        orgname = orgname or self.orgname
        ts = ts or get_ion_ts()
        attributes = attributes if attributes is not None else {}
        parent = parent or "/"
        de = DirEntry(org=orgname, parent=parent, key=key, attributes=attributes, ts_created=ts, ts_updated=ts)
        return de

    def _read_by_path(self, path, orgname=None, mult_keys=None):
        """
        Given a qualified path, find entry in directory and return DirEntry object or None if not found.
        """
        if path is None:
            raise BadRequest("Illegal arguments")
        orgname = orgname or self.orgname
        if mult_keys:
            parent = path or "/"
            key = mult_keys
        else:
            parent, key = path.rsplit("/", 1)
            parent = parent or "/"
        find_key = [orgname, key, parent]
        view_res = self.dir_store.find_by_view('directory', 'by_key', key=find_key, id_only=True, convert_doc=True)

        match = [doc for docid, index, doc in view_res]
        if mult_keys:
            entries_by_key = {doc.key: doc for doc in match}
            entries = [entries_by_key.get(key, None) for key in mult_keys]
            return entries
        else:
            if len(match) > 1:
                log.error("More than one directory entry found for key %s" % path)
                return match[0]
            elif match:
                return match[0]
            return None

    def _get_unique_parents(self, entry_list):
        """Returns a sorted, unique list of parents of DirEntries (excluding the root /)"""
        if entry_list and type(entry_list) not in (list, tuple):
            entry_list = [entry_list]
        parents = set()
        for entry in entry_list:
            parents.add(entry.parent)
        if "/" in parents:
            parents.remove("/")
        return sorted(parents)

    def _ensure_parents_exist(self, entry_list, create=True):
        parents_list = self._get_unique_parents(entry_list)
        pe_list = []
        try:
            for parent in parents_list:
                pe = self.lookup(parent)
                if pe is None:
                    pp, pk = parent.rsplit("/", 1)
                    direntry = self._create_dir_entry(parent=pp, key=pk)
                    pe_list.append(direntry)
                    if create:
                        try:
                            self.dir_store.create(direntry, create_unique_directory_id())
                        except BadRequest as ex:
                            if not ex.message.endswith("already exists"):
                                raise
                            # Else: Concurrent create
        except Exception as ex:
            log.warn("_ensure_parents_exist(): Error creating directory parents", exc_info=True)
        return pe_list

    def _cleanup_outdated_entries(self, dir_entries, common="key"):
        """
        This function takes all DirEntry from the list and removes all but the most recent one
        by ts_updated timestamp. It returns the most recent DirEntry and removes the others by
        direct datastore operations. If there are multiple entries with most recent timestamp, the
        first encountered is kept and the others non-deterministically removed.
        Note: This operation can be called for DirEntries without common keys, e.g. for all
        entries registering an agent for a device.
        """
        if not dir_entries:
            return
        newest_entry = dir_entries[0]
        try:
            for de in dir_entries:
                if int(de.ts_updated) > int(newest_entry.ts_updated):
                    newest_entry = de

            remove_list = [de for de in dir_entries if de is not newest_entry]

            log.info("Attempting to cleanup these directory entries: %s" % remove_list)
            for de in remove_list:
                try:
                    self.dir_store.delete(de)
                except Exception as ex:
                    log.warn("Removal of outdated %s directory entry failed: %s" % (common, de))
            log.info("Cleanup of %s old %s directory entries succeeded" % (len(remove_list), common))

        except Exception as ex:
            log.warn("Cleanup of multiple directory entries for %s failed: %s" % (
                common, str(ex)))

        return newest_entry
