""" Reliably determining a leader among equal distributed processes """

__author__ = 'Michael Meisinger'

import gevent
from gevent.event import Event

from pyon.public import BadRequest, log, get_ion_ts_millis


class LeaderManager(object):
    """ This class spawns a background thread that acquires a leader lock for a given
    scope, so that concurrent peer processes can determine a leader among them.
    The mechanism guarantees that there is never more than 1 leader.
    It is based on an atomic, central directory (database) lock with timeout.
    It does not guarantee that there is always an active leader, e.g. when a leader
    suddenly fails. It does, however, have the leader lock expire after a while,
    so that a surviving peer can claim the leader role.
    """

    def __init__(self, scope, process):
        self.scope = scope
        self.process = process
        self.container = process.container
        self.process_id = self.process.id

        self.leader_interval = 60
        self._has_leader = Event()
        self._lock_timeout = self.leader_interval * 1.5

        self._leader_thread = None
        self._leader_quit = None        # Signal to terminate background thread
        self._has_lock = False
        self._lock_expires = 0          # Timestamp in millis when lock expires
        self._leader_callbacks = []     # Callables cb(atts_dict) to be notified when leader status changes

    def start(self):
        self._leader_quit = Event()
        self._leader_thread = self.process._process.thread_manager.spawn(self._leader_loop)

    def stop(self):
        self.release_leader()
        self._leader_quit.set()
        self._leader_thread.join(timeout=2)

    def is_leader(self):
        """ Returns true if current instance is leader and lock did not expire """
        if self._has_lock and get_ion_ts_millis() >= self._lock_expires:
            log.warn("Master lock '%s' held and expired by %s", self.scope, self.process_id)
            self._inform_error("lock_expired")
            try:
                # Cannot call release_leader due to infinite recursion
                self.container.directory.release_lock(self.scope, lock_holder=self.process_id)
                self._inform_release()
            except Exception:
                pass
            self._has_lock = False
            self._lock_expires = 0
            return False

        return self._has_lock

    def release_leader(self):
        if self.is_leader():
            self.container.directory.release_lock(self.scope, lock_holder=self.process_id)
            self._inform_release()

    def add_leader_callback(self, cb):
        self._leader_callbacks.append(cb)

    def await_leader(self):
        self._has_leader.wait()

    # -------------------------------------------------------------------------

    def _leader_loop(self):
        """ Background loop that acquires a leader lock """
        log.info("Starting leader loop '%s' for pid=%s", self.scope, self.process.id)

        self._check_lock()
        self._has_leader.set()
        while not self._leader_quit.wait(timeout=self.leader_interval):
            try:
                self._check_lock()
            except Exception:
                log.exception("Exception in _leader_loop '%s' for pid=%s", self.scope, self.process_id)

    def _check_lock(self):
        cur_time = get_ion_ts_millis()
        was_leader = self.is_leader()
        self._has_lock = self.container.directory.acquire_lock(self.scope, self._lock_timeout, self.process_id)
        if self._has_lock:
            # We are the leader
            if not was_leader:
                log.info("Process %s is now the leader for '%s'", self.process_id, self.scope)
                self._inform_acquire()
            self._lock_expires = cur_time + int(1000*self._lock_timeout)

    def _inform_acquire(self):
        for cb in self._leader_callbacks:
            attrs = dict(scope=self.scope, action="acquire_leader", process_id=self.process_id,
                         expires=self._lock_expires)
            cb(attrs)

    def _inform_release(self):
        for cb in self._leader_callbacks:
            attrs = dict(scope=self.scope, action="release_leader", process_id=self.process_id)
            cb(attrs)

    def _inform_error(self, err_type):
        for cb in self._leader_callbacks:
            attrs = dict(scope=self.scope, action="error", process_id=self.process_id,
                         err_type=err_type)
            cb(attrs)
