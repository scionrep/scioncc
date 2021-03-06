#!/usr/bin/env python

"""Classes to build and manage concurrent Pyon worker greenlets, aka threads."""

__author__ = "Adam R. Smith"


import time
import os
import signal
from gevent.event import AsyncResult

from pyon.util.async import Event, spawn
from pyon.util.log import log
from pyon.core.exception import ContainerError


class PyonThreadError(Exception):
    pass


class PyonThreadTraceback(object):
    """
    Sentinel class for extracting a real traceback.
    """
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return self._msg


class PyonHeartbeatError(PyonThreadError):
    pass


class PyonThread(object):
    """
    Thread-like base class for doing work in the container, based on gevent's greenlets.
    """

    def __init__(self, target=None, *args, **kwargs):
        """
        @param target The Callable to start as independent thread
        @param args  Provided as spawn args to thread
        @param kwargs  Provided as spawn kwargs to thread
        """
        super(PyonThread, self).__init__()

        if target is not None or not hasattr(self, 'target'):   # Allow setting target at class level
            self.target = target
        self.spawn_args = args
        self.spawn_kwargs = kwargs

        # The instance of Greenlet or subprocess or similar
        self.proc = None
        self.supervisor = None

        self.ev_exit = Event()   # Event that is set when greenlet exits

    def _pid(self):
        """ And internal, non global thread identifier.
        """
        return id(self.proc)

    def _spawn(self):
        """ Spawn a gevent greenlet using defined target method and args.
        """
        gl = spawn(self.target, *self.spawn_args, **self.spawn_kwargs)
        gl.link(lambda _: self.ev_exit.set())    # Set exit event when we terminate
        gl._glname = "ION Thread %s" % str(self.target)
        return gl

    def _join(self, timeout=None):
        return self.proc.join(timeout)

    def _stop(self):
        return self.proc.kill()

    def _running(self):
        return self.proc.started

    def _notify_stop(self):
        pass

    @property
    def pid(self):
        """ Return the internal process ID for the spawned thread. If not spawned yet, return 0. """
        if self.proc is None:
            return 0
        return self._pid()

    @property
    def running(self):
        """ Is the thread actually running? """
        return bool(self.proc and self._running())

    def start(self):
        self.proc = self._spawn()
        self.proc._glname = ""
        return self

    def notify_stop(self):
        """ Get ready, you're about to get shutdown. """
        self._notify_stop()

    def stop(self):
        if self.running:
            self._stop()

        if self.supervisor is not None:
            self.supervisor.child_stopped(self)

        return self

    def join(self, timeout=None):
        if self.proc is not None and self.running:
            self._join(timeout)
            self.stop()

        return self

    def get(self):
        """
        Returns the value (or raises the exception) of the wrapped thread.

        If not running yet, returns None.
        """
        if self.proc is not None:
            return self.proc.get()

        return None

    def get_ready_event(self):
        """
        By default, it is always ready.

        Override this in your specific process.
        """
        ev = Event()
        ev.set()
        return ev


class ThreadManager(object):
    """
    Manage spawning greenlet threads and ensure they're alive.
    TODO: Add heartbeats with zeromq for monitoring and restarting.
    """

    def __init__(self, heartbeat_secs=10.0, failure_notify_callback=None):
        """
        Creates a ThreadManager.

        @param  heartbeat_secs              Seconds between heartbeats.
        @param  failure_notify_callback     Callback to execute when a child fails unexpectedly. Should be
                                            a callable taking two params: this process supervisor, and the
                                            thread that failed.
        """
        super(ThreadManager, self).__init__()

        # NOTE: Assumes that pids never overlap between the various process types
        self.children = []
        self.heartbeat_secs = heartbeat_secs
        self._shutting_down = False
        self._failure_notify_callback = failure_notify_callback
        self._shutdown_event = AsyncResult()

    def _create_thread(self, target=None, **kwargs):
        """
        Creates a "thread" of the proper type.
        """
        return PyonThread(target=target, **kwargs)

    def spawn(self, target=None, **kwargs):
        """
        Spawn a pyon thread

        """
        log.debug("ThreadManager.spawn, target=%s, kwargs=%s", target, kwargs)
        proc = self._create_thread(target=target, **kwargs)
        proc.supervisor = self

        proc.start()
        self.children.append(proc)

        # install failure monitor
        proc.proc.link_exception(self._child_failed)

        return proc

    def _child_failed(self, gproc):
        # extract any PyonThreadTracebacks - one should be last
        extra = ""
        if len(gproc.exception.args) and isinstance(gproc.exception.args[-1], PyonThreadTraceback):
            extra = "\n" + str(gproc.exception.args[-1])

        log.error("Child failed with an exception: (%s) %s%s", gproc, gproc.exception, extra)
        if self._failure_notify_callback:
            self._failure_notify_callback(gproc)

    def ensure_ready(self, proc, errmsg=None, timeout=20):
        """
        Waits until either the thread dies or reports it is ready, whichever comes first.

        If the thread dies or times out while waiting for it to be ready, a ContainerError is raised.
        You must be sure the thread implements get_ready_event properly, otherwise this method
        returns immediately as the base class behavior simply passes.

        @param  proc        The thread to wait on.
        @param  errmsg      A custom error message to put in the ContainerError's message. May be blank.
        @param  timeout     Amount of time (in seconds) to wait for the ready, default 20 seconds.
        @throws ContainerError  If the thread dies or if we get a timeout before the process signals ready.
        """
        if not errmsg:
            errmsg = "ensure_ready failed"

        ev = Event()

        def cb(*args, **kwargs):
            ev.set()

        # link either a greenlet failure due to exception OR a success via ready event
        proc.proc.link_exception(cb)
        ready_evt = proc.get_ready_event()
        ready_evt.rawlink(cb)

        retval = ev.wait(timeout=timeout)

        # unlink the events: ready event is probably harmless but the exception one, we want to install our own later
        ready_evt.unlink(cb)

        # if the thread is stopped while we are waiting, proc.proc is set to None
        if proc.proc is not None:
            proc.proc.unlink(cb)

        # raise an exception if:
        # - we timed out
        # - we caught an exception
        if not retval:
            raise ContainerError("%s (timed out)" % errmsg)
        elif proc.proc is not None and proc.proc.dead and not proc.proc.successful():
            raise ContainerError("%s (failed): %s" % (errmsg, proc.proc.exception))

    def child_stopped(self, proc):
        if proc in self.children:
            # no longer need to listen for exceptions
            if proc.proc is not None:
                proc.proc.unlink(self._child_failed)

    def join_children(self, timeout=None):
        """ Give child threads "timeout" seconds to shutdown, then forcibly terminate. """

        time_start = time.time()
        child_count = len(self.children)

        for proc in self.children:

            # if a child thread has already exited, we don't need to wait on anything -
            # it's already good to go and can be considered joined. Otherwise we will likely
            # double call notify_stop which is a bad thing.
            if proc.proc.dead:
                continue

            time_elapsed = time.time() - time_start
            if timeout is not None:
                time_remaining = timeout - time_elapsed

                if time_remaining > 0:
                    # The nice way; let it do cleanup
                    try:
                        proc.notify_stop()
                        proc.join(time_remaining)
                    except Exception:
                        # not playing nice? just kill it.
                        proc.stop()

                else:
                    # Out of time. Cya, sucker
                    proc.stop()
            else:
                proc.join()

        time_elapsed = time.time() - time_start
        #log.debug("Took %.2fs to shutdown %d child threads", time_elapsed, child_count)

        return time_elapsed

    def wait_children(self, timeout=None):
        """
        Performs a join to allow children to complete, then a get() to fetch their results.

        This will raise an exception if any of the children raises an exception.
        """
        self.join_children(timeout=timeout)
        return [x.get() for x in self.children]

    def target(self):
        try:
            while not self._shutting_down:
                self.send_heartbeats()
                self._shutdown_event.wait(timeout=self.heartbeat_secs)
        except:
            log.error("thread died", exc_info=True)

    def send_heartbeats(self):
        """ TODO: implement heartbeat and monitors """
        #log.debug("lub-dub")
        pass

    def shutdown(self, timeout=30.0):
        """
        @brief Give child thread "timeout" seconds to shutdown, then forcibly terminate.
        """
        self._shutting_down = True
        self._shutdown_event.set(True)
        elapsed = self.join_children(timeout)

        #unset()
        return elapsed


class PyonThreadManager(ThreadManager, PyonThread):
    """
    A thread manager that runs in a thread and can spawn threads.
    """
    pass
