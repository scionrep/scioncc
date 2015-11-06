""" Maintains stats for the ScionCC container and enables interested clients to access stats and callbacks. """

__author__ = 'Michael Meisinger'

from pyon.core.bootstrap import CFG
from pyon.core.exception import ContainerConfigError
from pyon.util.log import log
from pyon.util.tracer import CallTracer


class ContainerStatsManager(object):
    _started = False
    _stats_callbacks = None

    SAVE_MSG_MAX = 1000

    def __init__(self, container):
        self.container = container
        self.stats_groups = ["PROC", "SVCREQ", "MSG", "DB"]

    def start(self):
        if self._started:
            return

        self._clear_stats_groups()
        self._activate_collection()

        # Install the container tracer
        self.container.tracer = CallTracer
        self.container.tracer.configure(CFG.get_safe("container.tracer", {}))

        self._started = True

    def stop(self):
        if self._started:
            self._deactivate_collection()
            self._clear_stats_groups()
            self._started = False

    # -------------------------------------------------------------------------

    def register_callback(self, group, cb_func):
        if not self._started:
            raise ContainerConfigError("StatsManager not started")
        if group not in self._stats_callbacks:
            raise ContainerConfigError("Unknown stats group: %s" % group)
        if group == "SVCREQ":
            # Special treatment because service gateway may be started late
            from ion.services.service_gateway import sg_instance
            if sg_instance and not sg_instance.request_callback:
                sg_instance.register_request_callback(self._sg_callback)
        cbs = self._stats_callbacks[group]
        if cb_func in cbs:
            return
        cbs.append(cb_func)

    def unregister_callback(self, group, cb_func):
        if not self._started:
            raise ContainerConfigError("StatsManager not started")
        if group not in self._stats_callbacks:
            raise ContainerConfigError("Unknown stats group: %s" % group)
        cbs = self._stats_callbacks[group]
        cbs.pop(cb_func, None)

    # -------------------------------------------------------------------------

    def _clear_stats_groups(self):
        self._stats_callbacks = dict(zip(self.stats_groups, [[] for i in range(len(self.stats_groups))]))

    def _activate_collection(self):
        CallTracer.set_formatter("MSG.out", self._msg_trace_formatter)
        CallTracer.set_formatter("MSG.in", self._msg_trace_formatter)

        from pyon.net import endpoint
        endpoint.callback_msg_out = self._msg_out_callback
        endpoint.callback_msg_in = self._msg_in_callback

        from pyon.datastore.postgresql.base_store import set_db_stats_callback
        set_db_stats_callback(self._db_callback)

        from ion.services.service_gateway import sg_instance
        if sg_instance:
            # This container may not run the service gateway
            sg_instance.register_request_callback(self._sg_callback)

        from pyon.ion.process import set_process_stats_callback
        set_process_stats_callback(self._proc_callback)

    def _deactivate_collection(self):
        from ion.services.service_gateway import sg_instance
        if sg_instance:
            # This container may not run the service gateway
            sg_instance.register_request_callback(None)

        from pyon.ion.process import set_process_stats_callback
        set_process_stats_callback(None)

        from pyon.datastore.postgresql.base_store import set_db_stats_callback
        set_db_stats_callback(None)

        from pyon.net import endpoint
        endpoint.callback_msg_in = None
        endpoint.callback_msg_out = None

    # -------------------------------------------------------------------------

    def _call_callbacks(self, group, action, cb_data):
        cbs = self._stats_callbacks[group]
        for cb in cbs:
            try:
                cb(group, action, cb_data)
            except Exception:
                log.exception("Error in stats callback")

    def _sg_callback(self, action, req_info):
        self._call_callbacks("SVCREQ", action, req_info)

    def _proc_callback(self, **kwargs):
        self._call_callbacks("PROC", "op_complete", kwargs)

    def _msg_in_callback(self, msg, headers, env):
        log_entry = dict(status="RECV %s bytes" % len(msg), headers=headers, env=env,
                         content_length=len(msg), content=str(msg)[:self.SAVE_MSG_MAX])
        CallTracer.log_scope_call("MSG.in", log_entry, include_stack=False)
        self._call_callbacks("MSG", "in", log_entry)

    def _msg_out_callback(self, msg, headers, env):
        log_entry = dict(status="SENT %s bytes" % len(msg), headers=headers, env=env,
                         content_length=len(msg), content=str(msg)[:self.SAVE_MSG_MAX])
        CallTracer.log_scope_call("MSG.out", log_entry, include_stack=False)
        self._call_callbacks("MSG", "out", log_entry)

    def _db_callback(self, scope, log_entry):
        CallTracer.log_scope_call(scope, log_entry, include_stack=True)
        self._call_callbacks("DB", scope, log_entry)

    @staticmethod
    def _msg_trace_formatter(log_entry, **kwargs):
        # Warning: Make sure this code is reentrant. Will be called multiple times for the same entry
        frags = []
        msg_type = "UNKNOWN"
        sub_type = ""
        try:
            content = log_entry.get("content", "")
            headers = dict(log_entry.get("headers", {}))
            env = log_entry.get("env", {})

            if "sender" in headers or "sender-service" in headers:
                # Case RPC msg
                sender_service = headers.get('sender-service', '')
                sender = headers.pop('sender', '').split(",", 1)[-1]
                sender_name = headers.pop('sender-name', '')
                sender_txt = (sender_name or sender_service) + " (%s)" % sender if sender else ""
                recv = headers.pop('receiver', '?').split(",", 1)[-1]
                op = "op=%s" % headers.pop('op', '?')
                sub_type = op
                stat = "status=%s" % headers.pop('status_code', '?')
                conv_seq = headers.get('conv-seq', '0')

                if conv_seq == 1:
                    msg_type = "RPC REQUEST"
                    frags.append("%s %s -> %s %s" % (msg_type, sender_txt, recv, op))
                else:
                    msg_type = "RPC REPLY"
                    frags.append("%s %s -> %s %s" % (msg_type, sender_txt, recv, stat))
                try:
                    import msgpack
                    msg = msgpack.unpackb(content)
                    frags.append("\n C:")
                    frags.append(str(msg))
                except Exception as ex:
                    pass
            else:
                # Case event/other msg
                try:
                    import msgpack
                    msg = msgpack.unpackb(content)
                    ev_type = msg["type_"] if isinstance(msg, dict) and "type_" in msg else "?"
                    msg_type = "EVENT"
                    sub_type = ev_type
                    frags.append("%s %s" % (msg_type, ev_type))
                    frags.append("\n C:")
                    frags.append(str(msg))

                except Exception:
                    msg_type = "UNKNOWN"
                    frags.append(msg_type)
                    frags.append("\n C:")
                    frags.append(content)

            frags.append("\n H:")
            frags.append(str(headers))
            frags.append("\n E:")
            frags.append(str(env))
        except Exception as ex:
            frags = ["ERROR parsing message: %s" % str(ex)]
        log_entry["statement"] = "".join(frags)
        log_entry["msg_type"] = msg_type
        log_entry["sub_type"] = sub_type

        return CallTracer._default_formatter(log_entry, **kwargs)
