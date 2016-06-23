""" Simple agent that streams data and can be extended by plugins. """

__author__ = 'Michael Meisinger'

from pyon.public import log, RT, BadRequest, named_any, StreamPublisher
from pyon.util.containers import dict_merge


from interface.services.agent.istreaming_agent import BaseStreamingAgent


class StreamingAgent(BaseStreamingAgent):
    # Constants
    AGENTSTATE_NEW = "new"
    AGENTSTATE_INITIALIZED = "initialized"
    AGENTSTATE_CONNECTED = "connected"
    AGENTSTATE_STREAMING = "streaming"
    AGENTSTATE_ERROR = "error"

    # ION process type
    name = "streaming_agent"
    process_type = "agent"

    # Instance defaults (public)
    resource_type = "streaming_agent"
    resource_id = None
    agent_id = None
    agent_def_id = None
    agent_type = "agent"

    # Instance defaults (local)
    current_state = AGENTSTATE_NEW
    params = {}
    agent_plugin = None
    stream_name = None


    def on_init(self):
        log.info("Start agent %s pid=%s resource_id=%s", self.__class__.__name__, self.id, self.resource_id)
        self.current_state = self.AGENTSTATE_INITIALIZED
        self.agent_config = self.CFG.get_safe("agent_config") or {}
        self.params = {}
        self.agent_plugin = None
        self.stream_name = self.agent_config.get("stream_name", None) or "stream_" + self.resource_id
        if "plugin" in self.agent_config:
            agent_plugin_cls = named_any(self.agent_config["plugin"])
            log.info("Instantiate agent plugin '%s'", self.agent_config["plugin"])
            self.agent_plugin = agent_plugin_cls(self, self.agent_config)
        if self.agent_config.get("auto_streaming", False) is True:
            self.connect()
            self.start_streaming()


    def on_stop(self):
        pass

    def on_quit(self):
        if self.current_state in (self.AGENTSTATE_STREAMING, self.AGENTSTATE_CONNECTED):
            log.info("Terminate agent %s pid=%s resource_id=%s", self.__class__.__name__, self.id, self.resource_id)
            self.disconnect()

    def connect(self, connect_args=None):
        if self.current_state == self.AGENTSTATE_CONNECTED:
            return
        elif self.current_state != self.AGENTSTATE_INITIALIZED:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        try:
            self.stream_pub = StreamPublisher(process=self, stream=self.stream_name)
            args = dict_merge(self.agent_config, connect_args) if connect_args else self.agent_config
            res = self.on_connect(args)
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise
        self.current_state = self.AGENTSTATE_CONNECTED

    def on_connect(self, connect_args=None):
        pass

    def start_streaming(self, streaming_args=None):
        if self.current_state == self.AGENTSTATE_STREAMING:
            return
        if self.current_state == self.AGENTSTATE_INITIALIZED:
            self.connect(self.agent_config)
        if self.current_state != self.AGENTSTATE_CONNECTED:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        log.info("Start streaming")
        try:
            args = dict_merge(self.agent_config, streaming_args) if streaming_args else self.agent_config
            res = self.on_start_streaming(args)
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise
        self.current_state = self.AGENTSTATE_STREAMING

    def on_start_streaming(self, streaming_args=None):
        pass

    def stop_streaming(self):
        if self.current_state == self.AGENTSTATE_CONNECTED:
            return
        elif self.current_state != self.AGENTSTATE_STREAMING:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        log.info("Stop streaming")
        try:
            res = self.on_stop_streaming()
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise
        self.current_state = self.AGENTSTATE_CONNECTED

    def on_stop_streaming(self):
        pass

    def acquire_data(self, streaming_args=None):
        if self.current_state != self.AGENTSTATE_CONNECTED:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        try:
            args = dict_merge(self.agent_config, streaming_args) if streaming_args else self.agent_config
            res = self.on_acquire_data(args)
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise

    def on_acquire_data(self, streaming_args=None):
        pass

    def disconnect(self):
        if self.current_state in self.AGENTSTATE_INITIALIZED:
            return
        if self.current_state == self.AGENTSTATE_STREAMING:
            self.stop_streaming()
        if self.current_state != self.AGENTSTATE_CONNECTED:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        try:
            res = self.on_disconnect()
            self.stream_pub.close()
            self.stream_pub = None
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise
        self.current_state = self.AGENTSTATE_INITIALIZED

    def on_disconnect(self):
        pass

    def get_status(self):
        agent_status = dict(current_state=self.current_state)
        agent_status = self.on_get_status(agent_status)
        return agent_status

    def on_get_status(self, agent_status):
        return agent_status

    def get_params(self, param_list=None):
        if param_list:
            return {k: v for (k, v) in self.params.iteritems() if k in param_list}
        else:
            return self.params

    def set_params(self, params=None):
        self.params.update(params)

    def on_set_params(self, params=None):
        pass


class AgentPlugin(object):
    def __init__(self, process, config):
        self.process = process
        self.agent_config = config
