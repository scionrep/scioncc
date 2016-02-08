

from pyon.public import log, RT, BadRequest, StandaloneProcess, named_any

from interface.services.agent.istreaming_agent import BaseStreamingAgent


class StreamingAgent(StandaloneProcess):
    # Constants
    AGENTSTATE_NEW = "new"
    AGENTSTATE_INITIALIZED = "initialized"
    AGENTSTATE_CONNECTED = "connected"
    AGENTSTATE_STREAMING = "streaming"
    AGENTSTATE_ERROR = "error"

    # ION process type.
    name = "streaming_agent"
    process_type = "standalone"

    # Instance defaults
    current_state = AGENTSTATE_NEW
    params = {}
    agent_plugin = None


    def on_init(self):
        log.info("Start agent %s pid=%s", self.__class__.__name__, self.id)
        self.current_state = self.AGENTSTATE_INITIALIZED
        self.agent_config = self.CFG.get_safe("agent_config") or {}
        self.resource_id = self.agent_config.get("resource_id", None)
        self.params = {}
        self.agent_plugin = None
        if "plugin" in self.agent_config:
            agent_plugin_cls = named_any(self.agent_config["plugin"])
            log.info("Instantiate agent plugin '%s'", self.agent_config["plugin"])
            self.agent_plugin = agent_plugin_cls(self, self.agent_config)

        self.container.directory.register("/Agents", self.id,
                **dict(name=self._proc_name,
                       container=self.container.id,
                       resource_id=self.resource_id))

    def on_stop(self):
        if self.current_state in (self.AGENTSTATE_STREAMING, self.AGENTSTATE_CONNECTED):
            self.disconnect()

    def on_quit(self):
        self.container.directory.unregister_safe("/Agents", self.id)

    def connect(self, connect_args=None):
        if self.current_state == self.AGENTSTATE_CONNECTED:
            return
        elif self.current_state != self.AGENTSTATE_INITIALIZED:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        try:
            res = self.on_connect(connect_args)
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise
        self.current_state = self.AGENTSTATE_CONNECTED

    def on_connect(self, connect_args=None):
        pass

    def start_streaming(self, streaming_args=None):
        if self.current_state == self.AGENTSTATE_INITIALIZED:
            self.connect({})
        if self.current_state != self.AGENTSTATE_CONNECTED:
            raise BadRequest("Illegal agent state: %s" % self.current_state)
        log.info("Start streaming")
        try:
            res = self.on_start_streaming(streaming_args)
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
            res = self.on_acquire_data(streaming_args)
        except Exception:
            self.current_state = self.AGENTSTATE_ERROR
            raise

    def on_acquire_data(self, streaming_args=None):
        pass

    def disconnect(self):
        if self.current_state == self.AGENTSTATE_INITIALIZED:
            return
        if self.current_state != self.AGENTSTATE_STREAMING:
            self.stop_streaming()
        try:
            res = self.on_disconnect()
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
