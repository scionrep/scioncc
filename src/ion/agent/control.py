""" Support for controlling agents """

__author__ = 'Michael Meisinger'

from pyon.public import log, RT, BadRequest, Container, NotFound
from ion.core.process.proc_util import ProcessStateGate

from interface.services.agent.istreaming_agent import StreamingAgentProcessClient


class AgentControl(object):

    def __init__(self, resource_id=None):
        self.resource_id = resource_id
        self.process_id = None

    def launch_agent(self, resource_id, agent_type, agent_config):
        if StreamingAgentClient.is_agent_active(resource_id):
            raise BadRequest("Agent already active for resource_id=%s" % resource_id)

        if agent_type == "data_agent":
            agent_mod, agent_cls = "ion.agent.data_agent", "DataAgent"
        elif agent_type == "streaming_agent":
            agent_mod, agent_cls = "ion.agent.streaming_agent", "StreamingAgent"
        elif agent_type == "instrument_agent":
            agent_mod, agent_cls = "ion.agent.streaming_agent", "StreamingAgent"
        else:
            raise BadRequest("Unknown agent type: %s" % agent_type)

        agent_config = agent_config.copy() if agent_config else {}
        config = dict(agent_config=agent_config, agent=dict(resource_id=resource_id))

        agent_name = agent_type + "_" + resource_id
        self.process_id = Container.instance.spawn_process(agent_name, agent_mod, agent_cls, config)
        return self.process_id

    def terminate_agent(self):
        if self.process_id:
            Container.instance.terminate_process(self.process_id)
        elif self.resource_id:
            ac = StreamingAgentClient(self.resource_id)
            proc_id = ac.get_agent_process_id()
            if proc_id in Container.instance.proc_manager.procs:
                Container.instance.terminate_process(proc_id)
            else:
                raise BadRequest("Cannot terminate agent locally")
        else:
            raise BadRequest("Cannot terminate agent")


class StreamingAgentClient(StreamingAgentProcessClient):
    """
    Generic client for resource agents.
    """
    class FakeAgentProcess(object):
        name = "streaming_agent"
        id = ""
        container = Container.instance

    def __init__(self, resource_id, *args, **kwargs):
        """
        Resource agent client constructor.
        @param resource_id The ID this service represents.
        @param name Use this kwarg to set the target exchange name (= agent process id or service name)
        (service or process).
        """

        # Assert and set the resource ID.
        if not resource_id:
            raise BadRequest("resource_id must be set for an agent")
        self.resource_id = resource_id
        self.agent_process_id = None
        self.agent_dir_entry = None

        # Set the name, retrieve as proc ID if not set by user.
        if 'name' not in kwargs:
            self.agent_process_id = self._get_agent_process_id(self.resource_id, client_instance=self)
            if self.agent_process_id:
                log.debug("Use agent process %s for resource_id=%s" % (self.agent_process_id, self.resource_id))
            else:
                log.debug("No agent process found for resource_id %s" % self.resource_id)
                raise NotFound("No agent process found for resource_id %s" % self.resource_id)
        else:
            self.agent_process_id = kwargs.pop("name")

        # transpose name -> to_name to make underlying layer happy
        #kwargs["to_name"] = self.agent_process_id
        kwargs["to_name"] = resource_id

        # HACK to allow use of this client without process
        if "process" not in kwargs:
            log.warn("Using FakeProcess to allow agent client without process arg")
            kwargs["process"] = StreamingAgentClient.FakeAgentProcess()

        kwargs["declare_name"] = False

        # Superclass constructor.
        StreamingAgentProcessClient.__init__(self, *args, **kwargs)

    # -------------------------------------------------------------------------
    # Agent interface

    def connect(self, *args, **kwargs):
        return super(StreamingAgentClient, self).connect(*args, **kwargs)

    def start_streaming(self, *args, **kwargs):
        return super(StreamingAgentClient, self).start_streaming(*args, **kwargs)

    def stop_streaming(self, *args, **kwargs):
        return super(StreamingAgentClient, self).stop_streaming(*args, **kwargs)

    def acquire_data(self, *args, **kwargs):
        return super(StreamingAgentClient, self).acquire_data(*args, **kwargs)

    def disconnect(self, *args, **kwargs):
        return super(StreamingAgentClient, self).disconnect(*args, **kwargs)

    def get_status(self, *args, **kwargs):
        return super(StreamingAgentClient, self).get_status(*args, **kwargs)

    def get_params(self, *args, **kwargs):
        return super(StreamingAgentClient, self).get_params(*args, **kwargs)

    def set_params(self, *args, **kwargs):
        return super(StreamingAgentClient, self).set_params(*args, **kwargs)

    # -------------------------------------------------------------------------
    # Helpers

    @classmethod
    def _get_agent_process_id(cls, resource_id, client_instance=None):
        """
        Return the agent container process id given the resource_id.
        DO NOT USE THIS CALL. Use an instance of this class and ac.get_agent_process_id() instead
        """
        agent_procs = Container.instance.directory.find_by_value('/Agents', 'resource_id', resource_id)
        if agent_procs:
            agent_proc_entry = agent_procs[0]
            if len(agent_procs) > 1:
                log.warn("Inconsistency: More than one agent registered for resource_id=%s: %s" % (
                    resource_id, agent_procs))
                agent_proc_entry = Container.instance.directory._cleanup_outdated_entries(
                    agent_procs, "agent resource_id=%s" % resource_id)

            agent_id = agent_proc_entry.key
            if client_instance is not None:
                client_instance.agent_dir_entry = agent_proc_entry
            return str(agent_id)
        return None

    def get_agent_process_id(self):
        """
        Returns the process id for the agent process representing this instance's resource
        """
        return self.agent_process_id

    @classmethod
    def is_agent_active(cls, resource_id):
        try:
            agent_pid = cls._get_agent_process_id(resource_id)
            return bool(agent_pid)
        except NotFound:
            return False

    def get_agent_directory_entry(self):
        """
        Returns the directory entry for the agent process representing this instance's resource
        """
        return self.agent_dir_entry

    def await_agent_process_launch(self, timeout=0):
        raise NotImplementedError()
