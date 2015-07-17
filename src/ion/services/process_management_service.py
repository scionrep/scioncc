#!/usr/bin/env python

import gevent

from pyon.public import log, RT, OT, PRED, NotFound, BadRequest, CFG

from ion.core.process.pd_core import ProcessDispatcherClient

from interface.services.core.iprocess_management_service import BaseProcessManagementService
from interface.objects import ProcessStateEnum, Process


class ProcessManagementService(BaseProcessManagementService):

    def on_init(self):
        self.rr = self.clients.resource_registry
        self.pd_client = ProcessDispatcherClient(self.container, CFG.get_safe("service.process_dispatcher", {}))

    # -------------------------------------------------------------------------

    def create_process_definition(self, process_definition=None):
        self._validate_resource_obj("process_definition", process_definition, RT.ProcessDefinition, checks="name")
        if not process_definition.module or not process_definition.class_name:
            raise BadRequest("Argument process_definition must have module and class")

        pd_id, _ = self.rr.create(process_definition)
        return pd_id

    def read_process_definition(self, process_definition_id=''):
        pd_obj = self._validate_resource_id("process_definition_id", process_definition_id, RT.ProcessDefinition)
        return pd_obj

    def delete_process_definition(self, process_definition_id=''):
        pd_obj = self._validate_resource_id("process_definition_id", process_definition_id, RT.ProcessDefinition)
        self.rr.delete(process_definition_id)

    def create_process(self, process_definition_id='', process=None):
        pd_obj = self._validate_resource_id("process_definition_id", process_definition_id, RT.ProcessDefinition)
        self._validate_resource_obj("process", process, RT.Process, optional=True)

        if process is None:
            process = Process(name=self._create_process_name(None, pd_obj, None))
        process_id, _ = self.rr.create(process)
        self.rr.create_association(process_id, PRED.hasProcessDefinition, process_definition_id)

        return process_id

    def schedule_process(self, process_definition_id='', schedule=None, configuration=None, process_id='', name=''):
        pd_obj = self._validate_resource_id("process_definition_id", process_definition_id, RT.ProcessDefinition, optional=True)
        process_obj = self._validate_resource_id("process_id", process_id, RT.Process, optional=True)
        require_update = False

        if configuration is None:
            configuration = {}

        if process_obj:
            if not process_obj.name:
                process_obj.name = name or self._create_process_name(process_obj, pd_obj, configuration)
                require_update = True
        else:
            process_obj = Process(name=name or self._create_process_name(None, pd_obj, configuration))
            process_id = self.create_process(process_definition_id, process_obj)
            #process_obj = self.rr.read(process_id)

        if require_update:
            self.rr.update(process_obj)

        cmd_id = self._pd_core.schedule(process_id, pd_obj, schedule, configuration, name)
        return cmd_id

    def _create_process_name(self, process, process_definition, configuration):
        if process.name:
            return process.name
        pd_name = process_definition.name or "process"
        if pd_name.startswith("ProcessDefinition for "):
            pd_name = pd_name[len("ProcessDefinition for "):]
        name_parts = [str(pd_name), process._id]
        name = '_'.join(name_parts)

        return name

    def cancel_process(self, process_id=''):
        process_obj = self._validate_resource_id("process_id", process_id, RT.Process)

        cmd_id = self._pd_core.cancel(process_id)
        return cmd_id

    def read_process(self, process_id=''):
        process_obj = self._validate_resource_id("process_id", process_id, RT.Process)

        return self._pd_core.read_process(process_id)

    def list_processes(self):
        cmd_id = self._pd_core.list()

        return
