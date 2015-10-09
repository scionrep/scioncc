#!/usr/bin/env python

__author__ = "Michael Meisinger"

from pyon.core.bootstrap import get_service_registry
from pyon.public import CFG, get_safe

UISG_CFG_PREFIX = "process.ui_server.service_gateway"
ALL_SERVICES = "*"


class SwaggerSpecGenerator(object):
    def __init__(self, config=None):
        self.config = config or {}
        self.specs_cache = {}

    def get_spec(self, service_name=None):
        service_name = service_name or ALL_SERVICES
        if service_name in self.specs_cache:
            return self.specs_cache[service_name]
        service_spec = self._gen_service_spec(service_name)
        self.specs_cache[service_name] = service_spec
        return service_spec

    def _gen_service_spec(self, service_name):
        output = {
            "swagger": "2.0",
            "info": {},
            "host": CFG.get_safe("system.web_ui_url", "").rstrip("/").split("//", 1)[1],
            "basePath": CFG.get_safe(UISG_CFG_PREFIX + ".url_prefix"),
            "schemes": [
                "http"
            ],
        }
        output["info"].update(self.config.get("info", {}))
        if self.config.get("externalDocs", {}):
            output["externalDocs"] = self.config["externalDocs"]

        output["tags"] = self._gen_tags(service_name)
        output["paths"] = self._gen_paths(service_name)

        return output

    def _gen_tags(self, service_name):
        tags = []
        sr = get_service_registry()
        if service_name == ALL_SERVICES:
            for svc_name in sorted(sr.services_by_name):
                if svc_name in self.config.get("exclude_services", []):
                    continue
                svc_schema = sr.services[svc_name].schema
                tag_entry = dict(name=svc_name, description=svc_schema["description"])
                if self.config.get("externalDocs", {}):
                    tag_entry["externalDocs"] = self.config["externalDocs"]
                tags.append(tag_entry)
        else:
            svc_def = sr.services.get(service_name, None)
            if svc_def:
                svc_schema = svc_def.schema
                tag_entry = dict(name=service_name, description=svc_schema["description"])
                if self.config.get("externalDocs", {}):
                    tag_entry["externalDocs"] = self.config["externalDocs"]
                tags.append(tag_entry)

        return tags

    def _gen_paths(self, service_name):
        paths = {}
        sr = get_service_registry()
        if service_name == ALL_SERVICES:
            for svc_name in sorted(sr.services_by_name):
                if svc_name in self.config.get("exclude_services", []):
                    continue
                svc_schema = sr.services[svc_name].schema
                # For operations
                for op_name in svc_schema["op_list"]:
                    self._add_service_op_entries(paths, svc_name, op_name, svc_schema)
        else:
            svc_def = sr.services.get(service_name, None)
            if svc_def:
                svc_schema = svc_def.schema
                for op_name in svc_schema["op_list"]:
                    self._add_service_op_entries(paths, service_name, op_name, svc_schema)

        return paths

    def _add_service_op_entries(self, paths, service_name, op_name, svc_schema):
        self._add_service_op_get_path(paths, service_name, op_name, svc_schema)
        # POST
        # REST

    PAR_QUERY_TYPE_MAP = {"str": "string", "int": "integer", "float": "number", "bool": "boolean"}
    PAR_BODY_TYPE_MAP = {"str": "string", "int": "integer", "float": "number", "list": "array", "dict": "", "bool": "boolean"}

    def _get_result_schema(self, op_schema):
        schema = dict(type="object", properties=dict(status=dict(type="integer")))
        if len(op_schema["out_list"]) == 1:
            par_schema = op_schema["out"].values()[0]
            schema["properties"]["result"] = dict(type=self.PAR_BODY_TYPE_MAP.get(par_schema["type"], str(par_schema["type"])))
        elif len(op_schema["out_list"]) > 1:
            schema["properties"]["result"] = dict(type="string")

        return schema

    EXC_MAP = {"BadRequest": "400", "NotFound": "404", "Conflict": "409", "Unauthorized": "401", "NotAcceptable": "406"}

    def _add_throws(self, path_entry, op_schema):
        if not op_schema["throws"]:
            return
        for k, v in op_schema["throws"].iteritems():
            exc_num = self.EXC_MAP.get(k, None)
            if exc_num:
                path_entry["responses"][exc_num] = dict(description="%s: %s (JSON with keys status, exception, message)" % (k, v))


    def _add_service_op_get_path(self, paths, service_name, op_name, svc_schema):
        path_key = "/request/%s/%s" % (service_name, op_name)
        op_schema = svc_schema["operations"][op_name]
        path_entry = dict(
            tags=[service_name],
            summary=op_schema["description"].split(". ", 1)[0],
            description=op_schema["description"],
            operationId="%s/%s" % (service_name, op_name),
            produces=["application/json"],
            parameters=[],
            responses={"200": dict(description="Service result")}
        )
        for par_name in op_schema["in_list"]:
            par_schema = op_schema["in"][par_name]
            par_type = self.PAR_QUERY_TYPE_MAP.get(par_schema["type"], None)
            if not par_type:
                par_type = "string"
            par_entry = dict(name=par_name, type=par_type, required=False, description=par_schema["description"],
                             default=par_schema["default"])
            par_entry["in"] = "query"
            path_entry["parameters"].append(par_entry)

        if len(op_schema["out_list"]) == 0:
            path_entry["responses"]["200"]["description"] = path_entry["responses"]["200"]["description"] + \
                    ": JSON with keys status and result=empty"
        elif len(op_schema["out_list"]) == 1:
            par_schema = op_schema["out"].values()[0]
            path_entry["responses"]["200"]["description"] = path_entry["responses"]["200"]["description"] + \
                    ": JSON with keys status and result: %s (%s)" % (par_schema["name"], par_schema["type"])
        elif len(op_schema["out_list"]) > 1:
            path_entry["responses"]["200"]["description"] = path_entry["responses"]["200"]["description"] + \
                    ": JSON with keys status and result: list of %s" % (", ".join(op_schema["out_list"]))

        self._add_throws(path_entry, op_schema)

        path_entry["externalDocs"] = {"$ref": "#/externalDocs"}

        paths.setdefault(path_key, {})["get"] = path_entry
