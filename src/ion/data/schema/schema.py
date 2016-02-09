""" Tools for parsing data schema definitions. """

__author__ = 'Michael Meisinger'

import os
import yaml

from pyon.public import BadRequest


class DataSchemaParser(object):
    @classmethod
    def parse_schema_ref(cls, schema_ref):
        if not schema_ref:
            raise BadRequest("Data schema ref missing")
        schema_content = None
        if os.path.exists(schema_ref):
            with open(schema_ref, "r") as f:
                schema_content = f.read()
        elif not os.path.isabs(schema_ref) and os.path.exists("res/data/dataset/%s.yml" % schema_ref):
            with open("res/data/dataset/%s.yml" % schema_ref, "r") as f:
                schema_content = f.read()
        if not schema_content:
            raise BadRequest("Data schema not found from ref: %s" % schema_ref)
        schema_def = yaml.safe_load(schema_content)
        if schema_def.get("type", None) != "scion_data_schema_1":
            raise BadRequest("Invalid schema definition format: %s" % schema_def.get("type", ""))

        cls._check_variables(schema_def)

        return schema_def

    @classmethod
    def _check_variables(cls, schema_def):
        # Check variable types

        # Check that a time variable exists
        pass
