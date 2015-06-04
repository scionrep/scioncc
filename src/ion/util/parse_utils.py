#!/usr/bin/env python

"""Common utilities to parse external input, e.g. for preload"""

__author__ = 'Michael Meisinger, Ian Katz'

import ast

from pyon.public import BadRequest, IonObject, log

from interface import objects


def get_typed_value(value, schema_entry=None, targettype=None, strict=False):
    """
    Performs a value type conversion according to a schema entry or specified target type.
    Supports simplelist and parsedict special type parsing.
    @param strict  if True, raise error of not direct match
    """
    targettype = targettype or schema_entry["type"]
    if schema_entry and 'enum_type' in schema_entry:
        enum_clzz = getattr(objects, schema_entry['enum_type'])
        return enum_clzz._value_map[value]
    elif targettype == 'str':
        if type(value) is str:
            return value
        if type(value) is unicode:
            return value.encode("utf8")
        if not strict:
            return str(value)
        raise BadRequest("Value %s is no str" % value)
    elif targettype == 'bool':
        if value in ('TRUE', 'True', 'true', True):
            return True
        if value in ('FALSE', 'False', 'false', False):
            return False
        if not strict and value in ('1', 1):
            return True
        if not strict and value in ('0', 0, '', None):
            return False
        raise BadRequest("Value %s is no bool" % value)
    elif targettype == 'int':
        if type(value) in (int, long):
            return value
        if not strict:
            try:
                return int(value)
            except Exception:
                pass
        raise BadRequest("Value %s is type %s not int" % (value, type(value)))
    elif targettype == 'float':
        if type(value) == float:
            return value
        if not strict:
            try:
                return float(value)
            except Exception:
                pass
        raise BadRequest("Value %s is type %s not float" % (value, type(value)))
    elif targettype == 'simplelist':
        return parse_list(value)
    elif targettype == 'parsedict':
        return parse_dict(str(value))
    elif targettype == 'list':
        if type(value) is list:
            return value
        if not strict and (isinstance(value, tuple) or isinstance(value, set)):
            return list(value)
        try:
            ret_val = ast.literal_eval(value)
        except Exception:
            ret_val = None
        if isinstance(ret_val, list):
            return ret_val
        if not strict:
            if isinstance(ret_val, tuple):
                return list(ret_val)
            elif isinstance(value, basestring):
                return parse_list(value)
            else:
                return [value]
        raise BadRequest("Value %s is type %s not list" % (value, type(value)))
    elif targettype == 'dict':
        if type(value) is dict:
            return value
        if not strict and isinstance(value, dict):
            return dict(value)
        try:
            ret_val = ast.literal_eval(value)
        except Exception:
            ret_val = None
        if isinstance(ret_val, dict):
            return ret_val
        if not strict:
            if isinstance(value, basestring):
                return parse_dict(value)
            return dict(value=value)
        raise BadRequest("Value %s is type %s not dict" % (value, type(value)))
    elif targettype == 'NoneType':
        if value is None:
            return None
        if not strict:
            if value in ("None", "NONE", "none", "Null", "NULL", "null", ""):
                return None
            return value
    elif targettype == 'ANY':
        return ast.literal_eval(value)
    else:
        raise BadRequest("Value %s cannot be converted to target type %s" % (value, targettype))

def parse_list(value):
    """
    Parse a string to extract a simple list of string values.
    Assumes comma separated values optionally within []
    """
    if value.startswith('[') and value.endswith(']'):
        value = value[1:-1].strip()
    elif not value.strip():
        return []
    return list(value.split(','))

def parse_dict(text):
    """
    Parse a text string to obtain a dictionary of unquoted string keys and values.
    The following substitutions are made:
    keys with dots ('.') will be split into dictionaries.
    booleans "True", "False" will be parsed
    numbers will be parsed as floats unless they begin with "0" or include one "." and end with "0"
    "{}" will be converted to {}
    "[]" will be converted to []

    For example, an entry in preload would be this:

    PARAMETERS.TXWAVESTATS: False,
    PARAMETERS.TXREALTIME: True,
    PARAMETERS.TXWAVEBURST: false,
    SCHEDULER.ACQUIRE_STATUS: {},
    SCHEDULER.CLOCK_SYNC: 48.2
    SCHEDULER.VERSION.number: 3.0

    which would translate back to
    { "PARAMETERS": { "TXWAVESTATS": False, "TXREALTIME": True, "TXWAVEBURST": "false" },
      "SCHEDULER": { "ACQUIRE_STATUS": { }, "CLOCK_SYNC", 48.2, "VERSION": {"number": "3.0"}}
    }
    """

    substitutions = {"{}": {}, "[]": [], "True": True, "False": False}

    def parse_value(some_val):
        if some_val in substitutions:
            return substitutions[some_val]

        try:
            int_val = int(some_val)
            if str(int_val) == some_val:
                return int_val
        except ValueError:
            pass

        try:
            float_val = float(some_val)
            if str(float_val) == some_val:
                return float_val
        except ValueError:
            pass

        return some_val


    def chomp_key_list(out_dict, keys, value):
        """
        turn keys like ['a', 'b', 'c', 'd'] and a value into
        out_dict['a']['b']['c']['d'] = value
        """
        dict_ptr = out_dict
        last_ptr = out_dict
        for i, key in enumerate(keys):
            last_ptr = dict_ptr
            if not key in dict_ptr:
                dict_ptr[key] = {}
            else:
                if type(dict_ptr[key]) != type({}):
                    raise BadRequest("Building a dict in %s field, but it exists as %s already" %
                                         (key, type(dict_ptr[key])))
            dict_ptr = dict_ptr[key]
        last_ptr[keys[-1]] = value

    out = {}
    if text is None:
        return out

    pairs = text.split(',') # pairs separated by commas
    for pair in pairs:
        if 0 == pair.count(':'):
            continue
        fields = pair.split(':', 1) # pair separated by first colon
        key = fields[0].strip()
        value = fields[1].strip()

        keyparts = key.split(".")
        chomp_key_list(out, keyparts, parse_value(value))

    return out


def parse_phones(text):
    if ':' in text:
        out = []
        for type,number in parse_dict(text).iteritems():
            out.append(IonObject("Phone", phone_number=number, phone_type=type))
        return out
    elif text:
        return [ IonObject("Phone", phone_number=text.strip(), phone_type='office') ]
    else:
        return []
