#!/usr/bin/env python

"""Common utilities to parse external input, e.g. for preload and service gateway"""

__author__ = 'Michael Meisinger, Ian Katz'

import ast

from pyon.public import BadRequest, IonObject, log

from interface import objects


def get_typed_value(value, schema_entry=None, targettype=None, strict=False):
    """
    Performs a value type check or conversion according to a schema entry or specified target type.
    Supports simplelist and parsedict special type parsing from strings.
    @param strict  if True, raise error of type does not match
    """
    if not schema_entry and not targettype:
        raise BadRequest("Invalid schema or targettype")
    targettype = targettype or schema_entry["type"]
    if schema_entry and 'enum_type' in schema_entry:
        enum_clzz = getattr(objects, schema_entry['enum_type'])
        if type(value).__name__ == targettype and value in enum_clzz._str_map:
            return value
        if isinstance(value, basestring):
            if strict and value in enum_clzz._value_map:
                return enum_clzz._value_map[value]
            elif not strict:
                if value in enum_clzz._value_map:
                    return enum_clzz._value_map[value]
                for enum_key, enum_val in enum_clzz._value_map.iteritems():
                    if enum_key.lower() == value.lower():
                        return enum_val
        raise BadRequest("Value %s is not valid enum value" % value)

    elif targettype == 'str':
        if type(value) is str:
            return value
        elif type(value) is unicode:
            return value.encode("utf8")
        if strict:
            raise BadRequest("Value %s is type %s not str" % (value, type(value).__name__))
        return str(value)

    elif targettype == 'bool':
        if type(value) is bool:
            return value
        if strict:
            raise BadRequest("Value %s is type %s not bool" % (value, type(value).__name__))
        if value in ('TRUE', 'True', 'true', '1', 1):
            return True
        elif value in ('FALSE', 'False', 'false', '0', 0, '', None):
            return False
        raise BadRequest("Value %s cannot be converted to bool" % value)

    elif targettype == 'int':
        if type(value) in (int, long):
            return value
        if strict:
            raise BadRequest("Value %s is type %s not int" % (value, type(value).__name__))
        try:
            return int(value)
        except Exception:
            pass
        raise BadRequest("Value %s cannot be converted to int" % value)

    elif targettype == 'float':
        if type(value) == float:
            return value
        elif type(value) in (int, long):
            return float(value)
        if strict:
            raise BadRequest("Value %s is type %s not float" % (value, type(value).__name__))
        try:
            return float(value)
        except Exception:
            pass
        raise BadRequest("Value %s cannot be converted to float" % value)

    elif targettype == 'simplelist':
        if isinstance(value, basestring):
            return parse_list(value)
        raise BadRequest("Value %s cannot be converted to list as simplelist" % value)

    elif targettype == 'parsedict':
        if isinstance(value, basestring):
            return parse_dict(value)
        raise BadRequest("Value %s cannot be converted to dict as parsedict" % value)

    elif targettype == 'list':
        if type(value) is list:
            return value
        if strict:
            raise BadRequest("Value %s is type %s not list" % (value, type(value).__name__))
        if isinstance(value, (tuple, set)):
            return list(value)
        elif isinstance(value, basestring):
            try:
                ret_val = ast.literal_eval(value)
            except Exception:
                ret_val = None
            if isinstance(ret_val, list):
                return ret_val
            elif isinstance(ret_val, tuple):
                return list(ret_val)
        if isinstance(value, basestring):
            return parse_list(value)
        else:
            return [value]

    elif targettype == 'dict':
        if type(value) is dict:
            return value
        if strict:
            raise BadRequest("Value %s is type %s not dict" % (value, type(value).__name__))
        if isinstance(value, dict):
            return dict(value)
        elif isinstance(value, basestring):
            try:
                ret_val = ast.literal_eval(value)
            except Exception:
                ret_val = None
            if isinstance(ret_val, dict):
                return ret_val
            return parse_dict(value)
        return dict(value=value)

    elif targettype == 'NoneType':
        if value is None:
            return None
        if not strict:
            if value in ("None", "NONE", "none", "Null", "NULL", "null", ""):
                return None
            elif isinstance(value, basestring):
                return ast.literal_eval(value)
        return value

    elif targettype == 'ANY':
        if isinstance(value, basestring):
            return ast.literal_eval(value)
        return value

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

    For example, an entry could be this:
      PARAMETERS.TXWAVESTATS: False,
      PARAMETERS.TXREALTIME: True,
      PARAMETERS.TXWAVEBURST: false,
      SCHEDULER.ACQUIRE_STATUS: {},
      SCHEDULER.CLOCK_SYNC: 48.2
      SCHEDULER.VERSION.number: 3.0
    which would translate back to:
      { "PARAMETERS": { "TXWAVESTATS": False, "TXREALTIME": True, "TXWAVEBURST": "false" },
        "SCHEDULER": { "ACQUIRE_STATUS": {}, "CLOCK_SYNC", 48.2, "VERSION": {"number": "3.0"}}
      }
    """

    substitutions = {"{}": {}, "[]": [], "True": True, "False": False}

    def parse_value(some_val):
        some_val = substitutions.get(some_val, some_val)

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
                if type(dict_ptr[key]) is not dict:
                    raise BadRequest("Building a dict in %s field, but it exists as %s already" %
                                         (key, type(dict_ptr[key])))
            dict_ptr = dict_ptr[key]
        last_ptr[keys[-1]] = value

    out = {}
    if text is None:
        return out

    pairs = text.split(',')  # pairs separated by commas
    for pair in pairs:
        if pair.count(':') == 0:
            continue
        fields = pair.split(':', 1)  # pair separated by first colon
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
