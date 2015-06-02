#!/usr/bin/env python

__author__ = 'Adam R. Smith, Michael Meisinger, Tom Lennan'

import ast
import os
import re
import inspect
from collections import OrderedDict, Mapping, Iterable

from pyon.util.log import log
from pyon.core.exception import BadRequest

BUILT_IN_ATTRS = {'_id', '_rev', 'type_', 'blame_'}

# Validation decorators
DECO_VALIDATE_REQUIRED = 'Required'
DECO_VALIDATE_CONTENT_TYPE = 'ContentType'
DECO_VALIDATE_CONTENT_COUNT = 'ContentCount'
DECO_VALIDATE_VALUE_RANGE = 'ValueRange'
DECO_VALIDATE_VALUE_PATTERN = 'ValuePattern'


class IonObjectBase(object):
    """
    Base class for all ION objects. This base class provides a common ancestor of all types
    that can be evaluated using isinstance. It also provides some helpers and schema validation.
    An instance keeps all first level schema attributes inside the object's __dict__.
    The interface generator will create subclasses of this base class with additional fields,
    such as _schema, and _class_info and __init__ functions with subtype attributes.
    """
    _schema = {}
    _class_info = {}

    def __str__(self):
        ds = ", ".join("%s=%r" % (k, self.__dict__[k]) for k in sorted(self.__dict__.keys()) if k != "type_")
        return "%s(%s)" % (self.__class__.__name__, ds)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if type(other) == type(self):
            if other.__dict__ == self.__dict__:
                return True
        return False

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def __contains__(self, item):
        return hasattr(self, item)

    def has_key(self, key):
        return hasattr(self, key)

    def _validate(self, validate_objects=True):
        """
        Compare fields to the schema and raise AttributeError if mismatched.
        Named _validate instead of validate because the data may have a field named "validate".
        """
        fields, schema = self.__dict__, self._schema

        # Check for extra fields not defined in the schema
        extra_fields = fields.viewkeys() - schema.viewkeys() - BUILT_IN_ATTRS
        if len(extra_fields) > 0:
            raise AttributeError("Invalid field(s): %r" % (list(extra_fields)))

        # Check required field criteria met
        for key in schema:
            if DECO_VALIDATE_REQUIRED in schema[key].get('decorators', {}):
                if key not in fields or fields[key] is None:
                    raise AttributeError("Value required for '%s'" % key)

        # Check each attribute
        for key in fields.iterkeys():
            if key in BUILT_IN_ATTRS:
                continue

            schema_val = schema[key]
            schema_val_type = schema_val['type']
            schema_val_decos = schema_val.get('decorators', {})

            # Side effect - Correct any float or long types that got downgraded to int
            if isinstance(fields[key], int):
                if schema_val_type == 'float':
                    fields[key] = float(fields[key])
                elif schema_val_type == 'long':
                    fields[key] = long(fields[key])

            # Side effect - Work around for OrderedDict vs dict issue
            if type(fields[key]) == dict and schema_val_type == 'OrderedDict':
                fields[key] = OrderedDict(fields[key])

            # Basic type checking
            field_val = fields[key]
            field_val_type = type(field_val).__name__
            #log.debug("Validating %s: %s: %s: %s" % (key, schema_val_type, schema_val_decos, field_val))

            # This happens when TBD
            if field_val_type != schema_val_type:

                # If the schema type is None, all types are allowed
                if schema_val_type == 'NoneType':
                    continue

                # Allow unicode instead of str. This may be too lenient.
                if schema_val_type == 'str' and field_val_type == 'unicode':
                    continue

                # Already checked for required above.  Assume optional and continue
                if field_val is None:
                    continue

                # Allow unicode instead of str. This may be too lenient.
                if schema_val_type == 'str' and field_val_type == 'unicode':
                    continue

                # IonObjects are ok for dict fields too!
                if isinstance(field_val, IonObjectBase) and schema_val_type == 'OrderedDict':
                    continue

                # Check for inheritance
                if self._check_inheritance_chain(type(field_val), schema_val_type):
                    continue

                # Check enum types
                from pyon.core.registry import enum_classes
                if isinstance(field_val, int) and schema_val_type in enum_classes:
                    if field_val in enum_classes(schema_val_type)._str_map:
                        continue
                    raise AttributeError("Invalid enum value '%d' for field '%s.%s', should be between 1 and %d" %
                            (fields[key], type(self).__name__, key, len(enum_classes(schema_val_type)._str_map)))

                # Tuple allowed for list type (Msgpack decodes list to tuples)
                if type(field_val) == tuple and schema_val_type == 'list':
                    continue

                # IonObject allowed for dict type
                if isinstance(field_val, IonObjectBase) and schema_val_type == 'dict':
                    #log.warn('Please convert generic dict attribute type to abstract type for field "%s.%s"' % (type(self).__name__, key))
                    continue

                # Special case check for ION object being passed where default type is dict or str
                if DECO_VALIDATE_CONTENT_TYPE in schema_val_decos:
                    if isinstance(field_val, IonObjectBase) and schema_val_type in ('dict', 'str'):
                        self._check_content(key, field_val, schema_val_decos[DECO_VALIDATE_CONTENT_TYPE])
                        continue

                raise AttributeError("Invalid type '%s' for field '%s.%s', should be '%s'" %
                        (field_val_type, type(self).__name__, key, schema_val_type))

            if field_val_type == 'str' and DECO_VALIDATE_VALUE_PATTERN in schema_val_decos:
                self._check_string_pattern_match(key, field_val, schema_val_decos[DECO_VALIDATE_VALUE_PATTERN])

            if field_val_type in ('int', 'float', 'long') and DECO_VALIDATE_VALUE_RANGE in schema_val_decos:
                self._check_numeric_value_range(key, field_val, schema_val_decos[DECO_VALIDATE_VALUE_RANGE])

            if DECO_VALIDATE_CONTENT_TYPE in schema_val_decos:
                if schema_val_type == 'list':
                    self._check_collection_content(key, field_val, schema_val_decos[DECO_VALIDATE_CONTENT_TYPE])
                elif schema_val_type in ('dict', 'OrderedDict'):
                    self._check_collection_content(key, field_val.values(), schema_val_decos[DECO_VALIDATE_CONTENT_TYPE])
                else:
                    self._check_content(key, field_val, schema_val_decos[DECO_VALIDATE_CONTENT_TYPE])

            if DECO_VALIDATE_CONTENT_COUNT in schema_val_decos and schema_val_type in ('list', 'dict', 'OrderedDict'):
                self._check_collection_length(key, len(field_val), schema_val_decos[DECO_VALIDATE_CONTENT_COUNT])

            if validate_objects:
                # Only if desired - if entire object is walked anyways, these checks are redundant
                if isinstance(field_val, IonObjectBase):
                    field_val._validate()

                # Next validate only IonObjects found in child collections.
                # Note that this is non-recursive; only for first-level collections.
                elif isinstance(field_val, Mapping):
                    for subkey in field_val:
                        subval = field_val[subkey]
                        if isinstance(subval, IonObjectBase):
                            subval._validate()
                elif isinstance(field_val, Iterable):
                    for subval in field_val:
                        if isinstance(subval, IonObjectBase):
                            subval._validate()

    def _get_type(self):
        return self.__class__.__name__

    def _get_extends(self):
        excludes = {'IonObjectBase', 'object', self._get_type()}
        parents = [parent.__name__ for parent in self.__class__.__mro__ if parent.__name__ not in excludes]
        return parents

    def update(self, other):
        """
        Method that allows self object attributes to be updated with other object.
        Other object must be of same type or super type.
        """
        if type(other) != type(self):
            bases = inspect.getmro(self.__class__)
            if other.__class__ not in bases:
                raise BadRequest("Object %s and %s do not have compatible types for update" % (type(self).__name__, type(other).__name__))
        for key in other.__dict__:
            setattr(self, key, other.__dict__[key])

    # --- Decorator methods

    def get_class_decorator_value(self, decorator):
        if getattr(self, '_class_info'):
            if decorator in self._class_info['decorators']:
                return self._class_info['decorators'][decorator]

        return None

    def is_decorator(self, field, decorator):
        """Returns true if schema for given field defines the specified decorator"""
        if decorator in self._schema[field]['decorators']:
            return True

        return False

    def get_decorator_value(self, field, decorator):
        if decorator in self._schema[field]['decorators']:
            return self._schema[field]['decorators'][decorator]

        return None

    def find_field_for_decorator(self, decorator='', decorator_value=None):
        """
        This method will iterate the set of fields in the object and look for the first field
        that has the specified decorator and decorator value, if supplied.
        @param decorator: The decorator on the field to be searched for
        @param decorator_value: An optional value to search on
        @return fld: The name of the field that has the decorator
        """
        for fld in self._schema:
            if self.is_decorator(fld, decorator):
                if decorator_value is not None and self.get_decorator_value(fld, decorator) == decorator_value:
                    return fld
                else:
                    return fld

        return None

    # --- Decorator validation methods

    def _check_string_pattern_match(self, key, value, pattern):
        m = re.match(pattern, value)

        if not m:
            raise AttributeError("Invalid value pattern %s for field '%s.%s', should match regular expression %s" %
                    (value, type(self).__name__, key, pattern))

    def _check_numeric_value_range(self, key, value, value_range):
        value_range_parts = value_range.split(',', 1)
        min_val = ast.literal_eval(value_range_parts[0].strip())
        max_val = ast.literal_eval(value_range_parts[-1].strip())

        if value < min_val or value > max_val:
            raise AttributeError("Invalid value %s for field '%s.%s', should be between %d and %d" %
                (str(value), type(self).__name__, key, min_val, max_val))

    def _check_inheritance_chain(self, typ, expected_type):
        for baseclz in typ.__bases__:
            if baseclz.__name__ == expected_type:
                return True
            if baseclz.__name__ == "object":
                return False
        return False

    def _check_collection_content(self, key, list_values, content_types):
        from pyon.core.registry import issubtype
        split_content_types = {t.strip() for t in content_types.split(',')}

        for value in list_values:
            for content_type in split_content_types:
                # First check for valid ION types
                if isinstance(value, dict) and 'type_' in value:
                    if value['type_'] == content_type or issubtype(value['type_'], content_type):
                        break

                if type(value).__name__ == content_type:
                    break
                # Check for inheritance
                if self._check_inheritance_chain(type(value), content_type):
                    break
            else:
                # No break - no match found
                raise AttributeError("Invalid value type '%s' in collection field '%s.%s', should be one of '%s'" %
                        (value, type(self).__name__, key, content_types))

    def _check_content(self, key, value, content_types):
        split_content_types = {t.strip() for t in content_types.split(',')}

        for content_type in split_content_types:
            if type(value).__name__ == content_type:
                return

            # Check for inheritance
            if self._check_inheritance_chain(type(value), content_type):
                return

        raise AttributeError("Invalid value type %s in field '%s.%s', should be one of '%s'" %
                (str(value), type(self).__name__, key, content_types))

    def _check_collection_length(self, key, len_list, length):
        length_parts = length.split(',', 1)
        min_val = ast.literal_eval(length_parts[0].strip())
        max_val = ast.literal_eval(length_parts[-1].strip())

        if len_list < min_val or len_list > max_val:
            raise AttributeError("Invalid value length for collection field '%s.%s', should be between %d and %d" %
                    (type(self).__name__, key, min_val, max_val))


class IonMessageObjectBase(IonObjectBase):
    """
    Common base class for message object types.
    """
    pass


def walk(o, cb, modify_key_value='value'):
    """
    Utility method to do recursive walking of a possible iterable (incl dicts) and return a
    transformed similar structure.
    Requires a callback, to be called for nested values, returning a transformed value, including
    iterables and dicts. Callback is recursively called for iterable elements and dict elements.

    Dict items will be walked depending on modify_key_value, "key", "value" or "key_value".
    """
    newo = cb(o)

    if isinstance(newo, dict):
        if modify_key_value == 'key':
            return {cb(k): v for k, v in newo.iteritems()}
        elif modify_key_value == 'key_value':
            return {cb(k): walk(v, cb, 'key_value') for k, v in newo.iteritems()}
        else:
            return {k: walk(v, cb) for k, v in newo.iteritems()}
    elif isinstance(newo, (list, tuple, set)):
        return [walk(x, cb, modify_key_value) for x in newo]
    elif isinstance(newo, IonObjectBase):
        # Special case for IonObjects
        fields, set_fields = newo.__dict__, newo._schema

        for fieldname in set_fields:
            fieldval = getattr(newo, fieldname)
            newfo = walk(fieldval, cb, modify_key_value)
            if newfo != fieldval:   # TODO: Comparison here may be expensive
                setattr(newo, fieldname, newfo)
        return newo
    else:
        return newo


class IonObjectSerializationBase(object):
    """
    Base serialization class for serializing/deserializing IonObjects.

    Provides the operate method, which walks and applies a transform method. The operate method is
    renamed serialize/deserialize in derived classes.

    At this base level, the _transform method is undefined - you must pass one in. Using
    IonObjectSerializer or IonObjectDeserializer defines them for you.
    """
    def __init__(self, transform_method=None, **kwargs):
        self._transform_method = transform_method or self._transform

    def operate(self, obj):
        return walk(obj, self._transform_method)

    def _transform(self, obj):
        raise NotImplementedError("Implement _transform in a derived class")


class IonObjectSerializer(IonObjectSerializationBase):
    """
    Serializer for IonObjects; used to encode objects for the datastore.

    Defines a _transform method to turn IonObjects into dictionaries to be deserialized by
    an IonObjectDeserializer.
    """

    def _transform(self, update_version=False):

        def _transform(obj):

            if isinstance(obj, IonObjectBase):
                res = {k:v for k, v in obj.__dict__.iteritems() if k in obj._schema or k in BUILT_IN_ATTRS}
                if not 'type_' in res:
                    res['type_'] = obj._get_type()

                return res

            return obj
        return _transform


    def serialize(self, obj, update_version=False):
        self._transform_method = self._transform(update_version)

        return IonObjectSerializationBase.operate(self, obj)


class IonObjectBlameSerializer(IonObjectSerializer):

    def _transform(self, obj):
        res = IonObjectSerializer._transform(self, obj)
        blame = None
        try:
            blame = os.environ["BLAME"]
        except:
            pass
        if blame and isinstance(obj, IonObjectBase):
            res["blame_"] = blame

        return res


class IonObjectDeserializer(IonObjectSerializationBase):
    """
    Deserializer for IonObjects.

    Defines a _transform method to transform dictionaries produced by IonObjectSerializer back
    into IonObjects. You *MUST* pass an object registry
    """

    deserialize = IonObjectSerializationBase.operate

    def __init__(self, transform_method=None, obj_registry=None, **kwargs):
        assert obj_registry
        self._obj_registry = obj_registry
        IonObjectSerializationBase.__init__(self, transform_method=transform_method)

    def _transform(self, obj):
        # Note: This check to detect an IonObject is a bit risky (only type_)
        if isinstance(obj, dict) and "type_" in obj:
            objc  = obj
            otype = objc['type_'].encode('ascii')   # Correct?

            # don't supply a dict - we want the object to initialize with all its defaults intact,
            # which preserves things like IonEnumObject and invokes the setattr behavior we want there.
            ion_obj = self._obj_registry.new(otype)

            # get outdated attributes in data that are not defined in the current schema
            extra_attributes = objc.viewkeys() - ion_obj._schema.viewkeys() - BUILT_IN_ATTRS
            for extra in extra_attributes:
                objc.pop(extra)
                log.info('discard %s not in current schema' % extra)

            for k, v in objc.iteritems():
                # unicode translate to utf8
                if isinstance(v, unicode):
                    v = str(v.encode('utf8'))
                if k != "type_":
                    setattr(ion_obj, k, v)

            return ion_obj

        return obj


class IonObjectBlameDeserializer(IonObjectDeserializer):

    def _transform(self, obj):

        def handle_ion_obj(in_obj):
            objc    = in_obj.copy()
            type    = objc['type_'].encode('ascii')

            # don't supply a dict - we want the object to initialize with all its defaults intact,
            # which preserves things like IonEnumObject and invokes the setattr behavior we want there.
            ion_obj = self._obj_registry.new(type)
            for k, v in objc.iteritems():
                if k != "type_":
                    setattr(ion_obj, k, v)

            return ion_obj

        # Note: This check to detect an IonObject is a bit risky (only type_)
        if isinstance(obj, dict):
            if "blame_" in obj:
                if "type_" in obj:
                    return handle_ion_obj(obj)
                else:
                    obj.pop("blame_")
            else:
                if "type_" in obj:
                    return handle_ion_obj(obj)

        return obj

ion_serializer = IonObjectSerializer()
