#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr

from pyon.public import BadRequest
from pyon.util.unit_test import UnitTestCase

from ion.util.parse_utils import get_typed_value


@attr('UNIT')
class TestParseUtils(UnitTestCase):

    def test_get_typed_value(self):
        # TEST: Integers
        ret_val = get_typed_value(999, targettype="int", strict=True)
        self.assertEqual(ret_val, 999)
        with self.assertRaises(BadRequest):
            get_typed_value("999", targettype="int", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value("999.9", targettype="int", strict=False)
        with self.assertRaises(BadRequest):
            get_typed_value(None, targettype="int", strict=False)
        with self.assertRaises(BadRequest):
            get_typed_value("", targettype="int", strict=False)
        ret_val = get_typed_value("999", targettype="int", strict=False)
        self.assertEqual(ret_val, 999)
        long_val = 9999999999999999999
        self.assertEqual(type(long_val), long)
        ret_val = get_typed_value(long_val, targettype="int", strict=True)
        self.assertEqual(ret_val, long_val)

        schema_entry = dict(type="int")
        ret_val = get_typed_value(999, schema_entry=schema_entry, strict=True)
        self.assertEqual(ret_val, 999)

        # TEST: Float
        ret_val = get_typed_value(999.9, targettype="float", strict=True)
        self.assertEqual(ret_val, 999.9)
        with self.assertRaises(BadRequest):
            get_typed_value("999.9", targettype="float", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value(None, targettype="float", strict=False)
        with self.assertRaises(BadRequest):
            get_typed_value("", targettype="float", strict=False)
        ret_val = get_typed_value("999.9", targettype="float", strict=False)
        self.assertEqual(ret_val, 999.9)

        # TEST: String
        ret_val = get_typed_value("foo", targettype="str", strict=True)
        self.assertEqual(ret_val, "foo")
        with self.assertRaises(BadRequest):
            get_typed_value(999, targettype="str", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value(None, targettype="str", strict=True)
        ret_val = get_typed_value(999, targettype="str", strict=False)
        self.assertEqual(ret_val, "999")
        ret_val = get_typed_value(True, targettype="str", strict=False)
        self.assertEqual(ret_val, "True")
        unicode_val = u'foo \u20ac foo'
        ret_val = get_typed_value(unicode_val, targettype="str", strict=True)
        self.assertEqual(type(ret_val), str)
        self.assertEqual(ret_val, "foo \xe2\x82\xac foo")

        # TEST: Bool
        ret_val = get_typed_value(True, targettype="bool", strict=True)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value(False, targettype="bool", strict=True)
        self.assertEqual(ret_val, False)
        with self.assertRaises(BadRequest):
            get_typed_value("True", targettype="bool", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value(None, targettype="bool", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value("", targettype="bool", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value(123, targettype="bool", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value(0, targettype="bool", strict=True)
        ret_val = get_typed_value("True", targettype="bool", strict=False)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value("true", targettype="bool", strict=False)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value("TRUE", targettype="bool", strict=False)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value("1", targettype="bool", strict=False)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value(1, targettype="bool", strict=False)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value("False", targettype="bool", strict=False)
        self.assertEqual(ret_val, False)
        ret_val = get_typed_value("FALSE", targettype="bool", strict=False)
        self.assertEqual(ret_val, False)
        ret_val = get_typed_value("false", targettype="bool", strict=False)
        self.assertEqual(ret_val, False)
        ret_val = get_typed_value("0", targettype="bool", strict=False)
        self.assertEqual(ret_val, False)
        ret_val = get_typed_value("", targettype="bool", strict=False)
        self.assertEqual(ret_val, False)
        ret_val = get_typed_value(None, targettype="bool", strict=False)
        self.assertEqual(ret_val, False)
        with self.assertRaises(BadRequest):
            get_typed_value("F", targettype="bool", strict=False)
        with self.assertRaises(BadRequest):
            get_typed_value("Falsy", targettype="bool", strict=False)
        with self.assertRaises(BadRequest):
            get_typed_value("Truey", targettype="bool", strict=False)
        with self.assertRaises(BadRequest):
            get_typed_value(" True", targettype="bool", strict=False)

        # TEST: List
        list_val = [1, True, "foo"]
        ret_val = get_typed_value(list_val, targettype="list", strict=True)
        self.assertEqual(ret_val, list_val)
        ret_val = get_typed_value([], targettype="list", strict=True)
        self.assertEqual(ret_val, [])
        with self.assertRaises(BadRequest):
            get_typed_value(None, targettype="list", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value("[]", targettype="list", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value("", targettype="list", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value(tuple(), targettype="list", strict=True)

        ret_val = get_typed_value(1, targettype="list", strict=False)
        self.assertEqual(ret_val, [1])
        ret_val = get_typed_value(tuple(list_val), targettype="list", strict=False)
        self.assertEqual(ret_val, list_val)
        ret_val = get_typed_value(set(list_val), targettype="list", strict=False)
        self.assertEqual(type(ret_val), list)
        self.assertEqual(set(ret_val), set(list_val))
        ret_val = get_typed_value("1", targettype="list", strict=False)
        self.assertEqual(ret_val, ["1"])
        ret_val = get_typed_value("a,b,c", targettype="list", strict=False)
        self.assertEqual(ret_val, ["a", "b", "c"])
        ret_val = get_typed_value("[a,b,c]", targettype="list", strict=False)
        self.assertEqual(ret_val, ["a", "b", "c"])
        ret_val = get_typed_value("['a','b',3]", targettype="list", strict=False)
        self.assertEqual(ret_val, ["a", "b", 3])
        ret_val = get_typed_value("[]", targettype="list", strict=False)
        self.assertEqual(ret_val, [])
        ret_val = get_typed_value(None, targettype="list", strict=False)
        self.assertEqual(ret_val, [None])
        ret_val = get_typed_value(True, targettype="list", strict=False)
        self.assertEqual(ret_val, [True])

        # TEST: Simplelist
        ret_val = get_typed_value("a,b,c", targettype="simplelist")
        self.assertEqual(ret_val, ["a", "b", "c"])

        # TEST: Dict
        dict_val = {'a': 1, 'b': True}
        ret_val = get_typed_value(dict_val, targettype="dict", strict=True)
        self.assertEqual(ret_val, dict_val)
        ret_val = get_typed_value({}, targettype="dict", strict=True)
        self.assertEqual(ret_val, {})
        with self.assertRaises(BadRequest):
            get_typed_value(None, targettype="dict", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value("{}", targettype="dict", strict=True)
        with self.assertRaises(BadRequest):
            get_typed_value("", targettype="dict", strict=True)

        ret_val = get_typed_value("{}", targettype="dict", strict=False)
        self.assertEqual(ret_val, {})
        ret_val = get_typed_value("{'a': 1, 'b': True}", targettype="dict", strict=False)
        self.assertEqual(ret_val, dict_val)
        ret_val = get_typed_value("a: 1, b: c, c: True", targettype="dict", strict=False)
        self.assertEqual(ret_val, {'a': 1, 'b': 'c', 'c': True})
        ret_val = get_typed_value("a.x: 1, a.y: 2.2, b: false", targettype="dict", strict=False)
        self.assertEqual(ret_val, {'a': {'x': 1, 'y': 2.2}, 'b': 'false'})

        # TEST: None
        ret_val = get_typed_value(None, targettype="NoneType", strict=True)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value(1, targettype="NoneType", strict=True)
        self.assertEqual(ret_val, 1)
        ret_val = get_typed_value(True, targettype="NoneType", strict=True)
        self.assertEqual(ret_val, True)
        ret_val = get_typed_value("foo", targettype="NoneType", strict=True)
        self.assertEqual(ret_val, "foo")
        ret_val = get_typed_value("None", targettype="NoneType", strict=True)
        self.assertEqual(ret_val, "None")

        ret_val = get_typed_value("None", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value("NONE", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value("none", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value("Null", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value("NULL", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value("null", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)
        ret_val = get_typed_value("", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, None)

        ret_val = get_typed_value(1, targettype="NoneType", strict=False)
        self.assertEqual(ret_val, 1)
        ret_val = get_typed_value("1", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, 1)
        ret_val = get_typed_value(1.1, targettype="NoneType", strict=False)
        self.assertEqual(ret_val, 1.1)
        ret_val = get_typed_value("1.1", targettype="NoneType", strict=False)
        self.assertEqual(ret_val, 1.1)

        # TEST: Enum
        from interface.objects import SampleEnum
        schema_entry = dict(type="int", enum_type="SampleEnum")
        ret_val = get_typed_value(SampleEnum.MONDAY, schema_entry=schema_entry, strict=True)
        self.assertEqual(ret_val, SampleEnum.MONDAY)
        ret_val = get_typed_value("MONDAY", schema_entry=schema_entry, strict=True)
        self.assertEqual(ret_val, SampleEnum.MONDAY)
        with self.assertRaises(BadRequest):
            get_typed_value("Monday", schema_entry=schema_entry, strict=True)

        ret_val = get_typed_value("MONDAY", schema_entry=schema_entry, strict=False)
        self.assertEqual(ret_val, SampleEnum.MONDAY)
        ret_val = get_typed_value("Monday", schema_entry=schema_entry, strict=False)
        self.assertEqual(ret_val, SampleEnum.MONDAY)
        with self.assertRaises(BadRequest):
            get_typed_value("HOLIDAY", schema_entry=schema_entry)

        # TEST: Error conditions
        with self.assertRaises(BadRequest):
            get_typed_value(1)
        with self.assertRaises(BadRequest):
            get_typed_value(1, targettype="FOO")
