#!/usr/bin/env python
## coding: utf-8

__author__ = 'Adam R. Smith'


from pyon.core.registry import IonObjectRegistry
from pyon.core.bootstrap import IonObject
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
import unittest


@attr('UNIT')
class ObjectTest(IonIntegrationTestCase):
    def setUp(self):
        self.patch_cfg('pyon.core.bootstrap.CFG', {'container':{'objects':{'validate':{'setattr': True}}}})
        self.registry = IonObjectRegistry()

    def test_new(self):
        obj = self.registry.new('SampleObject')

        self.assertEqual(obj.name, '')
        self.assertEqual(obj.time, "1341269890404")

    def test_validate(self):

        obj = self.registry.new('SampleObject')
        self.name = 'monkey'
        self.int = 1
        obj._validate()

        obj.name = 3
        self.assertRaises(AttributeError, obj._validate)

        obj.name = 'monkey'
        assignment_failed = False
        try:
            obj.extra_field = 5
        except AttributeError:
            assignment_failed = True
        self.assertTrue(assignment_failed)
        
        taskable_resource = self.registry.new('TaskableResource')
        taskable_resource.name = "Fooy"
        obj.abstract_val = taskable_resource
        self.assertRaises(AttributeError, obj._validate)
        
        user_info = self.registry.new('ActorIdentity')
        user_info.name = "Fooy"
        obj.abstract_val = user_info
        obj._validate

    @unittest.skip("no more recursive encoding on set")
    def test_recursive_encoding(self):
        obj = self.registry.new('SampleObject')
        a_dict = {'1':u"♣ Temporal Domain ♥",
                  u'2Ĕ':u"A test data product Ĕ ∆",
                  3:{'1':u"♣ Temporal Domain ♥", u'2Ĕ':u"A test data product Ĕ ∆",
                        4:[u"♣ Temporal Domain ♥", {u'2Ĕ':u'one', 1:u"A test data product Ĕ ∆"}]},
                  'Four': u'४',
                  u'४': 'Four',
                  6:{u'1':'Temporal Domain', u'2Ĕ':u"A test data product Ĕ ∆",
                        4:[u"♣ Temporal Domain ♥", {u'४':'one', 1:'A test data product'}]}}

        type_str = type('a string')
        type_inner_element = type(a_dict[3][4][1][1])
        type_another_element = type(a_dict[6][4][1][1])
        top_level_element = a_dict['Four']
        type_top_level_element = type(top_level_element)


        # check that the type of the innermost element is not string originally
        self.assertNotEqual(type_inner_element, type_str)
        # check that the type of the innermost element is originally str
        self.assertEqual(type('a string'), type_another_element)
        # check that the type of the innermost element is not string originally
        self.assertNotEqual(type_top_level_element, type_str)
        # check that a unicode element isn't utf-8 encoded
        self.assertNotEqual(top_level_element,'\xe0\xa5\xaa')

        # apply recursive encoding
        obj.a_dict = a_dict

        # check types of the innermost elements
        type_inner_element = type(obj.a_dict[3][4][1][1])
        type_remains_str = type(obj.a_dict[6][4][1][1])

        # check that the type of the first innermost element is now type str
        self.assertEqual(type_inner_element, type_str)
        # check that the type of the other innermost element remains str
        self.assertEqual(type_another_element, type_remains_str)

        # check that a unicode element did get utf-8 encoded
        self.assertEqual(obj.a_dict['Four'],'\xe0\xa5\xaa')



    def test_bootstrap(self):
        """ Use the factory and singleton from bootstrap.py/public.py """
        obj = IonObject('SampleObject')
        self.assertEqual(obj.name, '')
