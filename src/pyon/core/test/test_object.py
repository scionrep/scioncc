#!/usr/bin/env python
## coding: utf-8

__author__ = 'Adam R. Smith'

from nose.plugins.attrib import attr
import unittest

from pyon.util.int_test import IonIntegrationTestCase

from pyon.core.registry import IonObjectRegistry
from pyon.core.bootstrap import IonObject


@attr('UNIT')
class ObjectTest(IonIntegrationTestCase):
    def setUp(self):
        self.patch_cfg('pyon.core.bootstrap.CFG', {'container': {'objects': {'validate': {'setattr': True}}}})
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

        # TEST: setattr validation
        obj.name = 'monkey'
        with self.assertRaises(AttributeError):
            obj.extra_field = 5

        # TEST: Validate of object inheritance
        taskable_resource = self.registry.new('TaskableResource')
        taskable_resource.name = "Fooy"
        obj.abstract_val = taskable_resource
        self.assertRaises(AttributeError, obj._validate)
        
        exec_res = self.registry.new('ExecutableResource')
        exec_res.name = "Fooy"
        obj.abstract_val = exec_res
        obj._validate()

        # TEST: Validate of object inheritance in message objects
        from interface.messages import resource_registry_create_in
        msg_obj = resource_registry_create_in()
        msg_obj.object = IonObject("Resource", name="foo")
        msg_obj._validate()

        msg_obj.object = IonObject("InformationResource", name="foo")
        msg_obj._validate()

        msg_obj.object = IonObject("TestInstrument", name="foo")
        msg_obj._validate()

        msg_obj.object = IonObject("Association")
        self.assertRaises(AttributeError, msg_obj._validate)

    def test_bootstrap(self):
        """ Use the factory and singleton from bootstrap.py/public.py """
        obj = IonObject('SampleObject')
        self.assertEqual(obj.name, '')
