#!/usr/bin/env python

'''
@file prototype/hdf/test/test_hdf_array_iterator.py
@author Swarbhanu Chatterjee
@test prototype.hdf.test.test_hdf_array_iterator.py test suite for hdf_array_iterator.py
'''

import os
from prototype.hdf.hdf_array_iterator import acquire_data

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest

@attr('UNIT', group='dm')
class HDFArrayIteratorTest(IonIntegrationTestCase):

    def setUp(self):

        import h5py, numpy

        #--------------------------------------------------------------------
        # Create an hdf file for testing
        #--------------------------------------------------------------------

        self.salinity = numpy.arange(50)

        file = h5py.File('data.hdf5', 'w')

        dset = file.create_dataset("salinity", data=self.salinity)

        file.close()

        #--------------------------------------------------------------------
        # Create another file for testing
        #--------------------------------------------------------------------

        self.temp = numpy.arange(30)

        self.cond = numpy.arange(100)

        file = h5py.File('measurements.hdf5', 'w')

        dset = file.create_dataset("temperature", data=self.temp)
        dset2 = file.create_dataset("conductivity", data = self.cond)

        file.close()

        #--------------------------------------------------------------------
        # Create a file for testing recursively searching down the group tree in hdf files
        # seeking out_dict the datasets
        #--------------------------------------------------------------------

        altitude = numpy.arange(30)

        depth = numpy.arange(100)

        file = h5py.File('recursive_searching.hdf5', 'w')

        grp1 = file.create_group('fields')
        grp2 = file.create_group('other_fields')

        dset3 = grp1.create_dataset("altitude", data=altitude)
        dset4 = grp2.create_dataset("depth", data = depth)

        file.close()


    def tearDown(self):
        """
        Cleanup. Delete Subscription, Stream, Process Definition
        """

        os.remove('measurements.hdf5')
        os.remove('data.hdf5')
        os.remove('recursive_searching.hdf5')

    def test_acquire_data_from_multiple_files(self):
        """
        Test whether data can be acquired from multiple hdf5 files
        """

        generator = acquire_data(hdf_files = ['data.hdf5','measurements.hdf5'],
                                var_names = ['salinity', 'temperature'],
                                concatenate_size = 50,
                                bounds = None
                                )

        out_dict = generator.next()
        # assert that the dataset 'salinity' in the first hdf5 file has been opened
        self.assertEquals( str(out_dict['salinity']['values']), str(self.salinity) )

        out_dict = generator.next()
        # assert that the second hdf5 file has been opened and its data read
        self.assertEquals( str(out_dict['temperature']['values']), str(self.temp) )

    def test_acquire_data_from_multiple_datasets(self):
        """
        Test whether data can be acquired from multiple datasets from an hdf5 file
        """

        concatenate_size = 50

        generator = acquire_data(hdf_files = ['measurements.hdf5'],
                                     var_names = ['temperature', 'conductivity'],
                                     concatenate_size = concatenate_size,
                                     bounds = None
                                )

        out_dict = generator.next() # the first time next() is called, it loads up the temperature data.

        # assert that the dataset 'salinity' in the first hdf5 file has been opened
        self.assertEquals( str(out_dict['concatenated_array'][:self.temp.size]), str(self.temp) )

        #---------------------------------------------------

        out_dict = generator.next() # this time, the second dataset, conductivity, should get read

        self.assertEquals( str(out_dict['concatenated_array']), str(self.cond[ : concatenate_size]) )

    def test_concatenate_size(self):
        """
        Test that the chunk of data that is read from the hdf file is of the size buffer_size
        """

        concatenate_size = 20

        generator = acquire_data(hdf_files = ['measurements.hdf5'],
            var_names = ['temperature', 'conductivity'],
            concatenate_size = concatenate_size,
            bounds = None
        )

        out_dict = generator.next()

        self.assertEquals( out_dict['concatenated_array'].size, concatenate_size)

    def test_larger_than_normal_concatenate_size(self):
        """
        Test that providing a very large buffer size is okay
        """

        generator = acquire_data(hdf_files = ['measurements.hdf5'],
            var_names = ['temperature', 'conductivity'],
            concatenate_size = 500,
            bounds = None
        )

        out_dict = generator.next()
        self.assertEquals( str(out_dict['concatenated_array']), str(self.temp) )

        #-----------------------------------------

        out_dict = generator.next()
        self.assertEquals( str(out_dict['concatenated_array']), str(self.cond) )

    def test_bounds(self):
        """
        Test that providing an arbitrary slice works
        """

        generator = acquire_data(hdf_files = ['measurements.hdf5'],
            var_names = ['temperature'],
            concatenate_size = 50,
            bounds = (0,10)
        )
        #------------------------------------------------------------------------------------------------
        # call next() once.....
        #------------------------------------------------------------------------------------------------

        out_dict = generator.next()

        print out_dict

        arrayOut1 = out_dict['concatenated_array']
        array2 = self.temp [0:10]

        # note that the bounds is inclusive, so the 10th element is meant to be read
        self.assertEquals(str(arrayOut1), str(array2))


    def test_bounds_small_concatenate_size(self):
        """
        Test that providing an arbitrary slice works
        """

        generator = acquire_data(hdf_files = ['measurements.hdf5'],
            var_names = ['temperature'],
            concatenate_size = 5,
            bounds = (0,10)
        )
        #------------------------------------------------------------------------------------------------
        # call next() once.....
        #------------------------------------------------------------------------------------------------

        out_dict = generator.next()

        print out_dict

        arrayOut1 = out_dict['concatenated_array']
        array2 = self.temp [0:5]

        # note that the bounds is inclusive, so the 10th element is meant to be read
        self.assertEquals(str(arrayOut1), str(array2))

        #------------------------------------------------------

        out_dict = generator.next()

        print out_dict

        arrayOut2 = out_dict['concatenated_array']
        array2 = self.temp [5:10]

        # note that the bounds is inclusive, so the 10th element is meant to be read
        self.assertEquals(str(arrayOut2), str(array2))

