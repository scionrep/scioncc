#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

import os
import sys

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# Add /usr/local/include to the path for macs, fixes easy_install for several packages (like gevent and pyyaml)
if sys.platform == 'darwin':
    os.environ['C_INCLUDE_PATH'] = '/usr/local/include'

version = '0.0.1-dev'

setup(  name = 'scioncc',
        version = version,
        description = 'Scientific Observatory Network Capability Container',
        long_description = read('README'),
        url = 'www.github.com/scionrep/scioncc',
        download_url = 'https://github.com/scionrep/scioncc/releases',
        license = 'BSD',
        author = 'SciON Contributors',
        author_email = 'michael.meisinger@gmail.com',
        keywords = ['scion', 'pyon', 'ion'],
        classifiers = ['Programming Language :: Python',
                       'Programming Language :: Python :: 2.7',
                       'License :: OSI Approved :: BSD License',
                       'Operating System :: OS Independent',
                       'Development Status :: 5 - Production/Stable',
                       'Intended Audience :: Developers',
                       'Environment :: Web Environment',
                       'Topic :: Internet',
                       'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
                       'Topic :: Scientific/Engineering',
                       'Topic :: Software Development',
                       'Topic :: Software Development :: Libraries :: Application Frameworks'],
        packages = find_packages(),   # or ('src')
        #package_dir = {'': ''},       # or '': 'src'
        entry_points = {
            'console_scripts' : [
                'pycc=scripts.pycc:entry',
                'control_cc=scripts.control_cc:main',
                'generate_interfaces=scripts.generate_interfaces:main',
                'store_interfaces=scripts.store_interfaces:main',
                'clear_db=pyon.datastore.clear_couch_util:main',
                ]
            },
        dependency_links = [
            'http://sddevrepo.oceanobservatories.org/releases/',
            'https://github.com/ooici/utilities/tarball/v2013.06.11#egg=utilities-2013.06.11',
        ],
        test_suite = 'pyon',
        package_data = {'': ['*.xml']},
        install_requires = [
            'setuptools',
            'utilities',
            'greenlet==0.4.0',     # We need the greenlet version matching gevent 0.13.8
            'gevent',              # Don't put == version on gevent so that buuldout.cfg can set it
            'simplejson',
            'msgpack-python==0.1.13',  # TBD: Check if this specific version is needed
            'pika==0.9.5',         # Messaging stack is tested and working with issues of this version
            'httplib2>=0.7.2',
            'pyzmq==2.2.0',
            'gevent_zeromq==0.2.5',
            'zope.interface',
            'couchdb==0.9',
            'psycopg2',
            'python-daemon==1.6',
            'M2Crypto',
            'nose==1.1.2',         # TBD: Check if this specific version is needed
            'ipython==0.13.0',     # TBD: Check if this specific version is needed
            'readline',
            'mock==0.8',           # TBD: Check if this specific version is needed
            'ndg-xacml==0.5.1',
            'requests',
            'psutil',
            'Flask',
            'python-dateutil',
            'pyparsing',
            # Check if all these are needed
            'ntplib',
            'xlrd',
            'xlwt',
            'antlr_python_runtime==3.1.3',
            'lxml',
        ],
     )
