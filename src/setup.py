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

version = '0.0.1.dev0'

setup(  name = 'scioncc',
        version = version,
        description = 'Scientific Observatory Network Capability Container',
        long_description = read('../README'),
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
        packages = find_packages(''),   # or ('src')
        #package_dir = {'': 'src'},       # or '': 'src'
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
        ],
        test_suite = 'pyon',
        package_data = {'': ['*.xml']},
        install_requires = [
            'setuptools',
            'pyyaml==3.10',
            'graypy==0.2.11',      # For utilities
            'greenlet==0.4.5',
            'gevent==1.0.1',
            'simplejson==3.6.5',
            'msgpack-python==0.1.13',  # TBD: Check if this specific version is needed
            'pika==0.9.5',             # Messaging stack is tested and working with issues of this version
            'httplib2==0.9',
            'pyzmq==2.2.0',
            'gevent_zeromq==0.2.5',
            'zope.interface==4.1.1',
            'couchdb==0.10',
            'psycopg2==2.5.4',
            'python-daemon==1.6',
            'M2Crypto==0.22.3',
            'nose==1.1.2',
            'ipython==0.13.0',
            'readline==6.2.4.1',
            'mock==0.8',
            'ndg-xacml==0.5.1',
            'requests==2.4.3',
            'psutil==2.1.3',
            'Flask==0.10.1',
            'python-dateutil==2.2',
            'pyparsing==2.0.3',
            # Check if all these are needed
            'ntplib==0.3.2',
            'xlrd==0.9.3',
            'xlwt==0.7.5',
            'antlr_python_runtime==3.1.3',
            'lxml==3.4.0',
            'bcrypt==1.0.1',
            'webtest==2.0.17'    # For service gateway test
        ],
     )
