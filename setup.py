#!/usr/bin/env python

from setuptools import setup, find_packages

import os
import sys

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_data_dirs(path, patterns):
    data_dirs = [(rt+"/"+dn+"/") for rt, ds, fs in os.walk(path) for dn in ds]
    data_dir_patterns = []
    for pat in patterns:
        data_dir_patterns += [(dn+pat)[len(path)+1:] for dn in data_dirs]
    return data_dir_patterns

# Add /usr/local/include to the path for macs, fixes easy_install for several packages (like gevent and pyyaml)
if sys.platform == 'darwin':
    os.environ['C_INCLUDE_PATH'] = '/usr/local/include'

VERSION = read("VERSION").strip()

# See http://pythonhosted.org/setuptools/setuptools.html
setup(  name='scioncc',
        version=VERSION,
        description='Scientific Observatory Network Capability Container',
        long_description=read('README'),
        url='https://www.github.com/scionrep/scioncc/wiki',
        download_url='https://github.com/scionrep/scioncc/releases',
        license='BSD',
        author='SciON Contributors',
        author_email='michael.meisinger@gmail.com',
        keywords=['scion', 'pyon', 'ion'],
        classifiers=['Programming Language :: Python',
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
        packages=find_packages('src') + find_packages('.'),
        package_dir={'': 'src',
                     'interface': 'interface',
                     'defs': 'defs'},
        include_package_data=True,
        package_data={
            '': ['*.yml', '*.txt'] + get_data_dirs("defs", ["*.yml", "*.sql", "*.xml"]),
        },
        entry_points={
            'console_scripts': [
                'pycc=scripts.pycc:entry',
                'control_cc=scripts.control_cc:main',
                'generate_interfaces=scripts.generate_interfaces:main',
                'store_interfaces=scripts.store_interfaces:main',
                'clear_db=pyon.datastore.clear_db_util:main',
                ]
            },
        dependency_links=[],
        install_requires=[
            'setuptools',
            'greenlet==0.4.5',
            'gevent==1.0.1',
            'pyyaml==3.10',
            'simplejson==3.6.5',
            'msgpack-python==0.1.13',  # TBD: Check if this specific version is needed
            'pika==0.9.5',             # Messaging stack is tested and working with issues of this version
            'httplib2==0.9',
            'zope.interface==4.1.1',
            'psycopg2==2.5.4',
            'numpy==1.9.1',
            'python-daemon==2.0.5',
            'ipython==0.13.0',
            'readline==6.2.4.1',
            'ndg-xacml==0.5.1',        # For policy rule engine
            'requests==2.4.3',
            'psutil==2.1.3',
            'Flask==0.10.1',
            'flask-socketio==0.4.1',
            'python-dateutil==2.2',
            'bcrypt==1.0.1',           # For password authentication
            'lovely.buildouthttp==0.6.1',    # For buildout
            'pyzmq==2.2.0',            # For IPython manhole
            'gevent_zeromq==0.2.5',

            # Test support
            'nose==1.1.2',
            'mock==0.8',
            'webtest==2.0.17',         # For service gateway test

            # Check if all these are needed
            'graypy==0.2.11',          # For production logging
            'ntplib==0.3.2',
            'pyproj==1.9.4'            # For geospatial calculations
            #'M2Crypto==0.22.3',        # For X.509 certificates (currently unused)
        ],
        extras_require={
            'scidata': [
                'Pydap==3.3.RC1',
                'netCDF4==1.0.9',
            ],
            'utils': [
                'xlrd==0.9.3',         # For Excel file read (dev tools)
                'xlwt==0.7.5',         # For Excel file write (dev tools)
            ],
            'parsing': [
                'lxml==3.4.2',
                'beautifulsoup4==4.3.2',
            ],
        }
     )
