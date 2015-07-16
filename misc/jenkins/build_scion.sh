#!/bin/bash
set -e
set -x

virtualenv venv
set +x
source venv/bin/activate
set -x

sed -i "s/dev0/dev$BUILD_NUMBER/" VERSION
SCIONCC_VERSION=`cat VERSION`

pip install --upgrade pip
pip install --upgrade setuptools

/var/lib/jenkins/tools/hudson.tasks.Ant_AntInstallation/1.9.4/bin/ant clean-buildout

python bootstrap.py -v 2.3.1

bin/buildout

export PYTHONPATH=$(pwd)

bin/generate_interfaces

cp -f res/config/examples/* res/config

sed -i "s/ password:$/ password: abcdefg/" res/config/pyon.local.yml
sed -i "s/admin_password:/admin_password: abcdefg/" res/config/pyon.local.yml
sed -i "s/admin_username:/admin_username: postgres/" res/config/pyon.local.yml

if bin/coverage run bin/nosetests -v --with-xunit; then
  bin/coverage xml
  bin/coverage report
else
  echo Oops! Exiting...
  exit 1
fi

bin/buildout setup . bdist_egg

