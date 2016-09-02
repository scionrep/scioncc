#!/bin/bash
set -e
set -x

virtualenv venv
set +x
source venv/bin/activate
set -x

sed -i "s/dev0/dev$BUILD_NUMBER/" VERSION
SCION_VERSION=`cat VERSION`

pip install --upgrade pip
pip install setuptools==20.0

mkdir -p logs

/usr/bin/ant clean-buildout

python bootstrap.py -v 2.3.1

bin/buildout

export PYTHONPATH=$(pwd)

bin/generate_interfaces

cp -f res/config/examples/logging.local.yml res/config
cp -f -T res/config/templates/build_pyon.local.yml res/config/pyon.local.yml

sed -i "s/%SYSNAME%/scioncc/g" res/config/pyon.local.yml
sed -i "s/%SERVICE_GWY_PORT%/3000/g" res/config/pyon.local.yml
sed -i "s/%ADMIN_UI_PORT%/8080/g" res/config/pyon.local.yml
sed -i "s/%WEB_UI_URL%/http:\/\/scion-dev.ucsd.edu:3000\//g" res/config/pyon.local.yml

sed -i "s/%PG_HOST%/localhost/g" res/config/pyon.local.yml
sed -i "s/%PG_USER%/ion/g" res/config/pyon.local.yml
sed -i "s/%PG_PASSWORD%/adcdef/g" res/config/pyon.local.yml
sed -i "s/%PG_ADMIN_USER%/postgres/g" res/config/pyon.local.yml
sed -i "s/%PG_ADMIN_PASSWORD%/abcdef/g" res/config/pyon.local.yml


#if bin/coverage run bin/nosetests -v --with-xunit; then
#  bin/coverage xml
#  bin/coverage report

if bin/nosetests -v --with-xunit; then
  echo Tests OK.
else
  echo Oops! Exiting...
  exit 1
fi

bin/buildout setup . bdist_egg
