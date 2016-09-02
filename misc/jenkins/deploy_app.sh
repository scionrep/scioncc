#!/bin/bash

set -e

SCION_HOME=/export/home/scion
SCION_APP_ROOT=$SCION_HOME/app/scion_app
SCION_APP_LOGS=$SCION_HOME/app/scion_logs

SCION_VERSION=`cat $SCION_APP_ROOT/VERSION`

cd $SCION_APP_ROOT

tar zxf dist/scion_project.tar.gz
rm dist/scion_project.tar.gz
tar zxf dist/scion_build.tar.gz
rm dist/scion_build.tar.gz

cp -f -T defs/res/config/templates/prod_pyon.scion.yml pyon.scion.yml

sed -i "s/%SYSNAME%/scion/g" pyon.scion.yml
sed -i "s/%SERVICE_GWY_PORT%/4000/g" pyon.scion.yml
sed -i "s/%ADMIN_UI_PORT%/9000/g" pyon.scion.yml
sed -i "s/%WEB_UI_URL%/http:\/\/scion-dev.ucsd.edu\//g" pyon.scion.yml

sed -i "s/%AMQP_HOST%/localhost/g" pyon.scion.yml
sed -i "s/%AMQP_PORT%/5672/g" pyon.scion.yml
sed -i "s/%AMQP_USER%/guest/g" pyon.scion.yml
sed -i "s/%AMQP_PASSWORD%/guest/g" pyon.scion.yml
sed -i "s/%AMQP_MPORT%/15672/g" pyon.scion.yml
sed -i "s/%AMQP_MUSER%/guest/g" pyon.scion.yml
sed -i "s/%AMQP_MPASSWORD%/guest/g" pyon.scion.yml

sed -i "s/%PG_HOST%/localhost/g" pyon.scion.yml
sed -i "s/%PG_USER%/ion/g" pyon.scion.yml
sed -i "s/%PG_PASSWORD%/abcdef/g" pyon.scion.yml
sed -i "s/%PG_ADMIN_USER%/postgres/g" pyon.scion.yml
sed -i "s/%PG_ADMIN_PASSWORD%/abcdef/g" pyon.scion.yml

sed -i "s/%DEPLOY_REGION%/default/g" pyon.scion.yml
sed -i "s/%DEPLOY_AZ%/one/g" pyon.scion.yml

#sed -i "s/load_policy: False/load_policy: True/g" pyon.scion.yml
#sed -i "s/interceptor: False/interceptor: True/g" pyon.scion.yml
#sed -i "s/setattr: False/setattr: True/g" pyon.scion.yml

virtualenv venv
set +x
source venv/bin/activate
set -x

pip install --upgrade pip
pip install --upgrade setuptools

python bootstrap.py -v 2.3.1

mkdir -p $SCION_APP_ROOT/eggs
ls -alt $SCION_APP_ROOT/eggs
pwd
find dist/ -type d -maxdepth 1 -mindepth 1 | xargs -i cp -fr {} eggs
ls -alt $SCION_APP_ROOT/eggs

bin/buildout -NU -c buildout_deploy.cfg

#rm -rf $SCION_APP_ROOT/logs
#ln -s $SCION_APP_LOGS logs
ln -s eggs/scion-${SCION_VERSION}-py2.7.egg/defs/objects obj
ln -s eggs/scion-${SCION_VERSION}-py2.7.egg/defs/res res
