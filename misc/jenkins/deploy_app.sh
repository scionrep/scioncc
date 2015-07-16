#!/bin/bash

set -e

APP_HOME=/home/app
APP_ROOT=$APP_HOME/app/app
APP_VERSION=`cat $APP_ROOT/VERSION`

cd $APP_ROOT

cp -f -T config/pyon.app.yml.template pyon.app.yml

sed -i "s/%SYSNAME%/dev/g" pyon.app.yml
sed -i "s/%WEB_UI_URL%/http:\/\/server.com\:8080\//g" pyon.app.yml

sed -i "s/%AMQP_HOST%/localhost/g" pyon.app.yml
sed -i "s/%AMQP_PORT%/5672/g" pyon.app.yml
sed -i "s/%AMQP_USER%/guest/g" pyon.app.yml
sed -i "s/%AMQP_PASSWORD%/guest/g" pyon.app.yml

sed -i "s/%AMQP_MPORT%/15672/g" pyon.app.yml
sed -i "s/%AMQP_MUSER%/guest/g" pyon.app.yml
sed -i "s/%AMQP_MPASSWORD%/guest/g" pyon.app.yml

sed -i "s/%PG_HOST%/dev-rds.abcdefg.us-east-1.rds.amazonaws.com/g" pyon.app.yml
sed -i "s/%PG_USER%/ion/g" pyon.app.yml
sed -i "s/%PG_PASSWORD%/abcdefg/g" pyon.app.yml
sed -i "s/%PG_ADMIN_USER%/master/g" pyon.app.yml
sed -i "s/%PG_ADMIN_PASSWORD%/abcdefg/g" pyon.app.yml

sed  -i "s/%PG_ADMIN_PASSWORD%/abcdefg/g" pyon.app.yml

sed -i "s/%ADMIN_UI_SERVER_PORT%/9000/g" pyon.app.yml

sed -i "s/%SMTP_HOST%/localhost/g" pyon.app.yml
sed -i "s/%SMTP_SENDER%/alerts@def.app.com/g" pyon.app.yml
sed -i "s/%SMTP_USER%//g" pyon.app.yml
sed -i "s/%SMTP_PSWD%//g" pyon.app.yml

sed -i "s/interceptor: False/interceptor: True/g" pyon.app.yml
sed -i "s/setattr: False/setattr: True/g" pyon.app.yml

virtualenv venv
set +x
source venv/bin/activate
set -x

pip install --upgrade pip
pip install --upgrade setuptools

python bootstrap.py -v 2.3.1

bin/buildout -N

ln -s eggs/app-${APP_VERSION}-py2.7.egg/defs/objects obj
ln -s eggs/app-${APP_VERSION}-py2.7.egg/defs/res res
ln -s ../app_ui/src ui
