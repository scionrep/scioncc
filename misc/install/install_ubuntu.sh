#!/bin/bash

# Update/upgrade packages
sudo apt-get update
#sudo apt-get upgrade -y

# Common dev packages
sudo apt-get install -y git git-core vim build-essential

# Dependencies for Python
sudo apt-get install -y python-pip python-virtualenv virtualenvwrapper python2.7 python2.7-dev python python-all-dev

# Dependencies for Python common libraries
sudo apt-get install -y libev-dev libffi-dev libpq-dev libxml2-dev libxslt1-dev libzmq-dev ncurses-dev swig

# Dependencies for Python geospatial libraries
sudo apt-get install -y libgeos-dev libgdal-dev gdal-bin libspatialindex-dev

# Dependencies for ant / Java
sudo apt-get install -y ant ant-optional

# Install RabbitMQ
sudo apt-get install -y rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo service rabbitmq-server restart

# Install PostgreSQL - See also: http://www.postgresql.org/download/linux/ubuntu/
sudo sh -c "echo 'deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main' > /etc/apt/sources.list.d/pgdg.list"
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update

sudo apt-get install -y postgresql postgresql-contrib postgis postgresql-9.4-postgis-2.1 postgresql-server-dev-9.4
sudo apt-get install -y postgresql-plpython-9.4 postgresql-9.4-plv8
