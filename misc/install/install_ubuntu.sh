#!/bin/bash

# Update packages
sudo apt-get update

# Dependencies for Python and packages
sudo apt-get install -y --no-install-recommends git git-core vim build-essential

sudo apt-get install -y --no-install-recommends python-pip python-virtualenv virtualenvwrapper python2.7 python2.7-dev python python-all-dev

sudo apt-get install -y --no-install-recommends libev-dev libffi-dev libpq-dev libxml2-dev libxslt1-dev libzmq-dev ncurses-dev swig

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

sudo apt-get install -y postgresql postgresql-contrib
sudo apt-get install -y postgis
sudo apt-get install -y postgresql-9.4-postgis-2.1
sudo apt-get install -y postgresql-server-dev-9.4
sudo apt-get install -y postgresql-plpython-9.4

sudo apt-get update
#sudo apt-get upgrade -y
