#!/bin/bash

set -e
set -x

# Updage packages
sudo apt-get update

# Basic package dependencies
sudo apt-get install -y ant
sudo apt-get install -y ant-optional
sudo apt-get install -y autoconf
sudo apt-get install -y build-essential
sudo apt-get install -y cloc
sudo apt-get install -y erlang-nox
sudo apt-get install -y git
sudo apt-get install -y git-core
sudo apt-get install -y ipython
sudo apt-get install -y libevent-1.4-2
sudo apt-get install -y libevent-dev
sudo apt-get install -y libfreetype6-dev
sudo apt-get install -y libperl5.18
sudo apt-get install -y libpgm-5.1-0
sudo apt-get install -y libssl1.0.0
sudo apt-get install -y libssl-dev
sudo apt-get install -y libtool
sudo apt-get install -y libxml2
sudo apt-get install -y libxml2-dev
sudo apt-get install -y libxslt1-dev
sudo apt-get install -y libyaml-0-2
sudo apt-get install -y libyaml-dev
sudo apt-get install -y libzmq-dev
sudo apt-get install -y libzmq1
sudo apt-get install -y lsof
sudo apt-get install -y most
sudo apt-get install -y ncurses-base
sudo apt-get install -y ncurses-bin
sudo apt-get install -y ncurses-dev
sudo apt-get install -y openssl
sudo apt-get install -y pep8
sudo apt-get install -y pyflakes
sudo apt-get install -y python
sudo apt-get install -y python-all-dev
sudo apt-get install -y python-apt
sudo apt-get install -y python-gevent
sudo apt-get install -y python-greenlet
sudo apt-get install -y python-pip
sudo apt-get install -y python-virtualenv
sudo apt-get install -y python2.7
sudo apt-get install -y python2.7-dev
sudo apt-get install -y python-zmq
sudo apt-get install -y readline-common
sudo apt-get install -y rsync
sudo apt-get install -y screen
sudo apt-get install -y swig
sudo apt-get install -y virtualenvwrapper
sudo apt-get install -y yaml-mode

# Dependencies for Thingworx/Java
sudo apt-get install -y ntp
sudo apt-get install -y authbind
sudo touch /etc/authbind/byport/443
sudo chmod 555 /etc/authbind/byport/443

# Install RabbitMQ
sudo apt-get install -y rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo service rabbitmq-server restart

# Install PostgreSQL
sudo apt-get install -y postgresql postgresql-contrib
sudo apt-get install -y postgis
sudo apt-get install -y postgresql-9.3-postgis-2.1
sudo apt-get install -y postgresql-server-dev-9.3
sudo apt-get install -y postgresql-plpython-9.3

