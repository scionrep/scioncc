#!/bin/bash

# Update/upgrade packages
sudo yum update -y

sudo yum install -y wget git vim

sudo rpm -ivh http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

sudo yum install -y zlib-dev openssl-devel sqlite-devel bzip2-devel
sudo yum groupinstall -y development

# Java
sudo yum install -y java
# Install ant manually (newer version)
# https://mehulbhatt.wordpress.com/2015/03/29/install-apache-ant-on-centos-7/

# PostgreSQL/PostGIS
sudo yum localinstall http://yum.postgresql.org/9.5/redhat/rhel-7-x86_64/pgdg-centos95-9.5-2.noarch.rpm
sudo yum install -y postgresql95 postgresql95-server postgresql95-contrib postgresql95-devel
sudo yum install -y v8 v8-devel plv8_95 postgis2_95 geos

sudo /usr/pgsql-9.5/bin/postgresql95-setup initdb

sudo systemctl enable postgresql-9.5.service
sudo systemctl start postgresql-9.5.service

# RabbitMQ
sudo yum install -y https://www.rabbitmq.com/releases/rabbitmq-server/v3.6.0/rabbitmq-server-3.6.0-1.noarch.rpm
sudo service rabbitmq-server start
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl enable rabbitmq-server

