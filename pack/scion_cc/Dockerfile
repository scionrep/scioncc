# Use phusion/baseimage as base image with locked version
FROM phusion/baseimage:0.9.16
MAINTAINER Michael Meisinger <michael.meisinger@gmail.com>

ENV HOME=/root BIN_DIR=/root/bin GIT_REV=master CODE_DIR=$HOME/code SCION_DIR=$HOME/code/scioncc
WORKDIR $HOME

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

# Basic stuff
RUN apt-get update && apt-get install -y --no-install-recommends \
        git git-core vim build-essential \
        python-pip python-virtualenv virtualenvwrapper \
        python2.7 python2.7-dev python python-all-dev

# Special libs for psycopg2, readline, lxml, M2Crypto, bcrypt
RUN apt-get update && apt-get install -y --no-install-recommends \
        libev-dev libffi-dev libpq-dev libxml2-dev libxslt1-dev libzmq-dev ncurses-dev swig \
        postgresql-client

# Java based stuff
#RUN apt-get install -y --no-install-recommends ant ant-optional

RUN apt-get update && apt-get upgrade -y

ADD build $HOME
RUN chmod -R +x $BIN_DIR

RUN bash $BIN_DIR/setup_code.sh

RUN bash $BIN_DIR/setup_install.sh

ADD run $HOME
RUN chmod -R +x $BIN_DIR
RUN cp $BIN_DIR/my_init.d/* /etc/my_init.d/

EXPOSE 4000 8080
