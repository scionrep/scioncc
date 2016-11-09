#!/bin/bash

# Define shell environment
cat <<EOF >> ~/.bash_profile
export HOME=/root
export PYTHONPATH=.
export WORKON_HOME='~/.virtualenvs'
source /usr/share/virtualenvwrapper/virtualenvwrapper.sh
workon scion
cd
EOF

# Initialize virtualenv for the first time
export WORKON_HOME='/root/.virtualenvs'
source /usr/share/virtualenvwrapper/virtualenvwrapper.sh

mkvirtualenv scion

source ~/.bash_profile

# Create virtualenv
workon scion
pip install setuptools --upgrade
pip install pip --upgrade

mkdir -p ~/.buildout/eggs ~/.buildout/dlcache
printf "[buildout]\neggs-directory=$HOME/.buildout/eggs\ndownload-cache=$HOME/.buildout/dlcache\n" > ~/.buildout/default.cfg

# Get code
mkdir -p code
cd code
mkdir -p logs

git clone https://github.com/scionrep/scioncc
cd scioncc
git checkout ${GIT_REV:-master}
