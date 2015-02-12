#!/bin/bash
source /usr/share/virtualenvwrapper.sh
cd /home/ubuntu/dev/scion
source /home/ubuntu/.virtualenvs/scion/bin/activate
bin/pycc -fc -n -r res/deploy/basic.yml >/dev/null &
