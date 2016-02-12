#!/bin/bash
set -e
set -x

export PYTHONPATH=".:/usr/lib/python2.7/dist-packages"
cd /home/scion/app/scion_app
source venv/bin/activate
bin/pycc -r res/deploy/scion.yml -c pyon.scion.yml -mx -n
