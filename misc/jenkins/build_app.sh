#!/bin/bash
set -e
set -x

git submodule update --force
git submodule status

virtualenv venv
set +x
source venv/bin/activate
set -x

# Build Egg

sed -i "s/dev0/dev$BUILD_NUMBER/" "$WORKSPACE"/VERSION
SCION_VERSION=$(cat "$WORKSPACE"/VERSION)
export PATH=/usr/pgsql-9.5/bin:$PATH

pip install --upgrade pip
pip install setuptools==20.0

rm -f "$WORKSPACE"/dist/*
/usr/bin/ant clean-buildout

python bootstrap.py -v 2.4.0

bin/buildout

export PYTHONPATH=$(pwd)

bin/generate_interfaces

cp -f res/config/examples/logging.local.yml res/config/
cp -f -T res/config/templates/build_pyon.local.yml res/config/pyon.local.yml

sed -i "s/%SYSNAME%/scionbuild/g" res/config/pyon.local.yml
sed -i "s/%SERVICE_GWY_PORT%/3000/g" res/config/pyon.local.yml
sed -i "s/%ADMIN_UI_PORT%/8080/g" res/config/pyon.local.yml
sed -i "s/%WEB_UI_URL%/http:\/\/scion-dev.ucsd.edu:3000\//g" res/config/pyon.local.yml

sed -i "s/%PG_HOST%/localhost/g" res/config/pyon.local.yml
sed -i "s/%PG_USER%/ion/g" res/config/pyon.local.yml
sed -i "s/%PG_PASSWORD%/abcdef/g" res/config/pyon.local.yml
sed -i "s/%PG_ADMIN_USER%/postgres/g" res/config/pyon.local.yml
sed -i "s/%PG_ADMIN_PASSWORD%/abcdef/g" res/config/pyon.local.yml


#if bin/coverage run bin/nosetests -v --with-xunit; then
#  bin/coverage xml
#  bin/coverage report

if bin/nosetests -v --with-xunit; then
  echo Tests OK.
else
  echo Oops! Exiting...
  exit 1
fi

rm -f res/config/pyon.local.yml

bin/buildout setup . bdist_egg

# Build SciON Egg

GIT_SUBMODULE_STATUS=$(git submodule status | grep scioncc)
SCIONCC_ROOT_DIR=$(echo $GIT_SUBMODULE_STATUS | awk -F'[ ]' '{print $2;}')
SCIONCC_COMMIT_ID=$(echo $GIT_SUBMODULE_STATUS | awk -F'[+ ]' '{print $1;}')
SCIONCC_TAG_ID=$(echo $GIT_SUBMODULE_STATUS | awk -F'[()]' '{print $2;}')

SCIONCC_BUILT_COMMIT_ID=""
[ -f "$WORKSPACE"/scioncc.id ] && SCIONCC_BUILT_COMMIT_ID=$(cat "$WORKSPACE"/scioncc.id)

if [ "$SCIONCC_COMMIT_ID" != "$SCIONCC_BUILT_COMMIT_ID" ]; then

  echo "" > "$WORKSPACE"/scioncc.id
  cd "$WORKSPACE/$SCIONCC_ROOT_DIR"
  mkdir -p logs

  sed -i "s/dev0/dev$BUILD_NUMBER/" "$WORKSPACE/$SCIONCC_ROOT_DIR"/VERSION
  SCIONCC_VERSION=$(cat "$WORKSPACE/$SCIONCC_ROOT_DIR"/VERSION)

  /usr/bin/ant clean-buildout

  python bootstrap.py -v 2.4.0

  bin/buildout

  export PYTHONPATH=$(pwd)

  bin/generate_interfaces
  bin/buildout setup . bdist_egg

  echo $SCIONCC_COMMIT_ID > "$WORKSPACE"/scioncc.id
  cd "$WORKSPACE"

fi


cp -f "$WORKSPACE/$SCIONCC_ROOT_DIR"/dist/* "$WORKSPACE"/dist
ln -sf "$HOME"/.buildout/eggs/z*c.recipe* "$WORKSPACE"/dist
ln -sf "$HOME"/.buildout/eggs/numpy-1.9.2-py2.7-linux-x86_64.egg "$WORKSPACE"/dist
ln -sf "$HOME"/.buildout/eggs/scipy-0.16.0-py2.7-linux-x86_64.egg "$WORKSPACE"/dist
grep "$HOME/.buildout/eggs" bin/pycc | awk -F [/\'] '{print $8;}' | xargs -I{} ln -sf -t "$WORKSPACE"/dist "$HOME"/.buildout/eggs/{}

tar zchf scion_project.tar.gz bootstrap.py buildout.cfg buildout_deploy.cfg defs/res/config
mv scion_project.tar.gz dist

tar zchf scion_build.tar.gz dist
mv scion_build.tar.gz dist