#!/bin/bash
set -e
set -x

git submodule update --force
git submodule status

virtualenv venv
set +x
source venv/bin/activate
set -x

# Build App Egg

sed -i "s/dev0/dev$BUILD_NUMBER/" "$WORKSPACE"/VERSION
APP_VERSION=$(cat "$WORKSPACE"/VERSION)

pip install --upgrade pip
pip install --upgrade setuptools

rm "$WORKSPACE"/dist/*
/var/lib/jenkins/tools/hudson.tasks.Ant_AntInstallation/1.9.4/bin/ant clean-buildout

python bootstrap.py -v 2.4.0

bin/buildout

export PYTHONPATH=$(pwd)

bin/generate_interfaces

cp -f res/config/examples/* res/config

sed -i "s/ password:$/ password: abcdefg/" res/config/pyon.local.yml
sed -i "s/admin_password:/admin_password: abcdefg/" res/config/pyon.local.yml
sed -i "s/admin_username:/admin_username: postgres/" res/config/pyon.local.yml


if bin/coverage run bin/nosetests -v --with-xunit; then
  bin/coverage xml
  bin/coverage report
else
  echo Oops! Exiting...
  exit 1
fi

bin/buildout setup . bdist_egg

# Build SciON Egg

GIT_SUBMODULE_STATUS=$(git submodule status | grep scion)
SCION_ROOT_DIR=$(echo $GIT_SUBMODULE_STATUS | awk -F'[ ]' '{print $2;}')
SCION_COMMIT_ID=$(echo $GIT_SUBMODULE_STATUS | awk -F'[+ ]' '{print $1;}')
SCION_TAG_ID=$(echo $GIT_SUBMODULE_STATUS | awk -F'[()]' '{print $2;}')

SCION_BUILT_COMMIT_ID=""
[ -f "$WORKSPACE"/scion.id ] && SCION_BUILT_COMMIT_ID=$(cat "$WORKSPACE"/scion.id)

if [ "$SCION_COMMIT_ID" != "$SCION_BUILT_COMMIT_ID" ]; then

  echo "" > "$WORKSPACE"/scion.id
  cd "$WORKSPACE/$SCION_ROOT_DIR"
  mkdir -p logs

  sed -i "s/dev0/dev$BUILD_NUMBER/" "$WORKSPACE/$SCION_ROOT_DIR"/VERSION
  SCION_VERSION=$(cat "$WORKSPACE/$SCION_ROOT_DIR"/VERSION)

  /var/lib/jenkins/tools/hudson.tasks.Ant_AntInstallation/1.9.4/bin/ant clean-buildout

  python bootstrap.py -v 2.4.0

  bin/buildout

  export PYTHONPATH=$(pwd)

  bin/generate_interfaces
  bin/buildout setup . bdist_egg

  echo $SCION_COMMIT_ID > "$WORKSPACE"/scion.id
  cd "$WORKSPACE"

fi

ln -sf "$WORKSPACE/$SCION_ROOT_DIR"/dist/* "$WORKSPACE"/dist
ln -sf "$HOME"/.buildout/eggs/z*c.recipe* "$WORKSPACE"/dist
grep "$HOME/.buildout/eggs" bin/pycc | awk -F [/\'] '{print $8;}' | xargs -I{} ln -sf -t "$WORKSPACE"/dist "$HOME"/.buildout/eggs/{}

