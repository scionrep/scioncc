#!/bin/bash

echo "=== PREPARE CONTAINER ==="
source ~/.bash_profile

# Make a pyon.local.yml config file with defaults from ENV

sed "s/%SYSNAME%/${SYSNAME:-scion}/g;\
s/%AMQP_HOST%/${AMQP_HOST:-rabbitmq}/g;\
s/%AMQP_PORT%/${AMQP_PORT:-5672}/g;\
s/%AMQP_USER%/${AMQP_USER:-guest}/g;\
s/%AMQP_PASSWORD%/${AMQP_PASSWORD:-guest}/g;\
s/%PG_HOST%/${PG_HOST:-postgres}/g;\
s/%PG_PORT%/${PG_PORT:-5432}/g;\
s/%PG_USER%/${PG_USER:-ion}/g;\
s/%PG_PASSWORD%/${PG_PASSWORD:-$POSTGRES_ION_PASSWORD}/g;\
s/%PG_ADMIN_USER%/${PG_ADMIN_USER:-postgres}/g;\
s/%PG_ADMIN_PASSWORD%/${PG_ADMIN_PASSWORD:-$POSTGRES_PASSWORD}/g;\
s/%AMQP_MPORT%/${AMQP_MPORT:-15672}/g;\
s/%AMQP_MUSER%/${AMQP_MUSER:-guest}/g;\
s/%AMQP_MPASSWORD%/${AMQP_MPASSWORD:-guest}/g" \
res/pyon.local.yml.template > res/pyon.local.yml

cp res/pyon.local.yml code/scioncc/defs/res/config

# Wait for DB to be ready (ion user available)

export PGPASSWORD="${PG_ADMIN_PASSWORD:-$POSTGRES_PASSWORD}"
until psql -h "${PG_HOST:-postgres}" -U "${PG_ADMIN_USER:-postgres}" postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='ion';" | grep -q 1; do
  echo "PostgreSQL is not ready - sleeping"
  sleep 2
done
echo "PostgreSQL is ready"

# Wait for RabbitMQ to be ready
while ! nc -z "${AMQP_HOST:-rabbitmq}" ${AMQP_PORT:-5672}; do
  echo "RabbitMQ is not ready - sleeping"
  sleep 1
done
echo "RabbitMQ is ready"

cd $SCION_DIR

if [ -f "${ENTRYPOINT_PREHOOK:-none}" ]; then
    echo "Executing entrypoint prehook script '${ENTRYPOINT_PREHOOK}'"
    . $ENTRYPOINT_PREHOOK
fi

# Start the container

if [ -z "$NO_PYCC" ] ; then
    echo "=== STARTUP CONTAINER ==="
    echo Application dir: `pwd`
    echo "bin/pycc ${PYCC_ARGS:--n}"
    eval "bin/pycc ${PYCC_ARGS:--n}"
fi
