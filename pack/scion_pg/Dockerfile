FROM library/postgres:9.6
MAINTAINER Michael Meisinger <michael.meisinger@gmail.com>

ENV POSTGIS_MAJOR 2.3

RUN echo "deb http://apt.postgresql.org/pub/repos/apt xenial-pgdg main" >> /etc/apt/sources.list
RUN apt-get update \
      && apt-get install -y --no-install-recommends \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
           postgresql-contrib-$PG_MAJOR \
           postgresql-server-dev-$PG_MAJOR \
           postgresql-plpython-$PG_MAJOR \
           postgresql-$PG_MAJOR-plv8 \
      && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./initdb-postgis.sh /docker-entrypoint-initdb.d/postgis.sh
