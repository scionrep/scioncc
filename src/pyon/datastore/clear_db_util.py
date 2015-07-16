#!/usr/bin/env python

"""Admin tool to clear databases"""

from optparse import OptionParser
import sys

import logging
log = logging.getLogger('clear_db')

from pyon.datastore.datastore_common import DatastoreFactory


def main():

    usage = \
    """
    %prog [options] prefix
    """
    description = "Use this program to clear databases that match a given prefix"
    parser = OptionParser(usage=usage, description=description)
    parser.add_option("-P", "--port", dest="db_port", default=None, help="Port number for db", action="store", type=int, metavar="PORT")
    parser.add_option("-H", "--host", dest="db_host", default='localhost', help="The host name or ip address of the db server", action="store", type=str, metavar="HOST")
    parser.add_option("-u", "--username", dest="db_uname", default=None, help="Username for the db server", action="store", type=str, metavar="UNAME")
    parser.add_option("-p", "--password", dest="db_pword", default=None, help="Password for the db server", action="store", type=str, metavar="PWORD")
    parser.add_option("-s", "--sysname", dest="sysname", default=None, help="The sysname prefix to clear databases", action="store", type=str, metavar="SYSNAME")
    parser.add_option("-t", "--store_type", dest="db_type", default="postgresql", help="Datastore type", action="store", type=str, metavar="DSTYPE")
    parser.add_option("-v", "--verbose", help="More verbose output", action="store_true")
    parser.add_option("-d", "--dump", dest="dump_path", default=None, help="Dump sysname datastores to path", action="store", type=str, metavar="DPATH")
    parser.add_option("-l", "--load", dest="load_path", default=None, help="Load dumped datastore from path", action="store", type=str, metavar="LPATH")

    (options, args) = parser.parse_args()

    from pyon.core import log as logutil
    logutil.configure_logging(logutil.DEFAULT_LOGGING_PATHS)

    if options.dump_path:
        config = create_config(options.db_host, options.db_port, options.db_uname, options.db_pword)
        sysname = options.sysname or "scion"
        log.info("dumping %s datastores to %s", sysname, options.dump_path)
        from pyon.datastore.datastore_admin import DatastoreAdmin
        datastore_admin = DatastoreAdmin(config=config, sysname=sysname)
        datastore_admin.dump_datastore(path=options.dump_path)
    elif options.load_path:
        config = create_config(options.db_host, options.db_port, options.db_uname, options.db_pword)
        sysname = options.sysname or "scion"
        log.info("loading %s datastores from dumped content in %ss", sysname, options.dump_path)
        from pyon.datastore.datastore_admin import DatastoreAdmin
        datastore_admin = DatastoreAdmin(config=config, sysname=sysname)
        datastore_admin.load_datastore(path=options.load_path)
    else:
        if len(args) == 0:
            log.error("Error: no prefix argument specified")
            parser.print_help()
            sys.exit()

        if len(args) != 1:
            log.error("Error: You can not specify multiple prefixes. Received args: %s", str(args))
            parser.print_help()
            sys.exit()

        prefix = args[0]

        if prefix == "":
            log.error("Error: You can not give the empty string as a prefix!")
            parser.print_help()
            sys.exit()

        config = create_config(options.db_host, options.db_port, options.db_uname, options.db_pword, options.db_type)
        _clear_db(config, prefix=prefix, sysname=options.sysname, verbose=bool(options.verbose))

def create_config(host, port, username, password, type="postgresql"):
    config = dict(host=host, port=port, username=username, password=password, type=type)
    return config

def clear_db(config, prefix, sysname=None):
    config = DatastoreFactory.get_server_config(config)
    _clear_db(config=config,
              prefix=prefix,
              sysname=sysname)

def _clear_db(config, prefix, sysname=None, verbose=False):
    server_type = config.get("type", "postgresql")
    if server_type == "postgresql":
        _clear_postgres(
        config=config,
        prefix=prefix,
        sysname=sysname,
        verbose=verbose)
    else:
        raise Exception("Unknown server type to clear: %s" % server_type)


def _clear_postgres(config, prefix, verbose=False, sysname=None):
    cfg_copy = dict(config)
    if "password" in cfg_copy:
        cfg_copy["password"] = "***"
    if "admin_password" in cfg_copy:
        cfg_copy["admin_password"] = "***"
    log.info("Clearing PostgreSQL databases using config=%s", cfg_copy)

    import getpass
    db_name = prefix if not sysname else sysname + "_" + config.get('database', 'ion')

    username = config.get("admin_username", None) or getpass.getuser()
    password = config.get("admin_password", None) or ""
    host = config.get('host', None) or 'localhost'
    port = str(config.get('port', None) or '5432')
    default_database = config.get('default_database', None) or 'postgres'


    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    dsn = "host=%s port=%s dbname=%s user=%s password=%s" % (host, port, default_database, username, password)
    with psycopg2.connect(dsn) as conn:
        log.info("Connected to PostgreSQL as: %s", dsn.rsplit("=", 1)[0] + "=***")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:

            cur.execute("SELECT pid, datname FROM pg_stat_activity")
            rows = cur.fetchall()
            conn_by_db = {}
            for row in rows:
                conn_id, dbn = row[0], row[1]
                conn_by_db.setdefault(dbn, []).append(conn_id)
            log.info("Found %s open connections", len(rows))

            cur.execute("SELECT datname FROM pg_database")
            rows = cur.fetchall()
            ignored_num = 0
            for row in rows:
                try:
                    db_name = row[0]
                    if (prefix == '*' and not db_name.startswith('_')) or db_name.lower().startswith(prefix.lower()):
                        log.info("(PostgreSQL) DROP DATABASE %s", db_name)
                        if conn_by_db.get(db_name, None):
                            for conn_id in conn_by_db[db_name]:
                                cur.execute("SELECT pg_terminate_backend(%s)", (conn_id, ))
                            log.info("Dropped %s open connections to database '%s'", len(conn_by_db[db_name]), db_name)
                        cur.execute("DROP DATABASE %s" % db_name)
                    else:
                        ignored_num += 1
                except Exception as ex:
                    log.exception("Could not drop database '%s'", db_name)

            log.info("There are %s databases not matching prefix", ignored_num)

if __name__ == '__main__':
    main()
