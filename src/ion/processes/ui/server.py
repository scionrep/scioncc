#!/usr/bin/env python

"""Provides a general purpose HTTP server that can be configured with content location,
service gateway and web sockets"""

__author__ = 'Michael Meisinger, Stephen Henrie'

import flask
from flask import Flask, request, abort, session, render_template, redirect, Response
from flask.ext.socketio import SocketIO, SocketIOServer
from gevent.wsgi import WSGIServer

import gevent


from pyon.public import Container, StandaloneProcess, log, NotFound, CFG, get_sys_name, BadRequest
from pyon.util.containers import is_valid_identifier, get_datetime_str, named_any


DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 4000

CFG_PREFIX = "process.ui_server"
JSON = "application/json"
HTML = "text/html"

# Initialize the Flask app
app = Flask("ui_server")
ui_instance = None


class UIServer(StandaloneProcess):
    """
    Generic UI server.
    """
    def on_init(self):
        self.http_server = None
        self.socket_io = None
        self.service_gateway = None

        # Retain a pointer to this object for use in routes
        global ui_instance
        ui_instance = self

        # Configuration
        self.server_enabled = self.CFG.get_safe(CFG_PREFIX + ".server.enabled") is True
        self.server_debug = self.CFG.get_safe(CFG_PREFIX + ".server.debug") is True
        self.server_hostname = self.CFG.get_safe(CFG_PREFIX + ".server.hostname", DEFAULT_WEB_SERVER_HOSTNAME)
        self.server_port = self.CFG.get_safe(CFG_PREFIX + ".server.port", DEFAULT_WEB_SERVER_PORT)
        self.server_log_access = self.CFG.get_safe(CFG_PREFIX + ".server.log_access") is True
        self.server_log_errors = self.CFG.get_safe(CFG_PREFIX + ".server.log_errors") is True
        self.server_socket_io = self.CFG.get_safe(CFG_PREFIX + ".server.socket_io") is True
        self.server_secret = self.CFG.get_safe(CFG_PREFIX + ".server.secret") or ""

        self.has_service_gateway = self.CFG.get_safe(CFG_PREFIX + ".service_gateway") is True
        self.extensions = self.CFG.get_safe(CFG_PREFIX + ".extensions") or []
        self.extension_objs = []

        # One time setup
        if self.server_enabled:
            app.secret_key = self.server_secret or self.__class__.__name__   # Enables sessions

            if self.server_debug:
                app.debug = True

            if self.server_socket_io:
                self.socket_io = SocketIO(app)

            if self.has_service_gateway:
                from ion.services.service_gateway import ServiceGateway, sg_blueprint
                self.service_gateway = ServiceGateway(process=self, config=self.CFG, response_class=app.response_class)
                app.register_blueprint(sg_blueprint)

            for ext_cls in self.extensions:
                cls = named_any(ext_cls)
                self.extension_objs.append(cls())

            for ext_obj in self.extension_objs:
                ext_obj.on_init(self, app)

            # Start the web server
            self.start_service()

            log.info("Started UI Server on %s:%s" % (self.server_hostname, self.server_port))

        else:
            log.warn("UI Server disabled in config")

    def on_quit(self):
        self.stop_service()

    def start_service(self):
        """Starts the web server."""
        if self.http_server is not None:
            self.stop_service()

        if self.server_socket_io:
            self.socket_io = SocketIO(app)
            self.http_server = WSGIServer((self.server_hostname, self.server_port),
                                          app,
                                          log=None)
            self.http_server.start()
        else:
            self.http_server = SocketIOServer((self.server_hostname, self.server_port),
                                              app.wsgi_app,
                                              resource='socket.io',
                                              log=None)
            self.http_server._gl = gevent.spawn(self.http_server.serve_forever)
            from werkzeug.serving import run_with_reloader
            #run_with_reloader()

        if self.service_gateway:
            self.service_gateway.start()

        for ext_obj in self.extension_objs:
            ext_obj.on_start()

        return True

    def stop_service(self):
        """Responsible for stopping the gevent based web server."""
        for ext_obj in self.extension_objs:
            ext_obj.on_stop()

        if self.http_server is not None:
            self.http_server.stop()

        if self.service_gateway:
            self.service_gateway.stop()

        # Need to terminate the server greenlet?
        return True


class UIExtension(object):
    def on_init(self, ui_server, flask_app):
        pass
    def on_start(self):
        pass
    def on_stop(self):
        pass
