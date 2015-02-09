#!/usr/bin/env python

"""Provides a general purpose HTTP server that can be configured with content location,
service gateway and web sockets"""

__author__ = 'Michael Meisinger, Stephen Henrie'

import traceback
import flask
from flask import Flask, request, abort, session, render_template, redirect, Response, jsonify
from flask.ext.socketio import SocketIO, SocketIOServer
import gevent
from gevent.wsgi import WSGIServer
import sys

from pyon.public import StandaloneProcess, log, NotFound, CFG, BadRequest, OT, get_ion_ts_millis
from pyon.util.containers import is_valid_identifier, get_datetime_str, named_any

from interface.services.core.iidentity_management_service import IdentityManagementServiceProcessClient


DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 4000
DEFAULT_SESSION_TIMEOUT = 600

CFG_PREFIX = "process.ui_server"
JSON = "application/json"
HTML = "text/html"

# Initialize the main  Flask app
app = Flask("ui_server")
ui_instance = None


class UIServer(StandaloneProcess):
    """
    Process to start a generic UI server that can be extended with content and service gateway
    """
    def on_init(self):
        # Retain a pointer to this object for use in routes
        global ui_instance
        ui_instance = self

        self.http_server = None
        self.socket_io = None
        self.service_gateway = None

        # Configuration
        self.server_enabled = self.CFG.get_safe(CFG_PREFIX + ".server.enabled") is True
        self.server_debug = self.CFG.get_safe(CFG_PREFIX + ".server.debug") is True
        self.server_hostname = self.CFG.get_safe(CFG_PREFIX + ".server.hostname", DEFAULT_WEB_SERVER_HOSTNAME)
        self.server_port = self.CFG.get_safe(CFG_PREFIX + ".server.port", DEFAULT_WEB_SERVER_PORT)
        self.server_log_access = self.CFG.get_safe(CFG_PREFIX + ".server.log_access") is True
        self.server_log_errors = self.CFG.get_safe(CFG_PREFIX + ".server.log_errors") is True
        self.server_socket_io = self.CFG.get_safe(CFG_PREFIX + ".server.socket_io") is True
        self.server_secret = self.CFG.get_safe(CFG_PREFIX + ".security.secret") or ""
        self.session_timeout = int(self.CFG.get_safe(CFG_PREFIX + ".security.session_timeout") or DEFAULT_SESSION_TIMEOUT)

        self.has_service_gateway = self.CFG.get_safe(CFG_PREFIX + ".service_gateway") is True
        self.extensions = self.CFG.get_safe(CFG_PREFIX + ".extensions") or []
        self.extension_objs = []

        self.idm_client = IdentityManagementServiceProcessClient(process=self)

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

    # -------------------------------------------------------------------------
    # Authentication

    def login(self):
        try:
            username = self.get_arg("username")
            password = self.get_arg("password")
            if username and password:
                actor_id = self.idm_client.check_actor_credentials(username, password)
                actor_user = self.idm_client.read_identity_details(actor_id)
                if actor_user.type_ != OT.UserIdentityDetails:
                    raise BadRequest("Bad identity details")

                full_name = actor_user.contact.individual_names_given + " " + actor_user.contact.individual_name_family

                valid_until = int(get_ion_ts_millis() / 1000 + self.session_timeout)
                self.set_auth(actor_id, username, full_name, valid_until=valid_until)
                user_info = self.get_auth()
                return self.build_json_response(user_info)

            else:
                raise BadRequest("Username or password missing")

        except Exception:
            return self.build_error()

    def get_session(self):
        try:
            user_info = self.get_auth()
            return self.build_json_response(user_info)
        except Exception:
            return self.build_error()

    def logout(self):
        try:
            user_info = self.get_auth()
            self.clear_auth()
            return self.build_json_response("OK")
        except Exception:
            return self.build_error()

    # -------------------------------------------------------------------------
    # Helpers

    def build_json_response(self, result_obj):
        status = 200
        result = dict(status=status, result=result_obj)
        return jsonify(result)

    def build_error(self):
        (type, value, tb) = sys.exc_info()
        status = getattr(value, "status_code", 500)
        result = dict(error=dict(message=value.message, type=type.__name__, trace=traceback.format_exc()),
                      status=status)

        return jsonify(result), status

    def get_arg(self, arg_name, default="", is_mult=False):
        if is_mult:
            aval = request.form.getlist(arg_name)
            return aval
        else:
            aval = request.values.get(arg_name, None)
            return str(aval) if aval else default

    def get_auth(self):
        return dict(actor_id=flask.session.get("actor_id", ""),
                    username=flask.session.get("username", ""),
                    full_name=flask.session.get("full_name", ""),
                    attributes=flask.session.get("attributes", {}),
                    roles=flask.session.get("roles", {}),
                    is_logged_in=bool(flask.session.get("actor_id", "")),
                    is_registered=bool(flask.session.get("actor_id", "")),
                    valid_until=flask.session.get("valid_until", 0))

    def set_auth(self, actor_id, username, full_name, valid_until, **kwargs):
        flask.session["actor_id"] = actor_id or ""
        flask.session["username"] = username or ""
        flask.session["full_name"] = full_name or ""
        flask.session["valid_until"] = valid_until or 0
        flask.session["attributes"] = kwargs.copy()
        flask.session["roles"] = {}
        flask.session.modified = True

    def clear_auth(self):
        flask.session["actor_id"] = ""
        flask.session["username"] = ""
        flask.session["full_name"] = ""
        flask.session["valid_until"] = 0
        flask.session["attributes"] = {}
        flask.session["roles"] = {}
        flask.session.modified = True


@app.route('/auth/login', methods=['GET', 'POST'])
def login_route():
    return ui_instance.login()


@app.route('/auth/session', methods=['GET', 'POST'])
def session_route():
    return ui_instance.get_session()


@app.route('/auth/logout', methods=['GET', 'POST'])
def logout_route():
    return ui_instance.logout()


class UIExtension(object):
    def on_init(self, ui_server, flask_app):
        pass
    def on_start(self):
        pass
    def on_stop(self):
        pass
