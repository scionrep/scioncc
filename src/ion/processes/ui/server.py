#!/usr/bin/env python

"""Provides a general purpose HTTP server that can be configured with content location,
service gateway and web sockets"""

__author__ = 'Michael Meisinger, Stephen Henrie'

from flask import Flask
import gevent
from gevent.wsgi import WSGIServer

from flask.ext.socketio import SocketIO, SocketIOServer
from pyon.public import StandaloneProcess, log, BadRequest, OT, get_ion_ts_millis
from pyon.util.containers import named_any
from ion.util.ui_utils import build_json_response, build_json_error, get_arg, get_auth, set_auth, clear_auth

from interface.services.core.iidentity_management_service import IdentityManagementServiceProcessClient


DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 4000
DEFAULT_SESSION_TIMEOUT = 600
DEFAULT_GATEWAY_PREFIX = "/service"

CFG_PREFIX = "process.ui_server"

# Initialize the main Flask app
app = Flask("ui_server", static_folder=None, template_folder=None)
ui_instance = None


class UIServer(StandaloneProcess):
    """
    Process to start a generic UI server that can be extended with content and service gateway
    """
    def on_init(self):
        # Retain a pointer to this object for use in routes
        global ui_instance
        ui_instance = self

        # Main references to basic components (if initialized)
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

        self.has_service_gateway = self.CFG.get_safe(CFG_PREFIX + ".service_gateway.enabled") is True
        self.service_gateway_prefix = self.CFG.get_safe(CFG_PREFIX + ".service_gateway.url_prefix") or DEFAULT_GATEWAY_PREFIX
        self.extensions = self.CFG.get_safe(CFG_PREFIX + ".extensions") or []
        self.extension_objs = []

        # TODO: What about https?
        self.base_url = "http://%s:%s" % (self.server_hostname, self.server_port)
        self.gateway_base_url = None

        self.idm_client = IdentityManagementServiceProcessClient(process=self)

        # One time setup
        if self.server_enabled:
            app.secret_key = self.server_secret or self.__class__.__name__   # Enables encrypted session cookies

            if self.server_debug:
                app.debug = True

            if self.server_socket_io:
                self.socket_io = SocketIO(app)

            if self.has_service_gateway:
                from ion.services.service_gateway import ServiceGateway, sg_blueprint
                self.gateway_base_url = self.base_url + self.service_gateway_prefix
                self.service_gateway = ServiceGateway(process=self, config=self.CFG, response_class=app.response_class)

                app.register_blueprint(sg_blueprint, url_prefix=self.service_gateway_prefix)

            for ext_cls in self.extensions:
                cls = named_any(ext_cls)
                self.extension_objs.append(cls())

            for ext_obj in self.extension_objs:
                ext_obj.on_init(self, app)
            if self.extensions:
                log.info("UI Server: %s extensions initialized", len(self.extensions))

            # Start the web server
            self.start_service()

            log.info("UI Server: Started server on %s" % self.base_url)

        else:
            log.warn("UI Server: Server disabled in config")

    def on_quit(self):
        self.stop_service()

    def start_service(self):
        """Starts the web server."""
        if self.http_server is not None:
            self.stop_service()

        if self.server_socket_io:
            self.http_server = SocketIOServer((self.server_hostname, self.server_port),
                                              app.wsgi_app,
                                              resource='socket.io',
                                              log=None)
            self.http_server._gl = gevent.spawn(self.http_server.serve_forever)
        else:
            self.http_server = WSGIServer((self.server_hostname, self.server_port),
                                          app,
                                          log=None)
            self.http_server.start()

        if self.service_gateway:
            self.service_gateway.start()
            log.info("UI Server: Service Gateway started on %s", self.gateway_base_url)

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
            username = get_arg("username")
            password = get_arg("password")
            if username and password:
                actor_id = self.idm_client.check_actor_credentials(username, password)
                actor_user = self.idm_client.read_identity_details(actor_id)
                if actor_user.type_ != OT.UserIdentityDetails:
                    raise BadRequest("Bad identity details")

                full_name = actor_user.contact.individual_names_given + " " + actor_user.contact.individual_name_family

                valid_until = int(get_ion_ts_millis() / 1000 + self.session_timeout)
                set_auth(actor_id, username, full_name, valid_until=valid_until)
                user_info = get_auth()
                return build_json_response(user_info)

            else:
                raise BadRequest("Username or password missing")

        except Exception:
            return build_json_error()

    def get_session(self):
        try:
            user_info = get_auth()
            return build_json_response(user_info)
        except Exception:
            return build_json_error()

    def logout(self):
        try:
            clear_auth()
            return build_json_response("OK")
        except Exception:
            return build_json_error()


# -------------------------------------------------------------------------
# Authentication routes

@app.route('/auth/login', methods=['GET', 'POST'])
def login_route():
    return ui_instance.login()


@app.route('/auth/session', methods=['GET', 'POST'])
def session_route():
    return ui_instance.get_session()


@app.route('/auth/logout', methods=['GET', 'POST'])
def logout_route():
    return ui_instance.logout()
