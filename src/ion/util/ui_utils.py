#!/usr/bin/env python

"""Utilities working with Flask UIs"""

__author__ = 'Michael Meisinger, Stephen Henrie'

import traceback
import flask
from flask import request, jsonify
import sys


class UIExtension(object):
    def on_init(self, ui_server, flask_app):
        pass
    def on_start(self):
        pass
    def on_stop(self):
        pass


# -------------------------------------------------------------------------
# UI Helpers

def build_json_response(result_obj):
    status = 200
    result = dict(status=status, result=result_obj)
    return jsonify(result)


def build_json_error():
    (type, value, tb) = sys.exc_info()
    status = getattr(value, "status_code", 500)
    result = dict(error=dict(message=value.message, exception=type.__name__, trace=traceback.format_exc()),
                  status=status)

    return jsonify(result), status


def get_arg(arg_name, default="", is_mult=False):
    if is_mult:
        aval = request.form.getlist(arg_name)
        return aval
    else:
        aval = request.values.get(arg_name, None)
        return str(aval) if aval else default


def get_auth():
    return dict(actor_id=flask.session.get("actor_id", ""),
                username=flask.session.get("username", ""),
                full_name=flask.session.get("full_name", ""),
                attributes=flask.session.get("attributes", {}),
                roles=flask.session.get("roles", {}),
                is_logged_in=bool(flask.session.get("actor_id", "")),
                is_registered=bool(flask.session.get("actor_id", "")),
                valid_until=flask.session.get("valid_until", 0))


def set_auth(actor_id, username, full_name, valid_until, **kwargs):
    flask.session["actor_id"] = actor_id or ""
    flask.session["username"] = username or ""
    flask.session["full_name"] = full_name or ""
    flask.session["valid_until"] = valid_until or 0
    flask.session["attributes"] = kwargs.copy()
    flask.session["roles"] = {}
    flask.session.modified = True


def clear_auth():
    flask.session["actor_id"] = ""
    flask.session["username"] = ""
    flask.session["full_name"] = ""
    flask.session["valid_until"] = 0
    flask.session["attributes"] = {}
    flask.session["roles"] = {}
    flask.session.modified = True
