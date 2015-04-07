#!/usr/bin/env python

"""Utilities working with Flask UIs"""

__author__ = 'Michael Meisinger, Stephen Henrie'

import traceback
import flask
from flask import request, jsonify
import sys
import json
import simplejson

from pyon.public import BadRequest, OT
from pyon.util.containers import get_datetime

from interface.objects import ActorIdentity, SecurityToken, TokenTypeEnum


CONT_TYPE_JSON = "application/json"
CONT_TYPE_HTML = "text/html"


class UIExtension(object):
    def on_init(self, ui_server, flask_app):
        pass
    def on_start(self):
        pass
    def on_stop(self):
        pass


# -------------------------------------------------------------------------
# Content encoding helpers

# Set standard json functions
json_dumps = json.dumps
json_loads = simplejson.loads   # Faster loading than regular json

def encode_ion_object(obj):
    return obj.__dict__


# -------------------------------------------------------------------------
# UI helpers


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
    return dict(user_id=flask.session.get("actor_id", ""),
                actor_id=flask.session.get("actor_id", ""),
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


class OAuthClientObj(object):
    """Object holding information for a OAuth client.
    Delegates information to ActorIdentity
    """
    client_id = None
    client_secret = "foo"
    is_confidential = False
    _redirect_uris = "https://foo"
    _default_scopes = "scioncc"

    @classmethod
    def from_actor_identity(cls, actor_obj):
        if not actor_obj or not isinstance(actor_obj, ActorIdentity) or not actor_obj.details or \
                        actor_obj.details.type_ != OT.OAuthClientIdentityDetails:
            raise BadRequest("Bad actor identity object")
        oauth_client = OAuthClientObj()
        oauth_client.actor = actor_obj
        oauth_client.client_id = actor_obj._id
        oauth_client.is_confidential = actor_obj.details.is_confidential
        oauth_client._redirect_uris = actor_obj.details.redirect_uris
        oauth_client._default_scopes = actor_obj.details.default_scopes
        return oauth_client

    @property
    def client_type(self):
        if self.is_confidential:
            return 'confidential'
        return 'public'

    @property
    def redirect_uris(self):
        if self._redirect_uris:
            return self._redirect_uris.split()
        return []

    @property
    def default_redirect_uri(self):
        return self.redirect_uris[0] if self.redirect_uris else ""

    @property
    def default_scopes(self):
        if self._default_scopes:
            return self._default_scopes.split()
        return []


class OAuthTokenObj(object):
    """Object holding information for an OAuth token.
    Delegates information to SecurityToken object
    """
    access_token = None
    refresh_token = None
    token_type = None
    client_id = None
    expires = None
    user = None
    _scopes = None

    @classmethod
    def from_security_token(cls, token_obj):
        if not token_obj or not isinstance(token_obj, SecurityToken) \
                        or not token_obj.token_type in (TokenTypeEnum.OAUTH_ACCESS, TokenTypeEnum.OAUTH_REFRESH):
            raise BadRequest("Bad token object")

        oauth_token = OAuthTokenObj()
        oauth_token.access_token = token_obj.attributes.get("access_token", "")
        oauth_token.refresh_token = token_obj.attributes.get("refresh_token", "")
        oauth_token.token_type = "Bearer"
        oauth_token._scopes = token_obj.attributes.get("scopes", "")
        oauth_token.client_id = token_obj.attributes.get("client_id", "")
        oauth_token.expires = get_datetime(token_obj.expires, local_time=False)
        oauth_token.user = {"actor_id": token_obj.actor_id}
        return oauth_token

    def delete(self):
        print "### DELETE TOKEN", self.access_token
        return self

    @property
    def scopes(self):
        if self._scopes:
            return self._scopes.split()
        return []