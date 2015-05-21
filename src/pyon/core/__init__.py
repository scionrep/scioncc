#!/usr/bin/env python

"""Pyon related basic definitions and constants"""

# Messaging constants

MSG_HEADER_PERFORMATIVE = "performative"
MSG_HEADER_FORMAT = "format"
MSG_HEADER_OP = "op"

MSG_HEADER_ACTOR = "ion-actor-id"
MSG_HEADER_VALID = "expiry"
MSG_HEADER_ROLES = "ion-actor-roles"
MSG_HEADER_TOKENS = "ion-actor-tokens"

MSG_HEADER_RESOURCE_ID = "resource-id"
MSG_HEADER_USER_CONTEXT_ID = "user-context-id"


# Process related constants

PROCTYPE_SERVICE = "service"
PROCTYPE_AGENT = "agent"
PROCTYPE_STREAMPROC = "stream_process"
PROCTYPE_STANDALONE = "standalone"
PROCTYPE_IMMEDIATE = "immediate"
PROCTYPE_SIMPLE = "simple"
