-- Full text indexing and search extensions
CREATE EXTENSION pg_trgm;


-- PostGIS extensions
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
-- create extension fuzzystrmatch;


-- JavaScript language extention
CREATE EXTENSION plv8;

-- Functions to query JSON columns
-- See here for originals(before porting to node.js):
-- https://gist.github.com/tobyhede/2715918/raw/370a4defac00d085a5351e084cb42b9d8ccb1092/postsql.sql


CREATE OR REPLACE FUNCTION json_string(data json, key text) RETURNS TEXT AS
$$
var res = data;
var keys = key.split(".");
for (var i=0; i<keys.length; i++) {
    if (res) {
        res = res[keys[i]];
    }
}
if (res === undefined) {
    res = null;
}
return res;
$$
LANGUAGE plv8 IMMUTABLE STRICT;

-- Results in a list of attributes
CREATE OR REPLACE FUNCTION json_attrs(data json) RETURNS TEXT[] AS
$$
return Object.keys(data);
$$
LANGUAGE plv8 IMMUTABLE STRICT;

-- Results in a list of nested object types
CREATE OR REPLACE FUNCTION json_nested(data json) RETURNS TEXT[] AS
$$
var res = [];
for (var key in data) {
    if (data.hasOwnProperty(key) && (!(data[key] instanceof Array)) && (data[key]) && (data[key]["type_"])) {
        res.push(data[key]["type_"]);
    }
}
return res;
$$
LANGUAGE plv8 IMMUTABLE STRICT;


-- Results in a list of keywords
CREATE OR REPLACE FUNCTION json_keywords(data json) RETURNS TEXT[] AS
$$
var res = [];
if (data["keywords"] && (data["keywords"] instanceof Array)) {
    res = data["keywords"];
}
return res;
$$
LANGUAGE plv8 IMMUTABLE STRICT;

-- Results in a special attribute extracted from object
CREATE OR REPLACE FUNCTION json_specialattr(data json) RETURNS TEXT AS
$$
function is_object(obj) {
    return obj && (obj instanceof Object) && !(obj instanceof Array);
}
var special = null;
doc_type = data["type_"];
switch (doc_type) {
    case "ActorIdentity": if (is_object(data["details"]) && is_object(data["details"]["contact"]) && data["details"]["contact"]["email"]) {
                            special = "contact.email=" + data["details"]["contact"]["email"];
                          }
                          break;
    case "Org": if (data["org_governance_name"]) {
                    special = "org_governance_name=" + data["org_governance_name"];
                }
                break;
    case "UserRole": if (data["governance_name"]) {
                        special = "governance_name=" + data["governance_name"];
                     }
                     break;
}
return special;
$$
LANGUAGE plv8 IMMUTABLE STRICT;

-- Results in a list of alternative id namespaces
CREATE OR REPLACE FUNCTION json_altids_ns(data json) RETURNS TEXT[] AS
$$
var alts = {};
var kw_list = data["alt_ids"];
var parts, alts_list;
if (kw_list && (kw_list instanceof Array)) {
    for (var i=0; i<kw_list.length; i++) {
        parts = kw_list[i].split(":");
        if (parts.length > 1){
            alts[parts[0]] = 1;
        } else{
            alts["_"] = 1;
        }
    }
}
alts_list = Object.keys(alts);
alts_list.sort();
return alts_list;
$$
LANGUAGE plv8 IMMUTABLE STRICT;

-- Results in a list of alternative ids
CREATE OR REPLACE FUNCTION json_altids_id(data json) RETURNS TEXT[] AS
$$
var delimiter = ":";
var alts_list, alt, i, delimiter_pos, kw;
var alts_set = {};
var kw_list = data["alt_ids"];
if (kw_list && (kw_list instanceof Array)) {
    for (i=0; i<kw_list.length; i++) {
        kw = kw_list[i];
        delimiter_pos = kw.indexOf(delimiter);
        if (delimiter_pos == -1) {
            alt = kw;
        } else {
            alt = kw.substring(delimiter_pos+1);
        }
        alts_set[alt] = true;
    }
}
alts_list = Object.keys(alts_set);
alts_list.sort();
return alts_list;
$$
LANGUAGE plv8 IMMUTABLE STRICT;

-- Results in all attributes in one big string for full text query
CREATE OR REPLACE FUNCTION json_allattr(data json) RETURNS TEXT AS
$$
function is_object(obj) {
    return obj && (obj instanceof Object) && !(obj instanceof Array);
}
var part, key;
var parts = [];
var max_length = 500;
var no_attrs = {
    "_id": true,
    "_rev": true,
    "type_": true,
    "ts_created": true,
    "ts_updated": true,
    "lcstate": true,
    "availability": true
};
// Ignore boolean. Because it was ignored in original PL/Python implementation
var ok_types = {
    "string": true,
    "number": true
};
for (key in data) {
    if (data.hasOwnProperty(key)) {
        if (!no_attrs[key] && ok_types[typeof data[key]]) {
            part = data[key];
            if(part.length > max_length){
                part = part.substring(0, max_length);
            }
            parts.push(part);
        }
    }
}
if (data["type_"] == "ActorIdentity") {
    if (is_object(data["details"]) && is_object(data["details"]["contact"])) {
        contact = data["details"]["contact"];
        for (key in contact){
            if (contact.hasOwnProperty(key)) {
                if (!no_attrs[key] && ok_types[typeof contact[key]]) {
                    part = contact[key];
                    if (part.length > max_length) {
                        part = part.substring(0, max_length);
                    }
                    parts.push(part);
                }
            }
        }
    }
}
return parts.join(" ");
$$
LANGUAGE plv8 IMMUTABLE STRICT;
