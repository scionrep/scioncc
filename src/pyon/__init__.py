# -------------------------------------------------------------------------
# Pyon framework and SciON capability container
# Static initialization
# -------------------------------------------------------------------------

# @WARN: GLOBAL STATE, STATIC CODE

# -------------------------------------------------------------------------
# Always monkey-patch as the very first thing (see gevent)

# TODO: Move this into a module that third parties can use
import sys

# Make monkey-patching work with debuggers and unittests by detecting already-imported modules
# Unload standard library modules so that gevent can monkey path them fresh - The order matters
# Fix for KeyError in <module 'threading'> when process stops
# http://stackoverflow.com/questions/8774958/keyerror-in-module-threading-after-a-successful-py-test-run
monkey_list = ['os', 'time', 'thread', 'socket', 'select', 'ssl', 'httplib', 'threading']
for mod in monkey_list:
    if mod in sys.modules:
        del sys.modules[mod]

if 'pydevd' in sys.modules:
    print "gevent monkey patching (for debugger use)"

    unmonkey = {'threading': ['_allocate_lock', '_get_ident']}
    unmonkey_backup = {}
    for modname, feats in unmonkey.iteritems():
        mod = __import__(modname)
        unmonkey_backup[modname] = dict((feat, getattr(mod, feat)) for feat in feats)

    from gevent import monkey; monkey.patch_all()
    
    for modname, feats_backup in unmonkey_backup.iteritems():
        mod = __import__(modname)
        for name,impl in feats_backup.iteritems():
            setattr(mod, name, impl)
else:
    print "gevent monkey patching (all)"

    from gevent import monkey; monkey.patch_all()

# Fix AttributeError("'_DummyThread' object has no attribute '_Thread__block'",) issue
# http://stackoverflow.com/questions/13193278/understand-python-threading-bug
import threading
try:
    threading._DummyThread._Thread__stop = lambda x: 42
except Exception as ex:
    pass


# -------------------------------------------------------------------------
# CONSTANTS FOR PYON CODE
# CHANGE HERE BEFORE IMPORTING ANY FURTHER PYON CODE TO OVERRIDE
DEFAULT_CONFIG_PATHS = ['res/config/pyon.yml']
DEFAULT_LOCAL_CONFIG_PATHS = ['res/config/pyon.local.yml']
