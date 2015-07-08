# -------------------------------------------------------------------------
# Pyon framework and SciON capability container
# Static initialization
# -------------------------------------------------------------------------

# @WARN: GLOBAL STATE, STATIC CODE

#print "pyon static initialization (in pyon/__init__), gevent monkey-patching ..."

# -------------------------------------------------------------------------
# Always monkey-patch as the very first thing (see gevent)

# Make monkey-patching work with debuggers and unittests by detecting already-imported modules
# TODO: Move this into a module that third parties can use
import sys

if 'pydevd' in sys.modules:
    # The order matters
    monkey_list = ['os', 'time', 'thread', 'socket', 'select', 'ssl', 'httplib']
    for monkey in monkey_list:
        if monkey in sys.modules:
            mod = sys.modules[monkey]

            # Reload so the non-monkeypatched versions in the debugger don't get patched
            #reload(mod)
            del sys.modules[monkey]

    
    unmonkey = {'threading': ['_allocate_lock', '_get_ident']}
    unmonkey_backup = {}
    for modname,feats in unmonkey.iteritems():
        mod = __import__(modname)
        unmonkey_backup[modname] = dict((feat, getattr(mod, feat)) for feat in feats)

    from gevent import monkey; monkey.patch_all()
    
    for modname, feats_backup in unmonkey_backup.iteritems():
        mod = __import__(modname)
        for name,impl in feats_backup.iteritems():
            setattr(mod, name, impl)
else:
    if 'threading' in sys.modules:
        # Fix for KeyError in <module 'threading' when process stops
        # http://stackoverflow.com/questions/8774958/keyerror-in-module-threading-after-a-successful-py-test-run
        del sys.modules['threading']

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
