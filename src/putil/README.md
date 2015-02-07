Pyon Utilities
==============

Modules contained in here are utility classes Pyon uses, but are not enclosed
in the `pyon/` top level directory. Anything included from `pyon/` does 
automatic gevent monkey-patching, and items here may be used in places where 
this is not allowed.

The name `putil` was chosen to avoid conflict with any other `util` module 
tree as Python does not merge module trees but overrides them.


Includes share utilities based on https://github.com/ooici/utilities

    version='2013.06.11',
    author='Jonathan Newbrough',
    author_email='jonathan.newbrough@gyregroup.com',

Note: these classes may have NO dependencies on any other OOI projects.
Non-project eggs are ok, BUT keep in mind that making this project depend on an egg
means making EVERY other project will depend on this egg too.

Current dependencies are pyyaml and graypy.
