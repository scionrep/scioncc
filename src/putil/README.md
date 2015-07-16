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
