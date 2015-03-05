#!/usr/bin/env python

__author__ = 'Michael Meisinger'

import sys
from pkg_resources import load_entry_point
import pyon

def main():
    sys.exit(
        load_entry_point('coverage', 'console_scripts', 'coverage')()
    )

if __name__ == '__main__':
    main()
