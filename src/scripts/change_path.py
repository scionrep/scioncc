""" Program for searching modules in buildout eggs directories and adding them to path using pth files
Useful when installing scipy, numpy, ...
"""

import os
import sys

SEARCH_DIRECTORIES = [
    "eggs",
    os.path.expanduser("~/.buildout/eggs")
]

from distutils.sysconfig import get_python_lib


def get_pth_path_for_module(module_name_full):
    """ Splits module name full into module name and version """
    name, version = get_module_name_version(module_name_full)
    if not version:
        # Only module name is specified
        pth_file_name = os.path.join(get_python_lib(), name+".pth")
    else:
        # Module name and verion are specified
        pth_file_name = os.path.join(get_python_lib(), name+"-"+version+".pth")
    return pth_file_name

def get_module_name_version(module_name_full):
    sp = module_name_full.split("==")
    if len(sp) == 1:
        return (sp[0], None)
    elif len(sp) == 2:
        return (sp[0], sp[1])
    else:
        raise ValueError("module name should be in form <name>==<version> or <name>")

def add_pth(module_name_full, module_path):
    """ Creates .pth file in site-packages with name <module_name_full>.pth which points out to module_path """
    pth_file_name = get_pth_path_for_module(module_name_full)
    with open(pth_file_name, "w") as f_out:
        f_out.write(module_path)
        print "Added .pth file", pth_file_name

def remove_pth(module_name_full):
    pth_file_name = get_pth_path_for_module(module_name_full)
    if os.path.exists(pth_file_name):
        print "Deleting", pth_file_name
        os.unlink(pth_file_name)
    else:
        print "File doesn't exist", pth_file_name

def find_file_by_prefix(prefix):
    """ Search for a file with prefix in SEARCH_DIRECTORIES
        Useful for finding module eggs
    """
    for directory in SEARCH_DIRECTORIES:
        if os.path.exists(directory) and os.path.isdir(directory):
            for file_name in os.listdir(directory):
                if file_name.startswith(prefix):
                    return os.path.abspath(os.path.join(directory, file_name))

def is_module_existing(module_name):
    try:
        __import__(module_name)
    except ImportError:
        return False
    else:
        return True

def add_module(module_name_full):
    module_name, version = get_module_name_version(module_name_full)
    if is_module_existing(module_name):
        print "Module already exists:", module_name
        if version:
            print "However, its version may be different from specified version"
        return
    if version:
        module_prefix = module_name + "-" + version
    else:
        module_prefix = module_name
    module_path = find_file_by_prefix(module_prefix)
    if not module_path:
        print "Cannot find module(egg)", module_name_full
    add_pth(module_name_full, module_path)

def main():
    if len(sys.argv) != 3:
        print "Usage %s add|remove <module_name>" % sys.argv[0]
        exit(1)
    command = sys.argv[1]
    module_name = sys.argv[2]
    if command == "add":
        add_module(module_name)
    elif command == "remove":
        remove_pth(module_name)
    else:
        print "Unknown command %s" % command
        sys.exit(1)

if __name__ == "__main__":
    main()
