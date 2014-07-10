#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright 2014 Continuuity, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#
# Checks that the license dependencies files used match the dependencies in the product.
# Run this script after building the SDK.
#


from optparse import OptionParser
import csv
import os
import subprocess
import sys
import tempfile

VERSION = "0.0.2"

DEFAULT_VERSION = "2.3.0-SNAPSHOT"

LICENSE_MASTERS = "license_masters"
MASTER_CSV = "reactor-dependencies-master.csv"

ENTERPRISE = "reactor-enterprise-dependencies"
LEVEL_1 = "reactor-level-1-dependencies"
SINGLENODE = "reactor-singlenode-dependencies"

LICENSES_SOURCE = "../../developer-guide/source/licenses"

REACTOR_VERSION_FILE = "../../../version.txt"

SPACE = " "*3
BACK_DASH = "\-"

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

def get_sdk_version():
    # Sets the Reactor Build Version from the version.txt file
    ver_path = os.path.join(SCRIPT_DIR_PATH, REACTOR_VERSION_FILE)
    try:
        with open(ver_path,'r') as f:
            sdk_version = f.read()
    except:
        print "Couldn't read DEFAULT_VERSION from path: %s" % ver_path
        sys.exit(1)
    return sdk_version

def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [options]",
        description="Checks that the license dependencies files used match the dependencies in the product.")

    parser.add_option(
        "-v", "--version",
        action="store_true",
        dest="version",
        help="Version of software",
        default=False)

    sdk_version = get_sdk_version()
    parser.add_option(
        "-w", "--build_version",
        dest="build_version",
        help="The built version of the Continuuity SDK "
             "(default: %s)" % sdk_version,
        default=sdk_version)

    parser.add_option(
        "-e", "--enterprise",
        action="store_true",
        dest="enterprise",
        help="Process enterprise dependencies",
        default=False)

    parser.add_option(
        "-l", "--level_1",
        action="store_true",
        dest="level_1",
        help="Process level 1 dependencies",
        default=False)

    parser.add_option(
        "-s", "--singlenode",
        action="store_true",
        dest="singlenode",
        help="Process singlenode dependencies",
        default=False)

    parser.add_option(
        "-a", "--rst_enterprise",
        action="store_true",
        dest="rst_enterprise",
        help="Print enterprise dependencies to an rst file",
        default=False)

    parser.add_option(
        "-b", "--rst_level_1",
        action="store_true",
        dest="rst_level_1",
        help="Print level1 dependencies to an rst file",
        default=False)

    parser.add_option(
        "-c", "--rst_singlenode",
        action="store_true",
        dest="rst_singlenode",
        help="Print singlenode dependencies to an rst file",
        default=False)

    parser.add_option(
        "-m", "--master_print",
        action="store_true",
        dest="master_print",
        help="Prints out the master dependency file",
        default=False)

    (options, args) = parser.parse_args()

    if options.version:
        print "Version: %s" % VERSION
        sys.exit(1)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return options, args


def log(message, type):
    """Basic logger, print output directly to stdout and errors to stderr.
    """
    (sys.stdout if type == 'notice' else sys.stderr).write(message + "\n")


def process_master():
    # Read in the master csv files and create a dictionary of it
    # Keys are the jars, Values are the Library instances
    # Get the current dependencies master csv file
    # "jar","Version","Classifier","License","License URL"
    master_libs_dict = {}
    print "Reading master file"
    csv_path = os.path.join(SCRIPT_DIR_PATH, LICENSE_MASTERS, MASTER_CSV)
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count += 1
            jar = row[0]
            if len(row)==5:
                lib = Library(row[0], row[3], row[4])
                # Place lib reference in dictionary
                if not master_libs_dict.has_key(lib.id):
                    master_libs_dict[lib.id] = lib
                else:
                    print "Duplicate key: %s" %lib.id
                    print "%sCurrent library: %s" % (SPACE, master_libs_dict[lib.id])
                    print "%sNew library: %s" % (SPACE, lib)
            else:
                print "%sError with %s\n%srow: %s" % (SPACE, jar, SPACE, row)
    # Print out the results
    keys = master_libs_dict.keys()
#     keys.sort()
#     for k in keys:
#         master_libs_dict[k].pretty_print()    
    print "Master CSV: Rows read: %s; Unique Keys created: %s" % (row_count, len(keys))
    return master_libs_dict

    
def master_print():
    master_libs_dict = process_master()
    # Print out the results
    keys = master_libs_dict.keys()
    keys.sort()
    for k in keys:
        master_libs_dict[k].pretty_print()    


def process_enterprise(input_file, options):
    return process_dependencies(ENTERPRISE)


def process_singlenode(input_file, options):
    return process_dependencies(SINGLENODE)

    
def process_level_1(input_file, options):
    master_libs_dict = process_master()
    # Build a lookup table for the artifacts
    # A dictionary relating an artifact to a Library instance
    master_artifact_ids= {}
    keys = master_libs_dict.keys()
    keys.sort()
    for k in keys:
        lib = master_libs_dict[k]
        if not master_artifact_ids.has_key(lib.base):
            master_artifact_ids[lib.base] = lib
            print "Master: %s" % lib.base
        
    # Read dependencies: assumes first row is a header
    level_1_dict = {}
    missing_libs_dict = {}
    csv_path = os.path.join(SCRIPT_DIR_PATH, LICENSES_SOURCE, LEVEL_1 + ".csv")
    print "Reading dependencies file:\n%s" % csv_path
    import csv
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        unique_row_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count += 1
            if row_count == 1:
                continue # Ignore header row
            group_id, artifact_id = row
            key = (group_id, artifact_id)
            if not level_1_dict.has_key(key):
                unique_row_count += 1
                artifact_has_hyphen = artifact_id.rfind("-")
                if master_artifact_ids.has_key(artifact_id):
                    # Look up lib reference in dictionary
                    lib = master_artifact_ids[artifact_id]
                    level_1_dict[key] = (group_id, artifact_id, lib.license, lib.license_url)
                    continue
                if artifact_has_hyphen != -1:
                    # Try looking up just the first part
                    sub_artifact_id = artifact_id[:artifact_has_hyphen]
                    if master_artifact_ids.has_key(sub_artifact_id):
                        lib = master_artifact_ids[sub_artifact_id]
                        level_1_dict[key] = (group_id, artifact_id, lib.license, lib.license_url)
                        continue
                if not missing_libs_dict.has_key(artifact_id):
                    missing_libs_dict[artifact_id] = group_id

    # Drop header row from count
    row_count -= 1 
    print "Level 1: Row count: %s" % row_count
    print "Level 1: Unique Row count: %s" % unique_row_count
    print "Level 1: Missing Artifacts: %s" % len(missing_libs_dict.keys())
    
    for key in missing_libs_dict.keys():
        print "Missing artifact_id: %s (for %s)" % (key, missing_libs_dict[key])

    # Return the "Package","Artifact","License","License URL"
    rst_data = []
    keys = level_1_dict.keys()
    keys.sort()
    for k in keys:
        row = level_1_dict[k]
        rst_data.append(row)
    return rst_data


def process_dependencies(dependency):
    # Read in the current master csv file and create a structure with it
    # Read in the new dependencies csv file
    # Create and print to standard out the list of the references
    # Make a list of the references for which links are missing and need to be added to the master
    # Make a new master list
    # Return "Package","Version","Classifier","License","License URL"
    
    master_libs_dict = process_master()
    
    # Read dependencies
    new_libs_dict = {}
    missing_libs_dict = {}
    csv_path = os.path.join(SCRIPT_DIR_PATH, LICENSES_SOURCE, dependency + ".csv")
    print "Reading dependencies file:\n%s" % csv_path
    import csv
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count += 1
            jar = row[0]
            lib = Library(row[0], "", "")
            print 'lib.id %s' % lib.id
            # Look up lib reference in master dictionary; if not there, add it
            if not master_libs_dict.has_key(lib.id):
                master_libs_dict[lib.id] = lib
                missing_libs_dict[lib.id] = lib
            # Place lib reference in dictionary
            if not new_libs_dict.has_key(lib.id):
                new_libs_dict[lib.id] = master_libs_dict[lib.id]
            else:
                print "Duplicate key: %s" %lib.id
                print "%sCurrent library: %s" % (SPACE, new_libs_dict[lib.id])
                print "%sNew library: %s" % (SPACE, lib)

    missing_entries = len(missing_libs_dict.keys())
    for lib_dict in [master_libs_dict]:
        keys = lib_dict.keys()
        keys.sort()
        missing_licenses = 0
        for k in keys:
            if lib_dict[k].license == "":
                missing_licenses += 1
#             Print out the results
#             lib_dict[k].pretty_print()
#         print "Records: %s" % len(keys)

    print "New CSV: Rows: %s" % len(new_libs_dict.keys())
    print "New Master CSV: Rows: %s" % len(master_libs_dict.keys())
    print "New Master CSV: Missing Entry Rows: %s" % missing_entries
    print "New Master CSV: Missing License Rows: %s" % missing_licenses

    # Write out a new master cvs file, only if not already exists 
    if missing_entries or missing_licenses:
        import csv
    
        csv_path = os.path.join(SCRIPT_DIR_PATH, LICENSE_MASTERS, MASTER_CSV + ".new.csv")
        if os.path.isfile(csv_path):
            print "New master CSV: Master file already exists: %s" % csv_path
        else:
            with open(csv_path, 'w') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                keys = master_libs_dict.keys()
                keys.sort()
                for k in keys:
#                     l = lib_dict[k]
#                     r = l.get_row()
                    csv_writer.writerow(lib_dict[k].get_row())
            print "New master CSV: wrote %s records to:\n%s" % (len(keys), csv_path)

    # Return the "Package","Version","Classifier","License","License URL"
    rst_data = []
    keys = new_libs_dict.keys()
    keys.sort()
    for k in keys:
        lib = new_libs_dict[k]
        row = list(lib.get_row())
        if row[2] == "":
            row[2] = BACK_DASH
#         print row
        rst_data.append(row)
    return rst_data


def print_rst_level_1(input_file, options):
    title = "Level 1"
    file_base = LEVEL_1
    header = '"Package","Artifact","License","License URL"'
    widths = "20, 20, 20, 40"
    data_list = process_level_1(input_file, options)
    print_dependencies(title, file_base, header, widths, data_list)


def print_rst_enterprise(input_file, options):
    title = "Distributed"
    file_base = ENTERPRISE
    header = '"Package","Version","Classifier","License","License URL"'
    widths = "20, 10, 10, 20, 35"
    data_list = process_enterprise(input_file, options)
    print_dependencies(title, file_base, header, widths, data_list)


def print_rst_singlenode(input_file, options):
    title = "SingleNode"
    file_base = SINGLENODE
    header = '"Package","Version","Classifier","License","License URL"'
    widths = "20, 10, 10, 20, 30"
    data_list = process_singlenode(input_file, options)
    print_dependencies(title, file_base, header, widths, data_list)

   
def print_dependencies(title, file_base, header, widths, data_list):
# Example: "Level 1", LEVEL_1, ...
    RST_HEADER=""".. :author: Continuuity, Inc.
   :version: %(version)s
============================================
Continuuity Reactor %(version)s\
============================================

Continuuity Reactor %(title)s Dependencies
--------------------------------------------

.. rst2pdf: PageBreak
.. rst2pdf: .. contents::

.. rst2pdf: build ../../../developer-guide/licenses-pdf/
.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet

.. csv-table:: **Continuuity Reactor %(title)s Dependencies**
   :header: %(header)s
   :widths: %(widths)s

"""
    sdk_version = get_sdk_version()        
    RST_HEADER = RST_HEADER % {'version': sdk_version, 'title': title, 'header': header, 'widths': widths}
    
    rst_path = os.path.join(SCRIPT_DIR_PATH, LICENSE_MASTERS, file_base + ".rst")
#     data_list = process_singlenode(input_file, options)
    try:
        with open(rst_path,'w') as f:
            f.write(RST_HEADER)
            for row in data_list:
                # Need to substitute quotes for double quotes in reST's csv table format
                row = map(lambda x: x.replace("\"", "\"\""), row)
                f.write(SPACE + '"' + '","'.join(row) + '"\n')
    except:
        raise
    print "Wrote rst file:\n%s" % rst_path


class Library:
    MAX_SIZES={}
    PRINT_ORDER = ['id','jar','base','version','classifier','license','license_url']
    
    def __init__(self, jar, license, license_url):
        self.jar = jar # aka "package"
        self.id = ""
        self.base = ""
        self.version =  ""
        self.classifier = ""
        self.license = license
        self.license_url = license_url
        self._convert_jar()
        self._set_max_sizes()
        
    def __str__(self):
        return "%s : %s" % (self.id, self.jar)

    def _convert_jar(self):
        # Looking for a string of the format "base-version[-classifier].jar"
        # If that fails, tries without the .jar
        # If still no match, uses jar as base instead.
        import re
        s_jar = r'(?P<base>.*?)-(?P<version>\d*[0-9.]*\d+)([.-]*(?P<classifier>.*?))\.jar$'
        s_no_jar = r'(?P<base>.*?)-(?P<version>\d*[0-9.]*\d+)([.-]*(?P<classifier>.*?))'
        try:
            m = re.match( s_jar, self.jar)
            if not m:
                m = re.match( s_no_jar, self.jar)
            if m:
                if m.group('classifier'):
                    c = m.group('classifier')
                else:
                    c = "<none>"
    #             print "%s: %s %s %s" % (jar, m.group('base'), m.group('version'), c )
                self.base = m.group('base')
                self.version =  m.group('version')
                self.classifier = m.group('classifier')
            else:
                self.base = self.jar
            if self.classifier:
                self.id = "%s-%s" % (self.base, self.classifier)
            else:
                self.id = self.base
        except:
            raise

    def _set_max_sizes(self):
        # Used for pretty-printing
        for element in self.__dict__.keys():
            if element[0] != "_":
                length = len(self.__dict__[element])
                if self.MAX_SIZES.has_key(element):
                    length = max(self.MAX_SIZES[element], length)
                self.MAX_SIZES[element] = length

    def pretty_print(self):
        SPACER = 2
        line = ""
        for element in self.PRINT_ORDER:
            if element[0] != "_":
                length = self.MAX_SIZES[element]
                line += self.__dict__[element].ljust(self.MAX_SIZES[element]+ SPACER)
        print line

    def get_row(self):
        return (self.jar, self.version, self.classifier, self.license, self.license_url)

    
#
# Main function
#

def main():
    """ Main program entry point.
    """
    options, input_file = parse_options()

    try:
        options.logger = log
        if options.enterprise:
            process_enterprise(input_file, options)
            
        elif options.level_1:
            process_level_1(input_file, options)
            
        elif options.singlenode:
            process_singlenode(input_file, options)
            
        elif options.rst_enterprise:
            print_rst_enterprise(input_file, options)
            
        elif options.rst_level_1:
            print_rst_level_1(input_file, options)
            
        elif options.rst_singlenode:
            print_rst_singlenode(input_file, options)
            
        elif options.master_print:
            master_print()
            
        else:
            print "Unknown test type: %s" % options.test
            sys.exit(1)
    except Exception, e:
        sys.stderr.write("Error: %s\n" % e)
        sys.exit(1)


if __name__ == '__main__':
    main()
