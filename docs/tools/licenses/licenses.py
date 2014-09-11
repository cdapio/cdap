#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright 2014 Cask Data, Inc.
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
# Usage: python licenses.py 
#
# Note that the level1 dependencies CSV file is assumed to have a header line at start.
# It reads:
# GroupId,ArtifactId


# Changes
# 
# v0.0.5
# Changed to accomodate multiple 


from optparse import OptionParser
import csv
import os
import pickle
import subprocess
import sys
import tempfile

VERSION = "0.0.5"

MASTER = "cdap-dependencies-master.obj"

MASTER_CSV = "cdap-dependencies-master.csv"

ENTERPRISE = "cdap-enterprise-dependencies"
LEVEL_1    = "cdap-level-1-dependencies"
STANDALONE = "cdap-standalone-dependencies"

LICENSES_SOURCE = "../../developer-guide/source/licenses"

SPACE = " "*3
BACK_DASH = "\-"

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

DEBUG = False

def get_sdk_version():
    # Sets the CDAP Build Version via maven
    return "2.5.0"
    mvn_version_cmd = "mvn help:evaluate -o -Dexpression=project.version -f ../../../pom.xml | grep -v '^\['"
    version = None
    try:
        version = subprocess.check_output(mvn_version_cmd, shell=True).strip().replace("-SNAPSHOT", "")
    except:
        print "Could not get version from maven"
        sys.exit(1)
    return version

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

    parser.add_option(
        "-d", "--debug",
        action="store_true",
        dest="debug",
        help="Print debug messages",
        default=False)

    sdk_version = get_sdk_version()
    parser.add_option(
        "-w", "--build_version",
        dest="build_version",
        help="The built version of the CDAP SDK "
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
        "-s", "--standalone",
        action="store_true",
        dest="standalone",
        help="Process standalone dependencies",
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
        "-c", "--rst_standalone",
        action="store_true",
        dest="rst_standalone",
        help="Print standalone dependencies to an rst file",
        default=False)

    parser.add_option(
        "-m", "--master_print",
        action="store_true",
        dest="master_print",
        help="Prints out the master dependency file",
        default=False)

    parser.add_option(
        "-t", "--master_test",
        action="store_true",
        dest="master_test",
        help="Loads and stores the master dependency file",
        default=False)

    (options, args) = parser.parse_args()
    
    global DEBUG
    DEBUG = options.debug
    
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


def load_master_csv():
    # Read in the master csv files and create a dictionary of it
    # Dictionary format:
    #   Keys are the jars
    #   Values are instances of Library class
    # Get the current dependencies master csv file
    # "jar","Version","Classifier","License","License URL"
    # Example csv line:
    # "bonecp-0.8.0.RELEASE.jar","0.8.0","RELEASE","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
    # As of v0.0.5, need to accomodate multiple versions of the same artifacts
    # Changed the master format to be a python object, and save the dict directly
    # Library object changed to accomodate multiple versions
    # 
    # 
    master_libs_dict = {}
    print "Reading master file"
    csv_path = os.path.join(SCRIPT_DIR_PATH, MASTER_CSV)
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count += 1
            jar = row[0]
            if len(row)==5:
                # jar, license, license_url
                lib = Library(row[0], row[3], row[4])
                # Place lib reference in dictionary
                if not master_libs_dict.has_key(lib.id):
                    master_libs_dict[lib.id] = lib
                else:
                    lib.print_duplicate(master_libs_dict)
            else:
                print "%sError with %s\n%srow: %s" % (SPACE, jar, SPACE, row)
                
    # Print out the results
    keys = master_libs_dict.keys()
#     keys.sort()
#     for k in keys:
#         master_libs_dict[k].pretty_print()    
    print "Master CSV: Rows read: %s; Unique Keys created: %s" % (row_count, len(keys))
    return master_libs_dict

def load_master():
    # Look for pickled master
    print "Looking for pickled master file"
    if os.path.exists(os.path.join(SCRIPT_DIR_PATH, MASTER)):
        return read_master_pickle()
    else:
        print "Pickled master file not found"
        print "Looking for CSV master file"
        # If not, use the csv
        return load_master_csv()

def save_master(master_dict):
    # Write out a new master csv file
    import csv
    csv_path = os.path.join(SCRIPT_DIR_PATH, MASTER_CSV + ".new.csv")
    if os.path.isfile(csv_path):
        print "New master CSV: Master file already exists: %s" % csv_path
    else:
        with open(csv_path, 'w') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            keys = master_dict.keys()
            keys.sort()
            for k in keys:
                r = master_dict[k].get_row()
                print_debug(r)
                csv_writer.writerow(r)
        print "New master CSV: wrote %s records to:\n%s" % (len(keys), csv_path)

def save_master_pickle(master_dict):
    # Write out a new master pickled file
    import pickle
    obj_path = os.path.join(SCRIPT_DIR_PATH, MASTER + ".new.obj")
    if os.path.isfile(obj_path):
        print "New master object: Master file already exists: \n%s" % obj_path
    else:
        with open(obj_path, 'w') as obj_file:
            pickle.dump(master_dict, obj_file)
            keys = master_dict.keys()
        print "New master object: wrote %s records to:\n%s" % (len(keys), obj_path)

def read_master_pickle():
    # Read an existing master pickled file
    import pickle
    obj_path = os.path.join(SCRIPT_DIR_PATH, MASTER)
    if not os.path.isfile(obj_path):
        print "Reading master object: Master file doesn't exist: \n%s" % obj_path
        master_dict = {}
    else:
        with open(obj_path, 'r') as obj_file:
            master_dict = pickle.load(obj_file)
            keys = master_dict.keys()
        print "Read master object: read %s records from:\n%s" % (len(keys), obj_path)
    return master_dict

def master_test():
    print "master_test"
    print "load_master"
    master_dict = load_master()
    print "save_master"
    save_master(master_dict)
    save_master_pickle(master_dict)


def master_print():
    master_libs_dict = load_master()
    # Print out the results
    keys = master_libs_dict.keys()
    keys.sort()
    for k in keys:
        master_libs_dict[k].pretty_print()    


def process_enterprise(input_file, options):
    return process_dependencies(ENTERPRISE)


def process_standalone(input_file, options):
    return process_dependencies(STANDALONE)

    
def process_level_1(input_file, options):
    master_libs_dict = load_master()
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

def convert_filename(d):
    # New form of dependency: 
    # antlr.antlr-2.7.7.jar
    # org.apache.spark.spark-core_2.10-1.0.1.jar
    # oss.spec.javax.annotation.jboss-annotations-api_1.1_spec-1.0.1.Final.jar
    # The old jar would be jboss-annotations-api_1.1_spec-1.0.1.Final.jar
    # 
    # find first period before first hyphen before first underscore
    # right-most hyphen (there is always a hyphen before the version)
    # if there is an underscore, it currently is always in the component and could use it
    l = len(d)
    # find first underscore
    u = d.find("_")
    # find right-most hyphen and use it as the marker for the old jar
    h = d.rfind("-")
    if u != -1 and u < h: # an underscore before the right-most hypen
        h = u # Use it as marker instead
    # find first period before first marker
    p = d.rfind(".", 0, h) +1
    return d[p:]
    

def process_dependencies(dependency):
    # Read in the current master file and create a structure with it
    # Structure (master_dict) required: 
    #   A dictionary where the 
    #       keys: string consisting of "group-id.artifact-id"
    #       values: Library instances
    # Read in a new dependencies csv file
    #   File format:
    #       group-id.artifact-id.jar-string
    #   jar-string (or "Package") consists of artifact-id.version."jar"
    #   Example line:
    #       com.github.stephenc.high-scale-lib.high-scale-lib-1.1.1.jar
    # Convert line into components, and look in master_dict for entry
    # If found:
    #   add version (if not there already)
    # If not found:
    #   add a new instance to master_dict
    #   add instance to list to be researched
    # Create and print to standard out the list of the references
    # Print to standard out list of the instances for which links are missing and need to be researched
    # Make a new master list (it may had new versions added)
    # Return "Package","Version","Classifier","License","License URL"
    # "Package" is the same as the jar
    
    master_libs_dict = load_master()
    master_keys = master_libs_dict.keys()
#     master_keys.sort()
#     for k in master_keys:
#         master_libs_dict[k].pretty_print()
#     print_debug("Records: %s" % len(master_keys))  
    
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
            old_jar = convert_filename(jar)
            print '%s\n  old jar: %s' % (jar, old_jar)
            lib = Library(jar, "", "")
            print 'lib.id %s' % lib.id
            # Look up lib reference in master dictionary; 
            # if not there, try the old jar format
            # if there, replace it;
            # if still not there, it might be a different version; need to search for it.
            # if still not there,
            # create a new one
            if master_libs_dict.has_key(lib.id):
                pass
            elif master_libs_dict.has_key(old_jar):
                print "Converting key: %s to %s" % (old_jar, jar)
                lib = master_libs_dict[lib.id]
                del master_libs_dict[lib.id]
                lib.id = jar
                master_libs_dict[lib.id] = lib
            else:
                # Look if any of the master_keys are in the jar
                for k in master_keys:
                    if jar.endswith(k):
                        # Use this key
                        print "Converting key: %s to %s" % (k, jar)
                        lib = master_libs_dict[k]
                        del master_libs_dict[k]
                        lib.id = jar
                        master_libs_dict[lib.id] = lib
                        break
            
            master_libs_dict[lib.id] = lib
            missing_libs_dict[lib.id] = lib

            # Place lib reference in new libs dictionary
            if not new_libs_dict.has_key(lib.id):
                new_libs_dict[lib.id] = master_libs_dict[lib.id]
            else:
                lib.print_duplicate(new_libs_dict)
    
    for lib_dict in [master_libs_dict]:
        keys = lib_dict.keys()
        keys.sort()
        missing_licenses = 0
        for k in keys:
            if lib_dict[k].license == "":
                missing_licenses += 1
            if DEBUG:
                lib_dict[k].pretty_print()
        print_debug("Records: %s" % len(keys))

    print "New CSV: Rows: %s" % len(new_libs_dict.keys())
    print "New Master CSV: Rows: %s" % len(master_libs_dict.keys())
    missing_entries = len(missing_libs_dict.keys())
    print "New Master CSV: Missing Entry Rows: %s" % missing_entries
    print "New Master CSV: Missing License Rows: %s" % missing_licenses

    # Write out a new master csv file 
    save_master(master_libs_dict)

    # Return the "Package","Version","Classifier","License","License URL"
    # Need to insert BACK_DASH if an empty string, otherwise rst won't work
    rst_data = []
    keys = new_libs_dict.keys()
    keys.sort()
    for k in keys:
        lib = new_libs_dict[k]
        row = list(lib.get_row())
        if row[2] == "":
            row[2] = BACK_DASH
            print_debug(row)
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


def print_rst_standalone(input_file, options):
    title = "Standalone"
    file_base = STANDALONE
    header = '"Package","Version","Classifier","License","License URL"'
    widths = "20, 10, 10, 20, 30"
    data_list = process_standalone(input_file, options)
    print_dependencies(title, file_base, header, widths, data_list)

   
def print_dependencies(title, file_base, header, widths, data_list):
# Example: "Level 1", LEVEL_1, ...
    RST_HEADER=""".. :author: Cask Data, Inc.
   :version: %(version)s
============================================
Cask Data Application Platform %(version)s\
============================================

Cask Data Application Platform %(title)s Dependencies
-----------------------------------------------------

.. rst2pdf: PageBreak
.. rst2pdf: .. contents::

.. rst2pdf: build ../../../developer-guide/licenses-pdf/
.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet

.. csv-table:: **Cask Data Application Platform %(title)s Dependencies**
   :header: %(header)s
   :widths: %(widths)s

"""
    sdk_version = get_sdk_version()        
    RST_HEADER = RST_HEADER % {'version': sdk_version, 'title': title, 'header': header, 'widths': widths}
    rst_path = os.path.join(SCRIPT_DIR_PATH, file_base + ".rst")

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

def print_debug(message):
    if DEBUG:
        print message


class Library:
    # version 2
    MAX_SIZES={}
    PRINT_ORDER = ['id','jar','base','version','classifier','license','license_url']
    SPACE = " "*3
    
    def __init__(self, jar, license, license_url):
        self.id = ""
        self.base = ""
        self.classifier = ""
        self.jar = jar # aka package
        self.version = ""
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
        # Converts the jar into its component parts: base, version, classifier
        # Sets the id as either the base or combination of base and classifier
        import re
        jar = self.jar
        s_jar = r'(?P<base>.*?)-(?P<version>\d*[0-9.]*\d+)([.-]*(?P<classifier>.*?))\.jar$'
        s_no_jar = r'(?P<base>.*?)-(?P<version>\d*[0-9.]*\d+)([.-]*(?P<classifier>.*?))'
        try:
            m = re.match( s_jar, jar)
            if not m:
                m = re.match( s_no_jar, jar)
            if m:
                if m.group('classifier'):
                    c = m.group('classifier')
                else:
                    c = "<none>"
                    print_debug("%s: %s %s %s" % (jar, m.group('base'), m.group('version'), c ))
                self.base = m.group('base')
                self.version =  m.group('version')
                self.classifier = m.group('classifier')
            else:
                self.base = jar
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

    def print_duplicate(self, lib_dict):
        print "Duplicate key: %s" % self.id
        print "%sCurrent library: %s" % (self.SPACE, lib_dict[self.id])
        print "%sNew library:     %s" % (self.SPACE, self)
    
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
            
        elif options.standalone:
            process_standalone(input_file, options)
            
        elif options.rst_enterprise:
            print_rst_enterprise(input_file, options)
            
        elif options.rst_level_1:
            print_rst_level_1(input_file, options)
            
        elif options.rst_standalone:
            print_rst_standalone(input_file, options)
            
        elif options.master_print:
            master_print()
            
        elif options.master_test:
            master_test()
            
        else:
            print "Unknown test type: %s" % options.test
            sys.exit(1)
    except Exception, e:
        sys.stderr.write("Error: %s\n" % e)
        sys.exit(1)


if __name__ == '__main__':
    main()
