#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright © 2014-2016 Cask Data, Inc.
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

# Checks that the license dependencies files used match the dependencies in the product.
# Run this script after building the SDK.
# Usage: python licenses.py 

import csv
import json
import os
import subprocess
import sys
import traceback

from optparse import OptionParser
from pprint import pprint

VERSION = '0.1.1'

COPYRIGHT_YEAR = '2016'

MASTER_CSV = 'cdap-dependencies-master.csv'
MASTER_CSV_COMMENTS = {'bower': """# Bower Dependencies
# dependency,version,type,license,license_url,homepage,license_page
""",
                       'npm': """#
# NPM Dependencies
# dependency,version,type,license,license_url,homepage,license_page
""",
                       'jar': """#
# Jar Dependencies
# dependency,version,type,license,license_url
""",
}
MASTER_CSV_TYPES = ('bower', 'npm', 'jar')

ENTERPRISE = 'cdap-enterprise-dependencies'
LEVEL_1    = 'cdap-level-1-dependencies'
STANDALONE = 'cdap-standalone-dependencies'
CDAP_UI    = 'cdap-ui-dependencies'

CASK_REVERSE_DOMAIN = 'co.cask'

LICENSES_SOURCE = '../../reference-manual/source/licenses'

CDAP_BOWER_DEPENDENCIES = ('../../../cdap-ui/bower.json', '#')
CDAP_NPM_DEPENDENCIES = ('../../../cdap-ui/package.json', '@')

CDAP_UI_SOURCES = {'bower': CDAP_BOWER_DEPENDENCIES, 'npm': CDAP_NPM_DEPENDENCIES}

CDAP_UI_CASK_DEPENDENCIES = u'cask-'
CDAP_UI_DEPENDENCIES_KEY = 'dependencies'
MIT_LICENSE = "'MIT'"

SPACE = ' '*3
BACK_DASH = '\-'

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

DEBUG = False
QUIET = False

def startup_checks():
    from datetime import date
    current_year = str(date.today().year)
    if current_year != COPYRIGHT_YEAR:
        print "\nWARNING: COPYRIGHT_YEAR of %s does not match current year of %s\n" % (COPYRIGHT_YEAR, current_year)

def get_sdk_version():
    # Sets the Build Version
    grep_version_cmd = "grep '<version>' ../../../pom.xml | awk 'NR==1;START{print $1}'"
    version = None
    try:
        # Python 2.6 commands
        p1 = subprocess.Popen(['grep' , '<version>', '../../../pom.xml' ], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['awk', 'NR==1;START{print $1}'], stdin=p1.stdout, stdout=subprocess.PIPE)
        full_version_temp = p2.communicate()[0]
        # Python 2.6 command end
#         full_version = subprocess.check_output(grep_version_cmd, shell=True).strip().replace('<version>', '').replace('</version>', '')
        full_version = full_version_temp.strip().replace('<version>', '').replace('</version>', '')
        version = full_version.replace('-SNAPSHOT', '')
    except:
        print '\nWARNING: Could not get version using grep\n'
        raise
    return version

def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [options] [file]",
        description='Checks that the license dependencies files used match the dependencies in the product.')

    parser.add_option(
        '-v', '--version',
        action='store_true',
        dest='version',
        help='Version of this software',
        default=False)

    parser.add_option(
        '-z', '--debug',
        action='store_true',
        dest='debug',
        help='Print debug messages',
        default=False)

    sdk_version = get_sdk_version()
    parser.add_option(
        '-w', '--build_version',
        dest='build_version',
        help='The built version of the CDAP SDK '
             "(default: %s)" % sdk_version,
        default=sdk_version)

    parser.add_option(
        '-u', '--ui',
        action='store_true',
        dest='cdap_ui',
        help='Process CDAP UI dependencies',
        default=False)

    parser.add_option(
        '-e', '--enterprise',
        action='store_true',
        dest='enterprise',
        help='Process enterprise dependencies',
        default=False)

    parser.add_option(
        '-l', '--level_1',
        action='store_true',
        dest='level_1',
        help='Process level 1 dependencies',
        default=False)

    parser.add_option(
        '-s', '--standalone',
        action='store_true',
        dest='standalone',
        help='Process standalone dependencies',
        default=False)

    parser.add_option(
        '-a', '--rst_enterprise',
        action='store_true',
        dest='rst_enterprise',
        help='Print enterprise dependencies to an rst file',
        default=False)

    parser.add_option(
        '-b', '--rst_level_1',
        action='store_true',
        dest='rst_level_1',
        help='Print level1 dependencies to an rst file',
        default=False)

    parser.add_option(
        '-c', '--rst_standalone',
        action='store_true',
        dest='rst_standalone',
        help='Print standalone dependencies to an rst file',
        default=False)

    parser.add_option(
        '-d', '--rst_cdap_ui',
        action='store_true',
        dest='rst_cdap_ui',
        help='Print CDAP UI dependencies to an rst file',
        default=False)

    parser.add_option(
        '-m', '--master_print',
        action='store_true',
        dest='master_print_terminal',
        help='Prints to terminal the master dependency file',
        default=False)

    parser.add_option(
        '-p', '--print',
        action='store_true',
        dest='master_print_file',
        help='Prints to file a new master dependency file',
        default=False)

    parser.add_option(
        '-t', '--list_special',
        action='store_true',
        dest='list_special',
        help='Lists dependencies that require special handling (typically those not Apache or MIT licenses)',
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
    (sys.stdout if type == 'notice' else sys.stderr).write(message + '\n')


def process_master():
    # Read in the master csv files and create a dictionary of it
    # Contains both Jars and Bower dependencies
    # Jar dependencies:
    #   Keys are the jars, Values are the Library instances
    #   'jar','Version','Classifier','License','License URL'
    #   Example:
    #       'bonecp-0.8.0.RELEASE.jar','0.8.0','RELEASE','Apache License, Version 2.0','http://www.apache.org/licenses/LICENSE-2.0.html'
    # NPM & Bower dependencies:
    #   Keys are the dependencies, Values are the Library instances
    #   'dependency','version','homepage','license','license_url', 'type'
    #   dependency,version,type,license,license_url,homepage,license_page
    #   Example:
    #       'angular','1.3.15','bower','MIT License','http://opensource.org/licenses/MIT','https://github.com/angular/bower-angular','https://github.com/angular/angular.js/blob/master/LICENSE'
    # Get the current dependencies master csv file
    #
    # As of version 0.1.1
    # Jar dependencies can now have a license_source_url, which is the URL where the license was determined.
    master_libs_dict = {}
    csv_path = os.path.join(SCRIPT_DIR_PATH, MASTER_CSV)
    print_quiet("Reading master file: %s" % csv_path)
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        comment_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count += 1
            dependency = row[0]
            if dependency.startswith('#'):
                # Comment line
                comment_count += 1
            elif len(row) in (5, 6, 7):
                if len(row) in (5, 6):
                    license_source_url = row[5] if len(row)==6 else None
                    lib = Library(dependency, row[3], row[4], license_source_url)
                else:
                    # len(row)==7:
                    lib = UI_Library(*row)
                # Place lib reference in dictionary
                if not master_libs_dict.has_key(lib.id):
                    master_libs_dict[lib.dependency] = lib
                else:
                    lib.print_duplicate(master_libs_dict)
            else:
                print "%sError with %s\n%srow: %s" % (SPACE, dependency, SPACE, row)
                
    # Print out the results
    keys = master_libs_dict.keys()
#     keys.sort()
#     for k in keys:
#         master_libs_dict[k].pretty_print()    
    print_quiet("Master CSV: Rows read: %s (comments: %s); Unique Keys created: %s" % (row_count, comment_count, len(keys)))
    return master_libs_dict

    
def master_print_terminal():
    master_libs_dict = process_master()
    # Print out the results
    keys = master_libs_dict.keys()
    keys.sort()
    max_characters = len("%d" % len(keys)) # set max_characters to maximum number of characters in keys
    i = 0
    for type in MASTER_CSV_TYPES:
        print MASTER_CSV_COMMENTS[type]
        for k in keys:
            if master_libs_dict[k].type == type:
                i += 1
                master_libs_dict[k].pretty_print(i, max_characters)    
#     for k in keys:
#         print "key: %s %s" % (k, master_libs_dict[k])
#         master_libs_dict[k].pretty_print()    


def test_for_application(app):
    results = subprocess.call(['which', app])
    if results == 0:
        print "Found executable for '%s'" % app
    else: 
        message = "No executable for '%s'" % app
        if app == 'bower':
            print "%s; install bower using: npm install -g bower" % message
        elif app == 'npm':
            print "%s; need to install npm to run license software" % message
        else:
            print message
    return not results


def process_cdap_ui(options):
    # Read in the current master csv file and create a structure with it
    # Read in the checked-in dependency files:
    #   cdap-ui/bower.json
    #   cdap-ui/package.json
    # Create and print to standard out the list of the references
    # Make a list of the references for which links are missing and need to be added to the master
    # Creates a new master list
    # Return a list:
    #   'Dependency','Version','homepage','License','License URL','type'
    # Versioning syntax: see https://nodesource.com/blog/semver-tilde-and-caret
    master_libs_dict = process_master()
    cdap_ui_dict = {}
    missing_libs_dict = {}
    new_versions_dict = {}
    
    print_quiet()

    for type in CDAP_UI_SOURCES.keys():
        source = CDAP_UI_SOURCES[type][0]
        json_path = os.path.join(SCRIPT_DIR_PATH, source)
        print_quiet("Reading '%s' dependencies file:\n%s" % (type, json_path))
        with open(json_path) as data_file:    
            data = json.load(data_file)
            
        if CDAP_UI_DEPENDENCIES_KEY in data.keys():
            for dependency in data[CDAP_UI_DEPENDENCIES_KEY]:
                if not dependency.startswith(CDAP_UI_CASK_DEPENDENCIES):
                    version = data[CDAP_UI_DEPENDENCIES_KEY][dependency]
                    if master_libs_dict.has_key(dependency):
                        # Look up reference in dictionary
#                         cdap_ui_dict[dependency] = master_libs_dict[dependency]
                        # Compare versions
                        # TO-DO: if versions differ by a prefix (~ or ^) this comparison fails
                        # need to strip off the prefix for the comparison but retain it
                        # perhaps add a function that does the comparison
                        if master_libs_dict[dependency].version != version:
                            if new_versions_dict.has_key(dependency):
                                print_quiet("Dependency already in new versions: %s current: %s new: %s newer: %s" % (dependency, 
                                    master_libs_dict[dependency].version, new_versions_dict[dependency], version))
                            else:
                                print_quiet("New version: %s for %s (old %s)" % (version, dependency, master_libs_dict[dependency].version))
                                new_versions_dict[dependency]=version
                                master_libs_dict[dependency].version = version
                        cdap_ui_dict[dependency] = master_libs_dict[dependency]
                        
                    else:
                        missing_libs_dict[dependency] = (type, version)

    keys = new_versions_dict.keys()
    count_new = len(keys)
    if count_new:
        print_quiet("\nCDAP UI: New Versions: %s" % count_new)
        keys.sort()
        for key in keys:
            print_quiet("%s : current: %s new: %s" % (key, master_libs_dict[key].version, new_versions_dict[key]))
        
    keys = missing_libs_dict.keys()
    count_missing = len(keys)
    print_quiet("\nCDAP UI: Missing Artifacts: %s" % count_missing)
    if count_missing:
        all_apps_available = True
        type_keys = CDAP_UI_SOURCES.keys()
        for type in type_keys:
            if not test_for_application(type):
                all_apps_available = False
                
        if all_apps_available: 
            keys.sort()
            missing_list = []
            for dependency in keys:
                type, version = missing_libs_dict[dependency]
                if type in type_keys:
                    dependency_version = "%s%s%s" % (dependency, CDAP_UI_SOURCES[type][1], version)
                    print_quiet(dependency_version)
                    if type == 'bower':
                        p1 = subprocess.Popen([type, 'info', dependency_version, 'homepage' ], stdout=subprocess.PIPE)
                        homepage = p1.communicate()[0].strip().split('\n')[-1:][0].replace("'", '')
                    elif type == 'npm':
                        p1 = subprocess.Popen([type, '--json', 'view', dependency_version, 'homepage' ], stdout=subprocess.PIPE)
                        homepage = p1.communicate()[0].strip().split('\n')[-1:][0]
                        homepage = homepage.split(' ')[-1:][0].replace('"', '')
                    row = '"%s","%s","%s","","","%s",""' % (dependency, version, type, homepage)
                    missing_list.append(row)
                else:
                    print_quiet("Unknown type: '%s' for dependency '%s', version '%s'" % (type, dependency, version))
        
            print_quiet('\nCDAP UI: Missing Artifacts List:')
            for row in missing_list:
                print_quiet(row)
        
            if options.debug:
                for dependency in keys:
                    version = missing_libs_dict[dependency]
                    dependency_version = "%s#%s" % (dependency, version)
                    print_quiet(dependency_version)
                    p1 = subprocess.Popen(['bower', 'info', dependency_version, 'license' ], stdout=subprocess.PIPE)
                    results = p1.communicate()[0]
                    if results.count(MIT_LICENSE):
                        # Includes MIT License
                        print_quiet('MIT License\n')
                    else:
                        # Unknown license
                        p1 = subprocess.Popen(['bower', 'info', dependency_version], stdout=subprocess.PIPE)
                        results = p1.communicate()[0]
                        print_quiet("Debug:\n%s" % results)
                        p1 = subprocess.Popen(['bower', 'home', dependency_version], stdout=subprocess.PIPE)        
        
    print_quiet("\nCDAP UI: Row count: %s" % len(cdap_ui_dict.keys()))

    # Return 'Dependency','Version', 'Type','License','License URL', 'License Source URL'
    cdap_ui_data = []
    keys = cdap_ui_dict.keys()
    keys.sort()
    for dependency in keys:
        lib = cdap_ui_dict[dependency]
        row = list(lib.get_row())
        cdap_ui_data.append(UILicense(*row))
        print_quiet("%s : %s" % (dependency, row))
        
    # Print new master entries
    print_quiet("\nNew Master Versions\n")
    keys = master_libs_dict.keys()
    keys.sort()
    for type in ['bower', 'npm']:
        print_quiet("# %s Dependencies\n# dependency,version,type,license,license_url,homepage,license_page" % type)
        for dependency in keys:
            lib = master_libs_dict[dependency]
            row = list(lib.get_row())
            if row[2] == type:
                print_quiet("%s : %s" % (dependency, row))
        print_quiet()

    # Write out a new master csv file, only if not already exists 
    if count_new or count_missing:
        write_new_master_csv_file(master_libs_dict)
    
    return cdap_ui_data

def process_level_1(input_file, options):
    master_libs_dict = process_master()
    level_1_dict = {}
    missing_libs_dict = {}
    csv_path = os.path.join(SCRIPT_DIR_PATH, LICENSES_SOURCE, LEVEL_1 + '.csv')
    print_quiet("Reading dependencies file:\n%s" % csv_path)
    import csv
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        unique_row_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count +=1
            jar,group_id, artifact_id = row
            key = jar
            if not level_1_dict.has_key(key):
                unique_row_count += 1
                if master_libs_dict.has_key(jar):
                    # Look up jar reference in dictionary
                    lib = master_libs_dict[jar]
                    print_quiet("lib.jar %s" % lib.jar)
                    level_1_dict[key] = (group_id, artifact_id, lib.license, lib.license_url)
                    continue
                if not missing_libs_dict.has_key(artifact_id) and not jar.startswith(CASK_REVERSE_DOMAIN):
                    missing_libs_dict[artifact_id] = jar

    print_quiet("Level 1: Row count: %s" % row_count)
    print_quiet("Level 1: Unique Row count: %s" % unique_row_count)
    print_quiet("Level 1: Missing Artifacts: %s" % len(missing_libs_dict.keys()))
    
    if len(missing_libs_dict.keys()) > 0:
        for key in missing_libs_dict.keys():
            print_quiet("Missing artifact_id: %s (for %s)" % (key, missing_libs_dict[key]))
        print_quiet('Add these lines to the Master file:')
        for key in missing_libs_dict.keys():
            print_quiet('"%s","","","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"' % missing_libs_dict[key])

    # Return the 'Package','Artifact','License','License URL'
    rst_data = []
    keys = level_1_dict.keys()
    keys.sort()
    for k in keys:
        row = level_1_dict[k]
        rst_data.append(Level1License(*row))
    return rst_data

def process_enterprise(input_file, options):
    return _process_dependencies(ENTERPRISE)

def process_standalone(input_file, options):
    return _process_dependencies(STANDALONE)
    
def _process_dependencies(dependency):
    # Read in the current master csv file and create a structure with it
    # Read in the new dependencies csv file
    # Create and print to standard out the list of the references
    # Make a list of the references for which links are missing and need to be added to the master
    # Make a new master list, if one does not already exist
    # Return 'Package','Version','Classifier','License','License URL'
    
    master_libs_dict = process_master()
    
    # Read dependencies
    new_libs_dict = {}
    missing_libs_dict = {}
    csv_path = os.path.join(SCRIPT_DIR_PATH, LICENSES_SOURCE, dependency + '.csv')
    print_quiet("Reading dependencies file:\n%s" % csv_path)
    import csv
    with open(csv_path, 'rb') as csvfile:
        row_count = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            row_count += 1
            jar = row[0]
            lib = Library(row[0], '', '')
            print_quiet("lib.jar %s" % lib.jar)
            # Look up lib reference in master dictionary; if not there, add it
            # if a CDAP jar, ignore it
            if not master_libs_dict.has_key(lib.jar):
                print_quiet(lib.jar + ' Not Present in Master')
                if lib.jar.startswith('cdap-'):
                    print_quiet('  Skipping')
                    continue
                else:
                    master_libs_dict[lib.jar] = lib
                    missing_libs_dict[lib.jar] = lib
                    print_quiet('  Adding to missing libs')
            # Place lib reference in dictionary
            if not new_libs_dict.has_key(lib.jar):
                new_libs_dict[lib.jar] = master_libs_dict[lib.jar]
            else:
                lib.print_duplicate(new_libs_dict)

    missing_entries = len(missing_libs_dict.keys())
    for lib_dict in [master_libs_dict]:
        keys = lib_dict.keys()
        keys.sort()
        missing_licenses = []
        for k in keys:
            if lib_dict[k].license == '':
                missing_licenses.append(lib_dict[k])
            if DEBUG:
                lib_dict[k].pretty_print()
        print_debug("Records: %s" % len(keys))

    print_quiet("New CSV: Rows: %s" % len(new_libs_dict.keys()))
    print_quiet("New Master CSV: Rows: %s" % len(master_libs_dict.keys()))
    print_quiet("New Master CSV: Missing License Rows: %s" % len(missing_licenses))
    print_quiet("New Master CSV: Missing Entry Rows: %s" % missing_entries)
    if missing_licenses:
        print_quiet('Missing Licenses')
        for miss in missing_licenses:
            print "  %s" % miss
    
    if missing_entries:
        print_quiet('Missing Entries')
        i = 0
        for key in missing_libs_dict.keys():
            i += 1
            print "  %2d: %s" % (i, missing_libs_dict[key])

    # Write out a new master csv file, only if not already exists 
    if missing_entries or missing_licenses:
        write_new_master_csv_file(master_libs_dict)

    # Return the 'Package','Version','Classifier','License','License URL'
    rst_data = []
    keys = new_libs_dict.keys()
    keys.sort()
    for k in keys:
        lib = new_libs_dict[k]
        row = list(lib.get_row())
        if row[2] == '':
            row[2] = BACK_DASH
            print_debug(row)
        rst_data.append(License(*row))
    return rst_data

def write_new_master_csv_file(lib_dict):
    print 'Creating new Master CSV file'
    import csv
    csv.register_dialect('masterCSV', delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')

    csv_path = os.path.join(SCRIPT_DIR_PATH, MASTER_CSV)
    backup_csv_path = os.path.join(SCRIPT_DIR_PATH, MASTER_CSV + '.bu.csv')
    if os.path.isfile(backup_csv_path):
        print "Backup Master CSV: Exiting, as backup Master file already exists: %s" % backup_csv_path
        return
    if os.path.isfile(csv_path):
        try:
            import shutil
            shutil.move(csv_path, backup_csv_path)
            print "Created Backup Master CSV at: %s" % backup_csv_path
        except:
            print "Backup Master CSV: Exiting, as unable to create backup Master: %s" % backup_csv_path
            return
    
    if os.path.isfile(csv_path):
        print "New Master CSV: Exiting, as new Master file already exists: %s" % csv_path
    else:
        csv_file = None
        try:
            csv_file = open(csv_path, 'w')
            csv_writer = csv.writer(csv_file, 'masterCSV')
            keys = lib_dict.keys()
            keys.sort()
            i = 0
            for type in MASTER_CSV_TYPES:
                csv_file.write(MASTER_CSV_COMMENTS[type])
                for k in keys:
                    r = lib_dict[k].get_full_row()
                    row_type = lib_dict[k].type
                    if row_type == type:
                        i += 1
                        csv_writer.writerow(r)
        finally:
            if csv_file is not None:
                csv_file.close()
            else:
                print "Unable to close New Master CSV: %s" % csv_path
            
        print "New Master CSV: wrote %s records of %s to: %s" % (i, len(keys), csv_path)

def master_print_file():
    master_libs_dict = process_master()
    write_new_master_csv_file(master_libs_dict)
    
def print_rst_level_1(input_file, options):
    title = 'Level 1'
    file_base = LEVEL_1
    header = '"Package","Artifact","License","License URL"'
    widths = '20, 20, 20, 40'
    data_list = process_level_1(input_file, options)
    _print_dependencies(title, file_base, header, widths, data_list)

def print_rst_enterprise(input_file, options):
    title = 'Distributed'
    file_base = ENTERPRISE
    header = '"Package","Version","Classifier","License","License URL"'
    widths = '20, 10, 10, 20, 35'
    data_list = process_enterprise(input_file, options)
    _print_dependencies(title, file_base, header, widths, data_list)

def print_rst_standalone(input_file, options):
    title = 'Standalone'
    file_base = STANDALONE
    header = '"Package","Version","Classifier","License","License URL"'
    widths = '20, 10, 10, 20, 30'
    data_list = process_standalone(input_file, options)
    _print_dependencies(title, file_base, header, widths, data_list)

def print_rst_cdap_ui(options):
    title = 'UI'
    file_base = CDAP_UI
    header = '"Dependency","Version","Type","License","License Source URL"'
    widths = '20, 10, 10, 20, 40'
    data_list = process_cdap_ui(options)
    print
    _print_dependencies(title, file_base, header, widths, data_list)

def _print_dependencies(title, file_base, header, widths, data_list):
# Example: 'Level 1', LEVEL_1, ...
    RST_HEADER=""".. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © %(year)s Cask Data, Inc.
    :version: %(version)s

=================================================
Cask Data Application Platform |version|
=================================================

Cask Data Application Platform %(title)s Dependencies
--------------------------------------------------------------------------------

.. rst2pdf: PageBreak
.. rst2pdf: .. contents::

.. rst2pdf: build ../../../reference/licenses-pdf/
.. rst2pdf: config ../../../_common/_templates/pdf-config
.. rst2pdf: stylesheets ../../../_common/_templates/pdf-stylesheet

.. csv-table:: **Cask Data Application Platform %(title)s Dependencies**
   :header: %(header)s
   :widths: %(widths)s

"""
    sdk_version = get_sdk_version()
    if sdk_version:
        RST_HEADER = RST_HEADER % {'version': sdk_version, 'title': title, 'header': header, 'widths': widths, 'year': COPYRIGHT_YEAR}
        rst_path = os.path.join(SCRIPT_DIR_PATH, file_base + '.rst')

        try:
            with open(rst_path,'w') as f:
                f.write(RST_HEADER)
                for data in data_list:
                    f.write(data.rst_csv())
        except:
            raise
        print "Wrote rst file:\n%s" % rst_path
    else:
        print "Unable to get SDK Version from Grep"
        
def print_debug(message=''):
    if DEBUG:
        print message

def print_quiet(message=''):
    if not QUIET:
        print message

def list_special(input_file, options):
    global QUIET
    QUIET = True
    LICENSE_LIST = ['Apache License, Version 2.0', 'MIT License', 'MIT+GPLv2', 'Public Domain',]

    data_dict = dict()
    data_list = process_enterprise(input_file, options) + process_standalone(input_file, options) + process_cdap_ui(options)
    for data in data_list:
        library = data.get_library()
        if library.startswith('com.sun.'):
            library = library[len('com.sun.'):]
        elif not library.startswith('javax'):
            library = data.get_short_library()
        if data.license not in LICENSE_LIST and library not in data_dict:
            data_dict[library] = data

    print "Listing special dependencies that require handling:\n"
    libraries = data_dict.keys()
    libraries.sort()
    incomplete = 0
    for library in libraries:
        data = data_dict[library]
        if data.license_source_url:
            license_url = data.license_source_url
            flag = ''
        else:
            license_url = "[%s %s]" % (data.id, data.license_url)
            flag = '* '
            incomplete += 1
        print "%slibrary: %s '%s' %s" % (flag, library, data.license, license_url)

    print "\nLibraries: %s (incomplete: %s)\n" % (len(libraries), incomplete)


class License:

    SPACE_3 = ' '*3
    
    def __init__(self, package, version, classifier, license, license_url, license_source_url=None):
        self.id = package
        self.package = package # aka 'jar' aka 'package' aka 'dependency'
        self.version = version
        self.classifier = classifier
        self.license = license
        self.license_url = license_url
        self.license_source_url =  license_source_url

    def __str__(self):
        return "%s : %s %s %s %s %s " % (self.package, self.version, self.classifier, self.license, self.license_url, self.license_source_url)

    def _get_row(self):
        return (self.package, self.version, self.classifier, self.license, self.license_url)

    def get_library(self):
        # The 'library' is the package less the version and classifier
        if self.classifier == '\-':
            library = self.package[:-len("-%s.jar" % self.version)]
        elif self.classifier:
            library = self.package[:-len("-%s.%s.jar" % (self.version, self.classifier))]
        return library
    
    def get_short_library(self):
        library = self.get_library()
        if library.count('.') < 2:
            return library
        else:
            # Slice off the right-most two parts of the string
            return library[library.rfind('.', 0, library.rfind('.'))+1:]

    def rst_csv(self):
        # Need to substitute quotes for double quotes in reST's csv table format
        row = map(lambda x: x.replace('\"', '\"\"'), self._get_row())
        return self.SPACE_3 + '"' + '","'.join(row) + '"\n'


class Level1License(License):

    def __init__(self, package, artifact, license, license_url, license_source_url=None):
        self.id = package
        self.package = package # aka 'jar' aka 'package' aka 'dependency'
        self.artifact = artifact
        self.license = license
        self.license_url = license_url
        self.license_source_url =  license_source_url

    def __str__(self):
        return "%s : %s %s %s %s " % (self.package, self.artifact, self.license, self.license_url, self.license_source_url)

    def _get_row(self):
        return (self.package, self.artifact, self.license, self.license_url)

    def get_library(self):
        return "%s.%s" % (self.package, self.artifact)


class UILicense(License):

    def __init__(self, dependency, version, type, license, license_url, license_source_url=None):
        self.id = dependency
        self.dependency = dependency # aka 'jar' aka 'package' aka 'dependency'
        self.version = version
        self.type = type
        self.license = license
        self.license_url = license_url
        self.license_source_url =  license_source_url

    def __str__(self):
        return "%s : %s %s %s %s %s " % (self.dependency, self.version, self.type, self.license, self.license_url, self.license_source_url)

    def _get_row(self):
        linked_license = "`%s <%s>`__" % (self.license, self.license_url)
        return (self.dependency, self.version, self.type, linked_license, self.license_source_url)

    def get_library(self):
        return self.dependency


class Library:
    MAX_SIZES={}
    PRINT_ORDER = ['id','jar','base','version','classifier','license','license_url', 'license_source_url']
    SPACE = ' '*3
    
    def __init__(self, jar, license, license_url, license_source_url=None):
        self.jar = jar # aka 'package' aka 'dependency'
        self.dependency = jar
        self.id = ''
        self.base = ''
        self.version =  ''
        self.type = 'jar'
        self.classifier = ''
        self.license = license
        self.license_url = license_url
        self.license_source_url = license_source_url
        self._initialize()
        
    def __str__(self):
        return "%s : %s" % (self.id, self.jar)

    def _convert_jar(self):
        # Looking for a string of the format 'base-version[-classifier].jar'
        # If that fails, tries without the .jar
        # If still no match, uses jar as base instead.
        # Converts the jar into its component parts: base, version, classifier
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
                    c = '<none>'
                    print_debug("%s: %s %s %s" % (self.jar, m.group('base'), m.group('version'), c ))
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
            if element[0] != '_':
                length = len(self.__dict__[element]) if self.__dict__[element] else 0
                if self.MAX_SIZES.has_key(element):
                    length = max(self.MAX_SIZES[element], length)
                self.MAX_SIZES[element] = length

    def _initialize(self):
        self._convert_jar()
        self._set_max_sizes()
    
    def pretty_print(self, i=0, digits=3):
        SPACER = 1
        line = ''
        for element in self.PRINT_ORDER:
            if element[0] != '_':
                length = self.MAX_SIZES[element]
                line += self.__dict__[element].ljust(self.MAX_SIZES[element]+ SPACER)
        if i != 0:
            format = "%%%dd:%%s" % digits
            line = format % (i, line)
        print line

    def get_row(self):
#         license_source_url = self.license_source_url if self.license_source_url else self.license_url        
        return (self.jar, self.version, self.classifier, self.license, self.license_url, self.license_source_url)

    def get_full_row(self):
        license_source_url = self.license_source_url if self.license_source_url else ''        
        return (self.jar, self.version, self.classifier, self.license, self.license_url, license_source_url)

    def print_duplicate(self, lib_dict):
        print "Duplicate key: %s" % self.id
        print "%sCurrent library: %s" % (self.SPACE, lib_dict[self.id])
        print "%sNew library:     %s" % (self.SPACE, self)


class UI_Library(Library):
    PRINT_ORDER = ['dependency','version','type','license','license_url','homepage','license_page']
    
    def __init__(self, id, version, type, license, license_url, homepage, license_page):
        self.id = id
        self.dependency = id
        self.version = version
        self.type = type
        self.license =  license
        self.license_url = license_url
        self.homepage = homepage
        self.license_page = license_page
        self._initialize()

    def __str__(self):
        return "%s : %s (%s)" % (self.id, self.version, self.type)

    def _initialize(self):
        self._set_max_sizes()

    def get_row(self):
        license_source_url = self.license_page if self.license_page else self.homepage
        return (self.id, self.version, self.type, self.license, self.license_url, license_source_url)

    def get_full_row(self):
        return (self.id, self.version, self.type, self.license, self.license_url, self.homepage, self.license_page)


#
# Main function
#
def main():
    """ Main program entry point.
    """
    startup_checks()
    options, input_file = parse_options()

    try:
        options.logger = log

        if options.cdap_ui:
            process_cdap_ui(options)

        if options.enterprise:
            process_enterprise(input_file, options)
            
        if options.level_1:
            process_level_1(input_file, options)
            
        if options.standalone:
            process_standalone(input_file, options)
            
        if options.rst_enterprise:
            print_rst_enterprise(input_file, options)
            
        if options.rst_level_1:
            print_rst_level_1(input_file, options)
            
        if options.rst_standalone:
            print_rst_standalone(input_file, options)
            
        if options.rst_cdap_ui:
            print_rst_cdap_ui(options)
            
        if options.master_print_terminal:
            master_print_terminal()

        if options.master_print_file:
            master_print_file()

        if options.list_special:
            list_special(input_file, options)

    except Exception, e:
        try:
            exc_info = sys.exc_info()
        finally:
            # Display the *original* exception
            traceback.print_exception(*exc_info)
            del exc_info
            sys.stderr.write('Error: %s\n' % e)
            sys.exit(1)

if __name__ == '__main__':
    main()
