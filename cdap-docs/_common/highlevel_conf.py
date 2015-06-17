# -*- coding: utf-8 -*-

# Copyright © 2014-2015 Cask Data, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# conf.py to build high-level document comprised of multiple manuals
# Relies on settings in common_conf.py

# Search Index
# Includes handler to build a common search index of all manuals combined

# JSON Creation (json-versions.js)
# Creates a JSON file with timeline and formatting information fron the data in the 
# common_conf.py ("versions_data")

import codecs
import os
import sys

from sphinx.search import js_index
from sphinx.util.osutil import movefile
from sphinx.util.console import bold

sys.path.append(os.path.abspath('../../_common'))
from common_conf import *

# Search Index
# Includes handler to build a common search index of all manuals combined

def setup(app):
    # Define handler to build the common index
    app.connect('build-finished', build_common_index)


# Handler to build common index
def build_common_index(app, exception):
    builder = app.builder
    if exception:
        return
    manuals = html_theme_options["manuals"]
    master = load_index(builder)
    clean(master)
    
    for manual in manuals:
        index = load_index(builder, "../../%s/build/html" % manual)
        master = merge(master, index, manual)
    
    dump_search_index(builder, master)


# Load index from a js file
def load_index(builder, index_rel_path=''):
    index = None
    try:
        if index_rel_path:
            searchindexfn = os.path.join(builder.outdir, index_rel_path, builder.searchindex_filename)
        else:
            searchindexfn = os.path.join(builder.outdir, builder.searchindex_filename)
        if builder.indexer_dumps_unicode:
            f = codecs.open(searchindexfn, 'r', encoding='utf-8')
        else:
            f = open(searchindexfn, 'rb')
        try:
            index = js_index.load(f)
        finally:
            f.close()
    except (IOError, OSError, ValueError):
        builder.warn('search index couldn\'t be loaded')
    return index


def dump_search_index(builder, index):
    builder.info(bold('dumping combined search index... '), nonl=True)
    searchindexfn = os.path.join(builder.outdir, builder.searchindex_filename)
    # first write to a temporary file, so that if dumping fails,
    # the existing index won't be overwritten
    if builder.indexer_dumps_unicode:
        f = codecs.open(searchindexfn + '.tmp', 'w', encoding='utf-8')
    else:
        f = open(searchindexfn + '.tmp', 'wb')
    try:
        js_index.dump(index, f)
    finally:
        f.close()
    movefile(searchindexfn + '.tmp', searchindexfn)
    builder.info('done')

FILENAMES = 'filenames'
TERMS = 'terms'
TITLES = 'titles'
TITLETERMS = 'titleterms'

# Remove all references to subdirectories from master as they are replaced
def clean(master):
    for ref in range(len(master[FILENAMES])):
        file = master[FILENAMES][ref]
        if file.endswith("/index"):
            # Remove any file number references
            for term in master[TERMS]:
                files = master[TERMS][term]
                if isinstance(files, list) and ref in files:
                    master[TERMS][term] = files.remove(ref)
                    print "Deleted list reference: %s" % ref             
                elif ref == files: # A single file reference
                    del master[TERMS][term]
                    print "Deleted single reference: %s" % ref             


# Merge an index back into master, where the second index is located in a manual
# index dict format [u'envversion', u'terms', u'objtypes', u'objnames', u'filenames', u'titles', u'objects', u'titleterms']
# Steps:
# - get number of master.filenames; that +1 becomes what's added to second
# - append all new.filenames as manual/filename to master.filenames
# - append all new.titles to master.titles
# - merge all new.terms to master.terms, converting filenumbers by adding offset
# - merge all new.titleterms to master.titleterms, converting filenumbers by adding offset
def merge(master, new, manual):
    offset = len(master[FILENAMES])
    
    # Append all new.titles to master.titles
    master[FILENAMES] = master[FILENAMES] + [ "%s/%s" % (manual, filename) for filename in new[FILENAMES]]
    master[TITLES] = master[TITLES] + new[TITLES]
    
    # Merge to terms
    master[TERMS] = merger(master[TERMS], new[TERMS], offset)
    # Merge to titleterms
    master[TITLETERMS] = merger(master[TITLETERMS], new[TITLETERMS], offset)
    
    return master


def merger(dict1, dict2, offset):
    # merges dict2 into dict1, adjusting by offset
    # accounts for if existing items are lists or single objects
    # dict:{document:[0,3],cdap:[0,3],placeholder:[1,4,2,5],test:0}

    terms = dict1.keys()
    for term in dict2.keys():
        # Offset all file references
        add_files = dict2[term]
        if isinstance(add_files, list): 
            add_files = [(file + offset) for file in add_files]
        else:
            add_files = add_files + offset
        # Add to existing?
        if term in terms:
            # Add to existing
            files = dict1[term]
            if not isinstance(files, list):
                files = [files]
            if not isinstance(add_files, list):
                add_files = [add_files]
            new_files = files + add_files
        else:
            new_files = add_files
        
        dict1[term]=new_files
    return dict1

# JSON Creation (json-versions.js)
# Creates a JSON file with timeline and formatting information fron the data in the 
# common_conf.py ("versions_data")

def _build_timeline():
    # Takes data in versions_data
    #   "versions_data":
    #     { "development": [
    #         ['3.1.0-SNAPSHOT', '3.1.0'], 
    #         ], 
    #       "current": ['3.0.0', '3.0.0', '2015-05-05'], 
    #       "older": [ 
    #         ['2.8.0', '2.8.0', '2015-03-23'], 
    #         ['2.7.1', '2.7.1', '2015-02-05'], 
    #         ['2.6.3', '2.6.3', '2015-05-15'], 
    #         ['2.6.2', '2.6.2', '2015-03-23'], 
    #         ['2.6.1', '2.6.1', '2015-01-29'], 
    #         ['2.6.0', '2.6.0', '2015-01-10'], 
    #         ['2.5.2', '2.5.2', '2014-11-14'], 
    #         ['2.5.1', '2.5.1', '2014-10-15'], 
    #         ['2.5.0', '2.5.0', '2014-09-26'],
    #         ],
    #     },
    # and creates a timeline of the form:
    # 'timeline': [
    #     ['0', '3.0.0', '2015-05-05', ' (43 days)'],
    #     ['0', '2.8.0', '2015-03-23', ' (46 days)'],
    #     ['0', '2.7.1', '2015-02-05', ' (26 days)'],
    #     ['0', '2.6.0', '2015-01-10', ' (106 days)'],
    #     ['1', '2.6.1', '2015-01-29', ' (19 days)'],
    #     ['1', '2.6.2', '2015-03-23', ' (53 days)'],
    #     ['0', '2.5.0', '2014-09-26', ''],
    #     ['1', '2.5.1', '2014-10-15', ' (19 days)'],
    #     ['1', '2.5.2', '2014-11-14', ' (30 days)'],
    # ]
    # It modifies the "older", adding an extra element ('1') to flag the highest version 
    # of the minor index 
    versions_data = html_theme_options["versions_data"]
    older = versions_data["older"]
    rev_older = list(older)
    rev_older.reverse() # Now in lowest to highest versions
    current = versions_data["current"]
    data = []
    data_index = []
    
    # Make look-up dictionary
    data_lookup_dict = {}
    for release in rev_older:
        data_lookup_dict[release[0]] = release[2]
        
    # Find and flag highest version of minor index
    versions = []
    releases = len(older)
    for i in range(0, releases):
        style = ''
        version = older[i][0]
        version_major_minor = version[:version.rfind('.')]
        if i < (releases-1):
            next_version = older[i+1][0]
            next_version_major_minor = next_version[:next_version.rfind('.')]
            if version_major_minor not in versions and version_major_minor >= next_version_major_minor:
                versions.append(version_major_minor)
                style = '1'
        elif i == (releases-1):
            if version_major_minor not in versions:
                versions.append(version_major_minor)
                style = '1'
        older[i].append(style)
        
    # Build Timeline
    previous_date = ''
    for release in rev_older:
        version = release[0]
        date = release[2]
        if not data:
            # First entry; always set indent to 0
            indent = '0'
            _add_to_start_of_timeline(data, data_index, indent, version, date)
        else:
            if version.endswith('0'):
                # is this a x.x.0 release?
                # yes: put at start of timeline
                previous_date = data_lookup_dict[data_index[0]]
                # Find the previous *.*.0 release
                for i in range(0, len(data_index)):
                    if data_index[i].endswith('0'):
                        previous_date = data_lookup_dict[data_index[i]]
                indent = '0'
                _add_to_start_of_timeline(data, data_index, indent, version, date)
            else:
                # no:
                # is there an x.x.0 release for this?
                version_major_minor = version[:version.rfind('.')]
                version_zero = version_major_minor + '.0'
                if version_zero in data_index:
                    # yes: put after last one in that series
                    index = 0
                    in_range = False
                    for i in range(0, len(data_index)):
                        entry = data_index[i]
                        entry_major_minor = entry[:entry.rfind('.')]
                        if entry_major_minor == version_major_minor:
                            index = i
                            in_range = True
                        if in_range and entry_major_minor != version_major_minor:
                            index += 1
                            break
                        if i == (len(data_index)-1):
                            index = i +1
                    indent = '1'
                    _insert_into_timeline(data, data_index, index, indent, version, date)
                else:
                    # no: add at top as the top level (indent=0)
                    index = 0
                    indent = '0'
                    _insert_into_timeline(data, data_index, index, indent, version, date)
    current = html_theme_options["versions_data"]["current"]
    _add_to_start_of_timeline(data, data_index, '0', current[0], current[2])
    # Calculate dates
    current_date = ''
    points = len(data)
    for i in range(0, points):
        # if d[0] = '0' doing outer level;
        level = data[i][0]
        date =  data[i][2]
        if level == '0':
            current_date = date
            # Find next '0' level:
            index = -1
            for k in range(i+1, points):
                if data[k][0] == '0':
                    index = k
                    break
            if index != -1:
                delta_string = diff_between_date_strings(date, data[index][2])
            else:
                delta_string = ''
        elif level == '1':
            # Dated from previous "0" level (current_date)
            delta_string = diff_between_date_strings(date, current_date)
            current_date = date
        data[i].append(delta_string)
    versions_data['older'] = older
    versions_data['timeline'] = data
    return versions_data

def _add_to_start_of_timeline(data, data_index, indent, version, date):
    _insert_into_timeline(data, data_index, 0, indent, version, date)

def _append_to_timeline(data, data_index, indent, version, date, style):
    _insert_into_timeline(data, data_index, len(data), indent, version, date)

def _insert_into_timeline(data, data_index, index, indent, version, date):
    data.insert(index, [indent, version, date])
    data_index.insert(index, version)

def diff_between_date_strings(date_a, date_b):
    date_format = '%Y-%m-%d'
    a = datetime.strptime(date_a, date_format)
    b = datetime.strptime(date_b, date_format)
    delta = b - a
    diff = abs(delta.days)
    if diff == 1:
        days = "day"
    else:
        days = "days"
    delta_string = " (%s %s)" % (diff, days)
    return delta_string

def get_json_versions():
    return "versionscallback(%s);" % _build_timeline()

def print_json_versions():
    print get_json_versions()

def pretty_print_json_versions():
    data_dict = _build_timeline()
    for key in data_dict.keys():
        print "Key: %s" % key
        data = data_dict[key]
        for d in data:
            print d

def print_json_versions_file():
    head, tail = os.path.split(html_theme_options["versions"])
    print tail
