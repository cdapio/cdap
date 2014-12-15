# -*- coding: utf-8 -*-

# Copyright © 2014 Cask Data, Inc.
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
# relies on settings in common_conf.py
# Includes handler to build a common index of all manuals combined

import codecs
import os
import sys

from sphinx.search import js_index
from sphinx.util.osutil import movefile
from sphinx.util.console import bold

sys.path.append(os.path.abspath('../../_common'))
from common_conf import *

print "imported common_conf; Project: %s" % project


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

 