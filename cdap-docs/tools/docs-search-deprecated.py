#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Copyright © 2016 Cask Data, Inc.
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
#
# Reads in the Javadocs page 
# (at http://docs.cask.co/cdap/<release-version>/en/reference-manual/javadocs/deprecated-list.html);
# extracts all of the listed deprecated items, and then searches in the Java, text, and
# ReST files of the examples and the documentation for any usage. 
#
# It may generate significant false positives as it does not perform a search with any context.
#
# Requires Python 2.7 and Beautiful Soup.

import os
import sys
try:
    from bs4 import BeautifulSoup
    from optparse import OptionParser
    import urllib2
except Exception, e:
    if sys.version_info < (2, 7):
        print "\nMust use python 2.7 or greater.\n"
    raise e


def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [release]",
        description="Searches for deprecated items in documentation ('cdap-docs') and examples ('cdap-examples'). "
            "If no release is specified, 'current' is used instead. "
            "Uses the list at 'http://docs.cask.co/cdap/[release]/en/reference-manual/javadocs/deprecated-list.html' ")

    (options, args) = parser.parse_args()

    return options, args, parser


def load_deprecated_items(release):
    deprecated_url = "http://docs.cask.co/cdap/%s/en/reference-manual/javadocs/deprecated-list.html" % release
    print "Loading deprecated info from '%s'" % deprecated_url
    page = urllib2.urlopen(deprecated_url).read()
    soup = BeautifulSoup(page, 'html.parser')
#     soup.prettify()
    deprecated_items = dict()
    longest = 0
    i = 0
    for a in soup.select('a[href^="co/cask/cdap"]'):
        line = None
 
        if a.contents:
            line = str(a.contents[0]).strip()
            if line.startswith('<code>'):
                line = None
            else:
                try:
                    line = ''
                    for t in a.contents:
                        line += str(t).strip()
                    print "%3d: %s" % (i, line)
                except Exception, e:
                    print "%3d: %s" % (i, a.contents)
                    raise e
        else:
            print "%s: Could not find text in: %s" % (i, a)           

        if line:
            paren = line.find('(')
            if paren != -1:
                method = line[:paren]
            else:
                method = line
            period = method.rfind('.')
            if period != -1:
                deprecated = method[period+1:]
            else:
                deprecated = method
            
            closing_tag = line.find('</')
            if closing_tag != -1:
                signature = line[:closing_tag]
            else:
                signature = line
            
            if deprecated in deprecated_items:
                deprecated_items[deprecated].append(signature)
            else:
                deprecated_items[deprecated] = [signature]
            if len(deprecated) > longest:
                longest = len(deprecated)
        i += 1
    return deprecated_items, longest


def search_docs(release):
    script_dir = os.getcwd()
    deprecated_items, longest = load_deprecated_items(release)
    print "Deprecated: %s" % len(deprecated_items)
    if not deprecated_items:
        return
    
    # Print out sorted list of deprecated items, and where each is from
    deprecated_keys = deprecated_items.keys()
    deprecated_keys.sort()
    for d_key in deprecated_keys:
        deprecated_display = "%s%s" % (d_key, ' ' * (longest - len(d_key)))
        deprecated_display = deprecated_display[0:longest+1]
        if len(deprecated_items[d_key]) == 1:
            print "  %s : %s" % (deprecated_display, deprecated_items[d_key][0])
        else:
            print "  %s : %s" % (deprecated_display, deprecated_items[d_key][0])
            for source in deprecated_items[d_key][1:]:
                print "  %s : %s" % (' ' * longest, source)
        print
    

    # Walk directories
    docs = '..'
    examples = '../../cdap-examples'
    # TODO: add other example and tutorial repos, test if they are there, and check them
    
    for dir in (docs, examples):
        dir_path = os.path.abspath(os.path.join(script_dir, dir))
        file_paths = []
        for root, dirs, files in os.walk(dir_path):
            for f in files:
                if f.endswith('.rst') or f.endswith('.txt') or f.endswith('.java'):
                    file_path = os.path.join(root, f)
                    file_paths.append(file_path)
        print "\nChecked %s... files checked: %s" % (dir_path, len(file_paths))
    
        for file_path in file_paths:
            file_object = open(file_path, 'r')
            file_string = file_object.read()
            file_object.close()
        
            for deprecated in deprecated_items:
                if file_path.endswith('.java'):
                    deprecated_string = "%s(" % deprecated # Look for method name followed by "(" in Java files
                else:
                    deprecated_string = deprecated
                if file_string.find(deprecated_string) != -1: 
                    deprecated_display = "%s%s" % (deprecated, ' ' * (longest - len(deprecated)))
                    deprecated_display = deprecated_display[0:longest+1]
                    print "Found %s in '%s'" % (deprecated_display, file_path)
            

def main():
    """ Main program entry point.
    """
    options, args, parser = parse_options()
    
    if args:
        release = args[0]
    else:
        release = 'current'
    
    search_docs(release)
        
if __name__ == '__main__':
    main()
