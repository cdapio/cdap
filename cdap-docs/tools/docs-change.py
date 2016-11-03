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
# Changes a doc set in place, modifying the headers, following these rules:
#
# - Looks for any empty title tags (<tile></title>) and adds the version in between, such as
#   <tile>CDAP 3.6.0 Documentation</title>
#
# - By default (unless flagged by "--robots"), adds link tags pointing to the canonical
#   numbered version at docs.cask.co. Only added if not already existing.
#
# - If flagged by "--robots" or version ends with "-SNAPSHOT", adds meta tags of 
#   "robots:no-index, no-follow". Only added if not already existing.
#
# - If the version derived from the basename is not the numbered version to be used for
#   empty title tags or the canonical link, use "--version=version" to specify the version.
#
#
# Examples
#
# python doc-change.py ~/Source/cdap/cdap-docs/target/3.6.0
# Changes the "3.6.0" doc set; sets empty titles, adds canonical link to 3.6.0
#
# python doc-change.py --version=3.6.0 ~/Source/cdap/cdap-docs/target/current
# Changes the "current" doc set; sets empty titles, adds canonical link to 3.6.0
# 
# python doc-change.py --robots ~/Source/cdap/cdap-docs/target/3.6.0
# Changes the "3.6.0" doc set; sets empty titles, adds a robots no-index no-follow meta-tag

from optparse import OptionParser
import os
import sys

def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [options] doc-set",
        description="Revises in-place a doc set (doc-set), given as an absolute path to a "
        "doc set directory. Unless flagged as 'robots' or is a '-SNAPSHOT' version, it "
        "adds canonical links pointing to a numbered doc set. Looks for any empty title "
        "tags (<tile></title>) and adds the version in between, such as <tile>CDAP version "
        "Documentation</title>.")

    parser.add_option(
        "-r", "--robots",
        action="store_true",
        dest="robots",
        help="If present, indicates that instead of canonical links, "
        "robots (no-index, no-follow) meta tags are to be added",
        default=False)

    parser.add_option(
        "-v", "--version",
        dest="version",
        help="Set a version, to be used for filling empty title tags and any added canonical numbered references; "
        "if not set, the basename of the doc-set is used instead",
        default=None)

    (options, args) = parser.parse_args()

    if not args:
        parser.print_help()
        sys.exit(1)

    return options, args, parser
    
def append_links(file_path, doc_set_path, doc_set, robots=False, version=None):
    # Get file content
    file_object = open(file_path, 'r')
    file_string = file_object.read()
    file_object.close()
    dirty = False

    # Look for an empty title tag
    empty_title_tags = '<title>%s</title>'
    empty_title_tags_index = file_string.lower().find(empty_title_tags % '')
    if empty_title_tags_index != -1 and version:
        empty_title_tags_end_index = empty_title_tags_index + len(empty_title_tags % '')
        file_string = file_string[:empty_title_tags_index] + empty_title_tags % "CDAP %s Documentation" % version + file_string[empty_title_tags_end_index:]
        dirty = True
    
    
    # Set robots meta-tag no-index no-follow on "future" and SNAPSHOT pages, but only if there isn't one already
    if (robots or (version and version.endswith("-SNAPSHOT"))) and file_string.lower().find('<meta name="robots"') == -1:
        meta_tag = '\n    <meta name=\"robots\" content=\"noindex, nofollow\">\n'
        open_head_tag = '<head>'
        open_head_tag_index = file_string.lower().find(open_head_tag)
        if open_head_tag_index != -1:
            open_head_tag_index = open_head_tag_index + len(open_head_tag)
            file_string = file_string[:open_head_tag_index] + meta_tag + file_string[open_head_tag_index:]
            dirty = True

    # Set canonical headers, but only if there isn't one already
    if not robots and file_string.lower().find('<link rel="canonical"') == -1:
        domain = 'docs.cask.co'
        end_path = file_path[len(doc_set_path):]
        canonical_link_tag = '\n\n    <link rel="canonical" href="http://%s/cdap/%s">\n' % (domain, version + end_path)
        close_title_tag = '</title>'
        close_title_tag_index = file_string.lower().find(close_title_tag)
        if close_title_tag_index != -1:
            close_title_tag_index = close_title_tag_index + len(close_title_tag)
            file_string = file_string[:close_title_tag_index] + canonical_link_tag + file_string[close_title_tag_index:]
            dirty = True
            
    if dirty:
        # Write new content to file
        file_object = open(file_path, 'w')
        file_object.write(file_string)
        file_object.close()

def convert_doc_set(doc_set_path, robots=False, version=None):
    doc_set = os.path.basename(doc_set_path)
    if not version:
        version = doc_set
    print "Converting doc set '%s' (%s)" % (doc_set, doc_set_path)
    file_paths = []
    for root, dirs, files in os.walk(doc_set_path):
        for f in files:
            if f.endswith('.html'):
                file_path = os.path.join(root, f)
                file_paths.append(file_path)
    print "HTML files to convert: %s" % len(file_paths)
    
    html_count = 0
    for file_path in file_paths:
        append_links(file_path, doc_set_path, doc_set, robots, version)
        html_count += 1
        sys.stdout.write('.')
        if (html_count / 100) * 100 == html_count:
            sys.stdout.write(" %s\n" % html_count)
        sys.stdout.flush()
    print "\nConverted HTML files: %s" % html_count
    
def main():
    """ Main program entry point.
    """
    options, args, parser = parse_options()
    if len(args) == 1:
        convert_doc_set(args[0], robots=options.robots, version=options.version)
    else:
        parser.print_help()
        print "\nOptions: %s" % options
        print "Args: %s" % args
        
if __name__ == '__main__':
    main()
