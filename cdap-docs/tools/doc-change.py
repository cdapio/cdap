#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright © 2016 Cask Data, Inc.
#
# Changes a doc set in place, modifying the headers.
#
# To change GitHub pages;
# Do it by changing a local copy of GH Pages, using the branch gh-pages-working:
# - To change the 'current' directory of GH pages, which is version 3.6.0, use:
# ./doc-augmentor.py ../../cdap-gh-pages/cdap --ghpages current 3.6.0
#
# - This will modify all numbered doc sets
# ./doc-augmentor.py ../../cdap-gh-pages/cdap --ghpages
#
# (While testing, to change a particular directory of GH pages, such as 3.6.0, use:)
# ./doc-augmentor.py ../../cdap-gh-pages/cdap --ghpages 3.6.0
#
# To change doc server pages:
# - Make a copy of the script on the web servers
# - Replace the current symlink with a copy of the directory it points to, naming it as "current"
#
# - Modify the new "current", using the version it contains:
# ./doc-augmentor.py /var/www/docs/cdap current 3.6.0
#
# - Then run, to modify all the other sets:
# ./doc-augmentor.py /var/www/docs/cdap

from optparse import OptionParser
import os
import sys

def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [options] doc-directory [docset [replacement-version]]",
        description="Revises in-place a set of documentation directories; <doc-directory> is a relative or absolute path to a directory containing docsets.")

    parser.add_option(
        "-g", "--ghpages",
        action="store_true",
        dest="ghpages",
        help="Flags as GitHub pages for adding a canonical header",
        default=False)

    (options, args) = parser.parse_args()

    if not args:
        parser.print_help()
        sys.exit(1)

    return options, args
    
#  This method appends the LINK_TAG string with the correct version after the end of the title tag (</title>)
#  Filename is accepted as the first command line arg and the current version is accepted as the second arg.
def append_links(filepath, version, ghpages=False, ending=None, new_version=None):
    # Get file content
    file_object = open(filepath, 'r')
    file_string = file_object.read()
    file_object.close()

    # Modify File String
    # Look for empty title
    empty_title_tags = '<title>%s</title>'
    empty_title_tags_index = file_string.lower().find(empty_title_tags % '')
    if empty_title_tags_index != -1:
        empty_title_tags_end_index = empty_title_tags_index + len(empty_title_tags)
        file_string = file_string[:empty_title_tags_index] + empty_title_tags % "CDAP %s" % version  + file_string[empty_title_tags_end_index:]

    # Set canonical headers on current and GitHub pages (indicated by --ghpages option)
    if version == 'current' or ghpages:
        if ghpages:
            if version == 'current':
                domain = 'docs.cask.io'
                replacement_version = new_version if not new_version else new_version
            else:
                domain = 'docs.cask.co'
                replacement_version = version
        else:
            # version == 'current':
            domain = 'docs.cask.co'
            replacement_version = version if not new_version else new_version
    
        canonical_link_tag = '\n\n    <link rel="canonical" href="http://%s/cdap/%s">\n' % (domain, replacement_version + ending)
        close_title_tag = '</title>'
        close_title_tag_index = file_string.lower().find(close_title_tag)
        if close_title_tag_index != -1:
            close_title_tag_index = close_title_tag_index + len(close_title_tag)
            file_string = file_string[:close_title_tag_index] + canonical_link_tag + file_string[close_title_tag_index:]
    
    # Set robots meta-tag on non-current pages
    if version != 'current':
        meta_tag = '\n    <meta name=\"robots\" content=\"noindex, nofollow\">\n'
        open_head_tag = '<head>'
        open_head_tag_index = file_string.lower().find(open_head_tag)
        if open_head_tag_index != -1:
            open_head_tag_index = open_head_tag_index + len(open_head_tag)
            file_string = file_string[:open_head_tag_index] + meta_tag + file_string[open_head_tag_index:]

    # Write new content to file
    file_object = open(filepath, 'w')
    file_object.write(file_string)
    file_object.close()

def convert_doc_set(doc_set_path, ghpages=False, new_version=None):
    doc_set = os.path.basename(doc_set_path)
    print "Converting doc set '%s' (%s)" % (doc_set, doc_set_path)
    html_count = 0
    for root, dirs, files in os.walk(doc_set_path):
        for f in files:
            if f.endswith('.html'):
                html_count += 1
    print "HTML files to convert: %s" % html_count
    html_count = 0
    for root, dirs, files in os.walk(doc_set_path):
        for f in files:
            if f.endswith('.html'):
                filepath = os.path.join(root, f)
                ending = filepath[len(doc_set_path):]
                append_links(filepath, doc_set, ghpages, ending, new_version)
                html_count += 1
                sys.stdout.write('.')
                if (html_count / 100) * 100 == html_count:
                    sys.stdout.write(" %s\n" % html_count)
                sys.stdout.flush()
    print "\nConverted HTML files: %s" % html_count

def convert_upper_level_html(html_path):
    html_file = os.path.basename(html_path)
    print "Converting html file '%s' (%s)" % (html_file, html_path)

def convert_item(item, docs_path, ghpages=False, new_version=None):
    item_path = os.path.join(docs_path, item)
    if item.startswith('.'):
        print "Ignoring '%s'" % item
    elif item.endswith('.html'):
        convert_upper_level_html(item_path)
    elif item.replace('.','').replace('-SNAPSHOT','').isdigit():
        convert_doc_set(item_path, ghpages)
    elif item == 'current':
        if os.path.islink(item_path):
            print "WARNING Ignoring '%s' as it is a symlink" % item
        elif new_version:
            convert_doc_set(item_path, ghpages, new_version)
        else:
            print "WARNING Ignoring '%s' there is no new version" % item
    else:
        print "Unknown item: '%s' (%s)" % (item, item_path)

def walk_docs_path(docs_path, ghpages=False):
    print "Using path: %s" % docs_path
    docs_path = os.path.abspath(docs_path)
    print "Using docs path: %s" % docs_path
    
    # List directory
    dirlisting = os.listdir(docs_path)
    for item in dirlisting:
        convert_item(item, docs_path, ghpages)

#
# Main function
#
def main():
    """ Main program entry point.
    """
    options, args = parse_options()

    if len(args) == 1:
        walk_docs_path(args[0], ghpages=options.ghpages)
    elif len(args) == 2:
        convert_item(args[1], args[0], ghpages=options.ghpages)
    elif len(args) == 3:
        convert_item(args[1], args[0], ghpages=options.ghpages, new_version=args[2])
    else:
        print "Need to supply a documentation set directory: should end with CDAP"
        print "args: %s" % args
        
if __name__ == '__main__':
    main()
