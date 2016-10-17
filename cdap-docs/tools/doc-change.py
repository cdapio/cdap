#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright © 2016 Cask Data, Inc.
#
# Changes a doc set in place, modifying the headers.
#
# To change GitHub pages;
# - Do it by changing a local copy of GH Pages, using the branch gh-pages-working:
#
# - Change all doc sets, declaring that '3.6.0' is the 'current' version:
# ./doc-change.py ../../cdap-gh-pages/cdap --ghpages --current=3.6.0
#
# For testing, use:
#
# ./doc-change.py ../../cdap-gh-pages/cdap --ghpages --current=3.6.0 2.5.0
#
# ./doc-change.py ../../cdap-gh-pages/cdap --ghpages --current=3.6.0 3.6.0
#
# ./doc-change.py ../../cdap-gh-pages/cdap --ghpages --current=3.6.0 current
#
# To change doc server pages:
# - Make a copy of the script on the web servers
# - Replace the current symlink with a copy of the directory it points to, naming it as "current"
#
# - Change all doc sets, declaring that '3.6.0' is the 'current' version:
# ./doc-change.py /var/www/docs/cdap --current=3.6.0
#
# For testing, use:
#
# ./doc-change.py /var/www/docs/cdap --current=3.6.0 2.5.0
#
# ./doc-change.py /var/www/docs/cdap --current=3.6.0 3.6.0
#
# ./doc-change.py /var/www/docs/cdap --current=3.6.0 current

from optparse import OptionParser
import os
import sys

def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [options] doc-directory [doc-set]",
        description="Revises in-place a set of documentation directories; <doc-directory> is a relative or absolute path to a directory containing docsets. <doc-set> is a directory in the <doc-directory>")

    parser.add_option(
        "-g", "--ghpages",
        action="store_true",
        dest="ghpages",
        help="Flags as GitHub pages doc sets; they point to docs.cask.co",
        default=False)

    parser.add_option(
        "-c", "--current",
        dest="current",
        help="The version to be considered as current; it won't have no-index, no-follow meta tags added",
        default=None)

    (options, args) = parser.parse_args()

    if not args:
        parser.print_help()
        sys.exit(1)

    return options, args
    
#  This method appends the LINK_TAG string with the correct version after the end of the title tag (</title>)
#  Filename is accepted as the first command line arg and the current version is accepted as the second arg.
def append_links(filepath, version, ghpages=False, ending='', current=None):
    # Get file content
    file_object = open(filepath, 'r')
    file_string = file_object.read()
    file_object.close()
    dirty = False

    # Modify File String
    
    # Look for an empty title
    empty_title_tags = '<title>%s</title>'
    empty_title_tags_index = file_string.lower().find(empty_title_tags % '')
    if empty_title_tags_index != -1:
        title_version = current if version == 'current' else version
        empty_title_tags_end_index = empty_title_tags_index + len(empty_title_tags % '')
        file_string = file_string[:empty_title_tags_index] + empty_title_tags % "CDAP %s" % title_version + file_string[empty_title_tags_end_index:]
        dirty = True
        
    # Set canonical headers on GitHub pages (indicated by --ghpages option) and current pages
    if (ghpages and (version == current or version == 'current')) or (version == 'current'):
        domain = 'docs.cask.co'
        if ghpages and version == 'current':
            replacement_version = version
        else:
            replacement_version = current
            
        canonical_link_tag = '\n\n    <link rel="canonical" href="http://%s/cdap/%s">\n' % (domain, replacement_version + ending)
        close_title_tag = '</title>'
        close_title_tag_index = file_string.lower().find(close_title_tag)
        if close_title_tag_index != -1:
            close_title_tag_index = close_title_tag_index + len(close_title_tag)
            file_string = file_string[:close_title_tag_index] + canonical_link_tag + file_string[close_title_tag_index:]
            dirty = True
            
    # Set robots meta-tag no-index no-follow on GitHub pages non-current pages and Docs.cask.co non-current_numbered_version
    if version != 'current' and version != current:
        meta_tag = '\n    <meta name=\"robots\" content=\"noindex, nofollow\">\n'
        open_head_tag = '<head>'
        open_head_tag_index = file_string.lower().find(open_head_tag)
        if open_head_tag_index != -1:
            open_head_tag_index = open_head_tag_index + len(open_head_tag)
            file_string = file_string[:open_head_tag_index] + meta_tag + file_string[open_head_tag_index:]
            dirty = True

    if dirty:
        # Write new content to file
        file_object = open(filepath, 'w')
        file_object.write(file_string)
        file_object.close()

def convert_docs(doc_set_path, ghpages=False, current=None):
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
                append_links(filepath, doc_set, ghpages, ending, current)
                html_count += 1
                sys.stdout.write('.')
                if (html_count / 100) * 100 == html_count:
                    sys.stdout.write(" %s\n" % html_count)
                sys.stdout.flush()
    print "\nConverted HTML files: %s" % html_count

def convert_upper_level_html(html_path):
    html_file = os.path.basename(html_path)
    print "Converting html file '%s' (%s)" % (html_file, html_path)

def convert_doc_set(doc_set, docs_path, ghpages=False, current=None):
    if current:
        print "Using '%s' as the current numbered version" % current
    doc_set_path = os.path.join(docs_path, doc_set)
    if doc_set.startswith('.'):
        print "Ignoring '%s'" % doc_set
    elif doc_set.endswith('.html'):
        convert_upper_level_html(doc_set_path)
    elif doc_set.replace('.','').replace('-SNAPSHOT','').isdigit():
        convert_docs(doc_set_path, ghpages=ghpages, current=current)
    elif doc_set == 'current':
        if os.path.islink(doc_set_path):
            print "WARNING Ignoring '%s' as it is a symlink" % doc_set
        else:
            convert_docs(doc_set_path, ghpages=ghpages, current=current)
    else:
        print "Unknown doc_set: '%s' (%s)" % (doc_set, doc_set_path)

def walk_docs_path(docs_path, ghpages=False, current=None):
    print "Using path: %s" % docs_path
    abs_docs_path = os.path.abspath(docs_path)
    print "Using docs path: %s" % abs_docs_path
    
    # List directory
    dir_listing = os.listdir(abs_docs_path)
    for doc_set in dir_listing:
        convert_doc_set(doc_set, abs_docs_path, ghpages=ghpages, current=current)

#
# Main function
#
def main():
    """ Main program entry point.
    """
    options, args = parse_options()

    if len(args) == 1:
        walk_docs_path(args[0], ghpages=options.ghpages, current=options.current)
    elif len(args) == 2:
        convert_doc_set(args[1], args[0], ghpages=options.ghpages, current=options.current)
    else:
        print "Need to supply a documentation set directory: should end with CDAP"
        print "args: %s" % args
        
if __name__ == '__main__':
    main()
