#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright © 2017 Cask Data, Inc.
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

# Sitemap XML
#
# Generates a sitemap XML file for a web server.
# V 0.2 adds a compare option to generate and count the number of unique files.
#
# version 0.2

from optparse import OptionParser

import hashlib
import os
import sys

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CDAP_VERSION = '0.0.0'

def retrieve_version():
    # Try to find the default CDAP version
    global DEFAULT_CDAP_VERSION
    version = None
    pom_xml = os.path.abspath(os.path.join(SCRIPT_DIR_PATH, '../../pom.xml'))
    if os.path.isfile(pom_xml):
        import xml.etree.ElementTree
        tree = xml.etree.ElementTree.parse(pom_xml)
        version = tree.find('{http://maven.apache.org/POM/4.0.0}version').text
        if version:
            DEFAULT_CDAP_VERSION = version
    if not version:
        print "Unable to find default version from file '%s'" % pom_xml

retrieve_version()

DEFAULT_INPUT_DIRECTORY = os.path.abspath(os.path.join(SCRIPT_DIR_PATH, "../target/%s" % DEFAULT_CDAP_VERSION))
DEFAULT_OUTPUT_SITEMAP_XML_FILE = os.path.abspath(os.path.join(DEFAULT_INPUT_DIRECTORY, 'sitemap.xml'))

SITEMAP_XML_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
%s</urlset>
"""

SITEMAP_URL_TEMPLATE = """<url>
  <loc>%s</loc>
</url>
"""


class FileURL:

    def __init__(self, path='', base=''):
        self.path = path
        self.base = base
        if self.path and self.base:
            self.path_fragment = self.path[len(self.base):]
        else:
            self.path_fragment = None

    def generate_md5(self):
        if self.path:
            self.md5_hash = hashlib.md5(open(self.path, 'rb').read()).hexdigest()
        else:
            self.md5_hash = None
        return self.md5_hash

def parse_options():
    """ Parses args options.
    """

    description = """Reads either the default documentation directory
(%(default_input)s) or a specified directory, walks the directory, generates a
sitemap.xml file, uses the CDAP version of either the default (%(default_version)s) or a
specified version, and writes it to either the default (%(default_output)s) or a specified
output. Also writes a 'current.xml' and 'develop.xml' version of the sitemap.xml.
""" % {'default_input': DEFAULT_INPUT_DIRECTORY,
       'default_version': DEFAULT_CDAP_VERSION,
       'default_output': DEFAULT_OUTPUT_SITEMAP_XML_FILE,
       }

    parser = OptionParser(
        usage="\n\n  %prog",
        description=description)

    parser.add_option(
        '-c', '--compare',
        action="store_true",
        dest='compare',
        help="If True, creates MD5 hashes of each file, compares them, and counts the number of unique HTML files",
        default=False)

    parser.add_option(
        '-i', '--input',
        dest='input',
        help="The directory of files to be scanned, if not the default '%s'" % DEFAULT_INPUT_DIRECTORY,
        metavar='FILE',
        default=DEFAULT_INPUT_DIRECTORY)

    parser.add_option(
        '-o', '--output',
        dest='output',
        help="The sitemap.xml file to be written to, if not the default '%s'" % DEFAULT_OUTPUT_SITEMAP_XML_FILE,
        metavar='FILE',
        default=DEFAULT_OUTPUT_SITEMAP_XML_FILE)

    parser.add_option(
        '-v', '--version',
        dest='version',
        help="The CDAP version to be used for documentation links, if not the default '%s'" % DEFAULT_CDAP_VERSION,
        metavar='cdap-version',
        default=DEFAULT_CDAP_VERSION)

    (options, args) = parser.parse_args()

    return options, args, parser

def build_sitemap(input):
    sitemap = []
    print "Using input directory: %s" % input
    if not os.path.isdir(input):
        print "Input directory is not a directory: %s" % input
        return
    for dirpath, dirs, files in os.walk(input):
        for name in files:
            if name.startswith('.') or name == 'sitemap.xml':
                continue
            sitemap.append(FileURL(os.path.join(dirpath, name), input))
            
    print "Files: %d" % len(sitemap)
    return sitemap

def write_sitemap(sitemap, output, version, compare=False):
    print "Using output file: %s" % output
    if not os.path.dirname(output):
        print "Output file parent directory does not exist: %s" % output
        return 1

    sitemap_urls = ''
    unique_files = []
    for fileURL in sitemap:
        sitemap_urls += SITEMAP_URL_TEMPLATE % ("http://docs.cask.co/cdap/%s%s" % (version, fileURL.path_fragment))
        if compare and fileURL.path.endswith('.html'):
            md5_hash = fileURL.generate_md5()
            if md5_hash not in unique_files:
                unique_files.append(md5_hash)
    
    sitemap_xml = SITEMAP_XML_TEMPLATE % sitemap_urls

    f = open(output, 'w')
    f.write(sitemap_xml)
    f.close()
    print "Wrote %d URLs to output file: %s" % (len(sitemap), output)
    if compare:
        print "Consisting of %d unique HTML files" % len(unique_files)

#
# Main function
#

def main():
    """ Main program entry point.
    """
    options, args, parser = parse_options()

    if args:
        parser.print_help()
        sys.exit(1)

    sitemap = build_sitemap(options.input)

    if sitemap:
        return_code = write_sitemap(sitemap, options.output, options.version, options.compare)
        for v in ('current', 'develop'):
            if not return_code:
                return_code = write_sitemap(sitemap, "%s.%s.xml" % (options.output, v), v, options.compare)
    else:
        return_code = 1

    return return_code

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
