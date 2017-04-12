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

# XML File to Release Notes
#
# Given an XML file of Release Notes, converts it into an RST file in the CDAP-appropriate format.
#
# To create an appropriate XML File:
#
# ./docs-xml-to-release-notes.py -i ~/Desktop/SearchRequest-11312.xml
#
# Output would be in ~/Desktop/SearchRequest-11312.rst
#
# version 0.2

from optparse import OptionParser

import htmlentitydefs
import os
import re
import sys

DEFAULT_OUTPUT_RST_FILE = 'release-notes.rst'
SOURCE_PATH = os.path.dirname(os.path.abspath(__file__))

def parse_options():
    """ Parses args options.
    """

    description = """Reads an input release notes XML file, converts it to an RST format, and
outputs it to a file of the same name ending in '.rst', unless otherwise specified.
"""

    parser = OptionParser(
        usage="\n\n  %prog -i <input-file>",
        description=description)

    parser.add_option(
        '-i', '--input',
        dest='input',
        help="The release notes XML file to be loaded",
        metavar='FILE')

    parser.add_option(
        '-o', '--output',
        dest='output',
        help="The release notes RST file to be written to, if not the default of the input file with '.rst'",
        metavar='FILE',
        default=None)

    (options, args) = parser.parse_args()

    if not options.input:
        parser.print_help()

    return options, args

##
# Removes HTML or XML character references and entities from a text string.
#
# @param text The HTML (or XML) source text.
# @return The plain text, as a Unicode string, if necessary.

def unescape(text):
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    return re.sub("&#?\w+;", fixup, text)

def unlink(text):
    """Find a link in a text string, and replaces it with RST-equiv."""
    A_HREF = '<a href="'
    A_HREF_CLOSE = '">'
    A_HREF_TAG_CLOSE = '</a>'

    find_a_href = text.find(A_HREF)
    if find_a_href == -1:
        return text

    find_a_href_close = text.find(A_HREF_CLOSE)
    if find_a_href_close == -1:
        raise Exception('unlink', "'%s' is missing a '%s'" % (text, A_HREF_CLOSE))
    find_a_href_close_end = find_a_href_close + len(A_HREF_CLOSE)

    find_a_href_tag_close = text.find(A_HREF_TAG_CLOSE)
    if find_a_href_tag_close == -1:
        raise Exception('unlink', "'%s' is missing a '%s'" % (text, A_HREF_TAG_CLOSE))
    find_a_href_tag_close_end = find_a_href_tag_close + len(A_HREF_TAG_CLOSE)

    open = text[0:find_a_href]
    link = text[find_a_href_close_end:find_a_href_tag_close]
    close = text[find_a_href_tag_close_end:]

    new_text = "%s`%s`__%s" % (open, link, close)
    # Recursive in case there are more
    return  unlink(new_text)

def read_lines(input, output):
    func = 'read_lines'
    KEY_ID = '                        <key id='
    KEY_ID_CLOSE = '">'
    KEY_ID_TAG_CLOSE = '</key>'
    CUSTOM_FIELD = '                            <customfieldvalue>'
    CUSTOM_FIELD__TAG_CLOSE = '</customfieldvalue>'
    print "Reading input file: %s" % input
    if not os.path.isfile(input):
        raise Exception(func, "'%s' not a valid path" % input)
    rst_lines = []
    lines = [line.rstrip('\n') for line in open(input)]
    key_count = 0
    new_lines = []
    issue = None
    for line in lines:
        if line.startswith(KEY_ID):
            key_count += 1
            i = line.find(KEY_ID_CLOSE)
            if i != -1:
                issue = line[i+len(KEY_ID_CLOSE):-len(KEY_ID_TAG_CLOSE)]
            else:
                print "Error in key_id line: %s" % line
            continue
        elif line.startswith(CUSTOM_FIELD):
            note = line[len(CUSTOM_FIELD):-len(CUSTOM_FIELD__TAG_CLOSE)]
            if issue and note:
                # Need to unescape twice, as the HTML entities have been encoded to be in XML
                new_note = unlink(unescape(unescape(note)))
                new_line = "- :cask-issue:`%s` - %s" % (issue, new_note)
                print new_line
                new_lines.append(new_line)
                issue = None
            else:
                print "Error in custom_field line: %s" % line

    f = open(output, 'w')
    print "Writing to output file: %s" % output
    if new_lines:
        for line in new_lines:
            f.write("%s\n\n" % line)
    f.close()

    print "Key Count:   %d" % key_count
    print "Lines Count: %d" % len(new_lines)

def make_output_path(input):
    # Input is probably of the form SearchRequest-xxxxx.xml
    output = DEFAULT_OUTPUT_RST_FILE
    if input and input.endswith('.xml'):
        output = input[:-len('.xml')] + '.rst'
    print "Using output file %s" % output
    return output

#
# Main function
#
def main():
    """ Main program entry point.
    """
    options, args = parse_options()
    return_code = 0
    try:
        if options.input:
            output = options.output
            if not output:
                output = make_output_path(options.input)
            return_code = read_lines(input=options.input, output=output)
    except Exception, e:
        sys.stderr.write("Error: %s\n" % e)
        sys.exit(1)

    return return_code

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
