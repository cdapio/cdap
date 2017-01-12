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

# GitHub Release Notes
#
# Give a CDAP release notes file in rst format, generates a file with release notes for
# the top-most section in GitHub format (Markdown).
#
# Reads and writes using common-sense defaults that can be over-written if required.
# Default output ends up as 'target/github-release-notes.txt'.
#
# It does not resolve references to the CDAP docs; these are flagged
# 'LINK_TO_BE_CORRECTED' and must be resolved manually.
#
# version 0.1

from optparse import OptionParser

import os
import sys

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_RELEASE_NOTES_FILE = os.path.abspath(os.path.join(SCRIPT_DIR_PATH, '../reference-manual/source/release-notes.rst'))
DEFAULT_OUTPUT_RELEASE_NOTES_FILE = os.path.abspath(os.path.join(SCRIPT_DIR_PATH, '../target/github-release-notes.txt'))

NOTES = 'notes.txt'
NOTES_PATH = os.path.join(SCRIPT_DIR_PATH, NOTES)
RST_INPUT_FORMAT = 'rst'
WEB_INPUT_FORMAT = 'web'
BUG = 'Bug'

SECTION_STRING = '** '
START_STRING = '    * [CDAP-'
DESCRIPTION_STRING = '] - '

RELEASE_NOTE = '- '
RELEASE_NOTE_DASH = ' - '

CASK_ISSUE_START = ':cask-issue:`'
CASK_ISSUE_END = '` - '

DOCS_LINK = "http://docs.cask.co/cdap/%s/en/"

def parse_options():
    """ Parses args options.
    """
    
    description = """Reads either the default release notes file
(%(default_release_notes_file)s)
or an input file (if provided), determines the top-most section of releases notes,
reformats it, and writes it to the terminal.
""" % {'default_release_notes_file': DEFAULT_INPUT_RELEASE_NOTES_FILE}

    parser = OptionParser(
        usage="\n\n  %prog",
        description=description)

    parser.add_option(
        '-i', '--input',
        dest='input',
        help="The release notes file to be loaded, if not the default '%s'" % DEFAULT_INPUT_RELEASE_NOTES_FILE,
        metavar='FILE',
        default=DEFAULT_INPUT_RELEASE_NOTES_FILE)

    parser.add_option(
        '-o', '--output',
        dest='output',
        help="The release notes file to be written to, if not the default '%s'" % DEFAULT_OUTPUT_RELEASE_NOTES_FILE,
        metavar='FILE',
        default=DEFAULT_OUTPUT_RELEASE_NOTES_FILE)

    parser.add_option(
        '-v', '--version',
        dest='version',
        help="The CDAP version to be used for documentation links, if not 'current'",
        metavar='cdap-version',
        default='current')

    (options, args) = parser.parse_args()

    return options, args

def replace_refs(line, version):
    # If any errors, just returns the line unchanged
    REF_RST = ':ref:`'
    ref_index = line.find(REF_RST)
    if ref_index == -1:
        return line
    ref_index_close = line.find('>`', ref_index + len(REF_RST))
    if ref_index_close == -1:
        return line
    ref = line[ref_index:ref_index_close]
    link_index = ref.find(' <')
    if link_index == -1:
        return line
    label = ref[ref.find('`')+1:link_index]
    ref = ref[link_index+2:]
    line = "%(line_start)s[%(label)s](%(doc_ref)s %(ref)s LINK_TO_BE_CORRECTED)%(line_end)s" % {
            'line_start': line[:ref_index],
            'doc_ref': DOCS_LINK % version,
            'label': label, 
            'ref': ref, 
            'line_end': line[ref_index_close+2:],
            }
    return replace_refs(line, version)

def extract_issues(line, start, end):
    new_line = line[:start] + line[end + len(CASK_ISSUE_END):].rstrip()
    issues = line[start:end].replace(':cask-issue:','').replace('`','').replace(',','').split()
    return new_line, issues
    
def build_new_line(line, issues):
    if not issues:
        return line
    else:
        issue_links = []
        for issue in issues:
            issue_links.append("[%s](https://issues.cask.co/browse/%s" % (issue, issue))
        return "%s (%s))\n" % (line, ','.join(issue_links))
    
def read_lines(input, output, version):
    output_format = 'github'
    print "Reading input file: %s" % input
    f = open(input, 'r')
    in_cask_issue = False
    in_para = False
    in_literal = False
    in_literal_indent = 0
    in_first_section = False
    in_lines = False
    issues = []
    indent = ''
    new_lines = []
    new_line = ''
    for line in f:
        if not in_first_section:
            if line.startswith('`'):
                in_first_section = True
                # Check if version matches:
                version_trimmed = version.replace('-SNAPSHOT','')
                if not line.startswith("`Release %s" % version_trimmed):
                    new_lines.append("Error: expected 'Release %s' but found '%s'\n" % (version_trimmed, line.strip()))
            continue
        if line.startswith('`'):
            if in_first_section:
                break
        if line.startswith('.. ') or line.startswith('==') :
            continue
        line = line.rstrip()
        if not in_lines and not line:
            continue
        else:
            in_lines = True

        if in_literal:
            if not line.strip():
                new_lines.append(line)
                continue
            if line.startswith(' '):
                line_strip = line.lstrip()
                indent = ' ' * (len(line) - len(line_strip))
                if not in_literal_indent:
                    in_literal_indent = indent
                if indent >= in_literal_indent:
                    new_lines.append(line)
                    continue
                else:
                    in_literal = False
                    in_literal_indent = 0
            else:
                in_literal = False
                in_literal_indent = 0
                
        cask_issue_start = line.find(CASK_ISSUE_START)
        cask_issue_end = line.find(CASK_ISSUE_END)
        if not in_cask_issue and cask_issue_start != -1 and cask_issue_end != -1 :
            in_cask_issue = True
            new_line, issues = extract_issues(line, cask_issue_start, cask_issue_end)
            indent = ' ' * cask_issue_start
        elif in_cask_issue and issues:
            line_strip = line.strip()
            if line_strip:
                new_line = "%s %s" % (new_line, line_strip)
            else:
                new_line = build_new_line(new_line, issues)
                new_lines.append(new_line)
                in_cask_issue = False
                if new_line.endswith('::\n'):
                    in_literal = True # Start of literal block
                issues = []
                new_line = ''
        elif not in_cask_issue and line.startswith(' ') and not in_para:
            in_para = True
            new_line = line.rstrip()
        elif in_para:
            line_strip = line.strip()
            if line_strip:
                new_line = "%s %s" % (new_line, line_strip)
            else:
                new_line += '\n'
                new_lines.append(new_line)
                in_para = False
                if new_line.endswith('::\n'):
                    in_literal = True # Start of literal block
                new_line = ''
        else:
            new_line = line
            new_lines.append(new_line)
            if new_line.endswith('::\n'):
                in_literal = True # Start of literal block
            new_line = ''
                
    # If there is anything leftover, append it
    if in_cask_issue and issues:
        new_line = build_new_line(new_line, issues)
    if new_line:
        new_lines.append(new_line)
    
    f = open(output, 'w')
    print "Writing to output file: %s" % output
    for line in new_lines:
        line = replace_refs(line, version)
        line = line.replace(' |---| ', '—')
        f.write("%s\n" % line)
    f.close()

#
# Main function
#

def main():
    """ Main program entry point.
    """
    options, args = parse_options()
    
    return_code = read_lines(input=options.input, output=options.output, version=options.version)
    
    return return_code

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
