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
# Default output ends up as 'target/html/github-release-notes.txt'.
#
# version 0.1

from optparse import OptionParser

import os
import sys

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_RELEASE_NOTES_RST_FILE = os.path.abspath(os.path.join(SCRIPT_DIR_PATH, '../reference-manual/source/release-notes.rst'))
DEFAULT_OUTPUT_RELEASE_NOTES_TXT_FILE = os.path.abspath(os.path.join(SCRIPT_DIR_PATH, '../reference-manual/target/html/release-notes.txt'))

CASK_ISSUE_START = ':cask-issue:`'
CASK_ISSUE_END = '` - '

LITERAL_INDENT = 8

def parse_options():
    """ Parses args options.
    """
    
    description = """Reads either the default release notes file
(%(default_release_notes_file)s)
or an input file (if provided), determines the top-most section of releases notes,
reformats it, and writes it to the terminal.
""" % {'default_release_notes_file': DEFAULT_INPUT_RELEASE_NOTES_RST_FILE}

    parser = OptionParser(
        usage="\n\n  %prog",
        description=description)

    parser.add_option(
        '-i', '--input',
        dest='input',
        help="The release notes file to be loaded, if not the default '%s'" % DEFAULT_INPUT_RELEASE_NOTES_RST_FILE,
        metavar='FILE',
        default=DEFAULT_INPUT_RELEASE_NOTES_RST_FILE)

    parser.add_option(
        '-o', '--output',
        dest='output',
        help="The release notes file to be written to, if not the default '%s'" % DEFAULT_OUTPUT_RELEASE_NOTES_TXT_FILE,
        metavar='FILE',
        default=DEFAULT_OUTPUT_RELEASE_NOTES_TXT_FILE)

    parser.add_option(
        '-v', '--version',
        dest='version',
        help="The CDAP version to be used for documentation links; if supplied, first section of doc is converted; if no version, entire file is used",
        metavar='cdap-version',
        default='')

    (options, args) = parser.parse_args()

    return options, args

def replace_refs(line):
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
    line = "%(line_start)s[%(label)s](#%(ref)s)%(line_end)s" % {
            'line_start': line[:ref_index],
            'label': label, 
            'ref': ref, 
            'line_end': line[ref_index_close+2:],
            }
    return replace_refs(line)

def rewrite_links(line, closing_ref):
    # If any errors, just returns the line unchanged
    close_index = line.find(closing_ref)
    if close_index == -1:
        return line
    open_index = line[:close_index].rfind('`')
    if open_index == -1:
        # Error, but return the line
        return line
    link = line[open_index+1:close_index]
    link_index = link.find(' <')
    if link_index == -1:
        # Error, but return the line
        return line
    label = link[:link_index]
    link = link[link_index+2:]
    line = "%(line_start)s[%(label)s](%(link)s)%(line_end)s" % {
            'line_start': line[:open_index],
            'label': label, 
            'link': link, 
            'line_end': line[close_index+len(closing_ref):],
            }
    return rewrite_links(line, closing_ref)

def rewrite_anchor(line):
    # If any errors, just returns the line unchanged
    # rst anchor format:
    # .. _release-notes-cdap-6837:
    if not (line.startswith('.. _') and line.endswith(':') and line[4:-1]):
        return line
    return "<a name=\"%s\"></a>" % line[4:-1]

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
    print "Reading input file: %s" % input
    f = open(input, 'r')
    in_cask_issue = False
    in_para = False
    in_literal = False
    in_literal_indent = 0
    in_first_section = False
    in_lines = False
    issues = []
    indent = 0
    new_lines = []
    new_line = ''
    for line in f:
        line_stripped = line.strip()
        if not in_first_section:
            if line.startswith('`'):
                in_first_section = True
                if version:
                    # Check if version matches:
                    version_trimmed = version.replace('-SNAPSHOT','')
                    if not line.startswith("`Release %s" % version_trimmed):
                        new_lines.append("Warning: expected 'Release %s' but found '%s'\n" % (version_trimmed, line.strip()))
            continue
        if line.startswith('`'):
            if in_first_section:
                if version:
                    break
                else:
                    continue
        if line_stripped.startswith('.. _'):
            new_lines.append(rewrite_anchor(line_stripped))
            continue
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
                line_indent = len(line) - len(line_strip)
                if not in_literal_indent:
                    if line_indent < LITERAL_INDENT:
                        in_literal_indent = LITERAL_INDENT - line_indent
                    else:        
                        in_literal_indent = line_indent
                if line_indent > indent:
                    new_lines.append(' ' * in_literal_indent + line)
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
            indent = cask_issue_start
        elif in_cask_issue and issues:
            line_strip = line.strip()
            if line_strip:
                new_line = "%s %s" % (new_line, line_strip)
            else:
                if new_line.endswith('::\n'):
                    in_literal = True # Start of literal block
                new_line = build_new_line(new_line, issues)
                new_lines.append(new_line)
                in_cask_issue = False
                issues = []
                new_line = ''
        elif not in_cask_issue and line.startswith(' ') and not in_para:
            in_para = True
            new_line = line.rstrip()
        elif in_para:
            line_strip = line.strip()
            if line_strip:
                if line.startswith(' '):
                    new_line = "%s %s" % (new_line, line_strip)
                else:
                    new_line = "%s\n\n%s" % (new_line, line_strip)
            else:
                new_line += '\n'
                new_lines.append(new_line)
                in_para = False
                if new_line.endswith('::\n'):
                    in_literal = True # Start of literal block
                new_line = ''
        elif line.startswith('- ') and not in_para:
            in_para = True
            indent = 2
            new_line = line.rstrip()
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
    if new_lines:
        for line in new_lines:
            line = line.replace(' |---| ', '—')
            if line.endswith('::\n'):
                line = line.replace('::', ':')
            line = line.replace('``\\', '``')
            line = replace_refs(line)
            line = rewrite_links(line, '>`__')
            line = rewrite_links(line, '>`_')
            line = line.replace('\\ ', '')
            f.write("%s\n" % line)
    else:
        if version:
            message = "for 'Release %s' " % version_trimmed
        else:
            message = ""
        f.write("Warning: expected notes %sbut found nothing.\n" % message)
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
