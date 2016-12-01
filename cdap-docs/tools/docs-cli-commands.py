#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright © 2016 Cask Data, Inc.
#
# Used to generate a table in the CLI documentation from the output of CLI tools.
# 
# If input_file is not found, puts out a warning message in the log and puts a note 
# in the table that is generated.
#

import os
import sys
from optparse import OptionParser

VERSION = "0.0.2"

LITERAL = '``'
SPACES = ' ' * 3
QUOTE = '\''
BACKSLASH = '\\'
SECTION_LINE = SPACES + '**'
COMMAND_LINE = SPACES + LITERAL
LITERAL_LINE = SPACES + ' | '

SKIP_SECTIONS = []

MISSING_FILE_TEMPLATE = "   **Missing Input File**,\"Missing input file %s\""
SECTION_TABLE_HEADER = """
%s
%s

.. csv-table::
   :header: Command,Description
   :widths: 50, 50

"""

def parse_options():
    """ Parses args options.
    """
    parser = OptionParser(
        usage="\n\n  %prog input_file output_file",
        description="Generates an rst table to be included in the documentation")

    parser.add_option(
        "-v", "--version",
        action="store_true",
        dest="version",
        help="Version of this software",
        default=False)

    (options, args) = parser.parse_args()
    
    if options.version:
        print "Version: %s" % VERSION
        sys.exit(1)

    if not args or len(args) != 2:
        parser.print_help()
        sys.exit(1)

    return options, args

#
# Utility functions
#

def skip_this_section(line):
    text = line.strip(' *').lower()
    skip_sections = [section.lower() for section in SKIP_SECTIONS]
    return text in skip_sections
    
def create_literal_line(line):
    right_line = line.lstrip()
    spaces = len(line) - len(right_line)
    return LITERAL_LINE + ' ' * spaces + LITERAL + right_line + LITERAL

def create_parsed_line(line):
    """ Parses a line and changes single-quotes to a combination of literals and single-quotes.
        Handles closing literals with a backslash correctly for alpha-numerics.
        Any text that is enclosed with single-quotes will be converted to a literal with single-quotes.
        'text' will become ``'text'``
    """
    i = 0
    in_literal = False
    finished_literal = False
    opening_literal_quote = LITERAL + QUOTE
    closing_literal_quote = QUOTE + LITERAL
    new_line = ''
    for c in line:
        if c == QUOTE:
            if not in_literal:
                new_line += opening_literal_quote
            else:
                new_line += closing_literal_quote
                finished_literal = True
            in_literal = not in_literal
        else:
            if finished_literal:
                if c.isalnum():
                    new_line += BACKSLASH
                finished_literal = False
            new_line += c
    return new_line

def create_new_section(line):
    sectionTitle = line.replace('**','').strip()
    underline = len(sectionTitle) * '-'
    return SECTION_TABLE_HEADER % (sectionTitle, underline)

#
# Create the table
#

def create_table(input_file, output_file):
    """ Creates the table by reading in a file, building the table, outputing to a new file
    """
    lines = []
    return_code = 0
    if os.path.isfile(input_file):
        print "Reading in %s" % input_file
        with open(input_file,'r') as f:
            at_start = True
            in_spaces = False
            in_literal = False
            skip_section = False
            for line in f:
                line = line.rstrip()
                if at_start and not line.startswith(' '):
                    continue
                else:
                    at_start = False
                if line.startswith(SECTION_LINE) or line.startswith(COMMAND_LINE):
                    if line.startswith(SECTION_LINE):
                        skip_section = skip_this_section(line)
                    if skip_section:
                        continue             
                    if in_spaces: # Insert a blank line
                        lines.append('')
                        in_spaces = False
                    if line.startswith(SECTION_LINE):
                        lines.append(create_new_section(line))
                    else:
                        lines.append(create_parsed_line(line))
                elif skip_section:
                    continue
                elif line.startswith(SPACES): # Handle spaces: assume a literal
                    in_spaces = True
                    lines.append(create_literal_line(line))
                else:
                    if in_spaces: # Insert a blank line
                        lines.append('')
                        in_spaces = False
                    lines.append(SPACES + create_parsed_line(line))
    else:
        print "Did not find %s" % input_file
        print "Wrote 'missing file' to table"
        lines.append(MISSING_FILE_TEMPLATE % input_file)
        return_code = 2
            
    output = open(output_file,'w')
    with open(output_file,'w') as output:
        for line in lines:
            output.write(line+'\n')
    
    print "Wrote to %s" % output_file
    return return_code
#
# Main function
#

def main():
    """ Main program entry point.
    """
    options, args = parse_options()
    
    return_code = create_table(args[0], args[1])
    return return_code

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
