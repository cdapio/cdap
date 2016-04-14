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

from optparse import OptionParser
import os

VERSION = "0.0.1"

LITERAL = '``'
SPACES = ' ' * 3
SECTION_LINE = SPACES + '**'
COMMAND_LINE = SPACES + LITERAL
LITERAL_LINE = SPACES + ' | '

SKIP_SECTIONS = ['security (beta)']

MISSING_FILE_TEMPLATE = "   **Missing Input File**,\"Missing input file %s\""
TABLE_HEADER = """.. csv-table::
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

def line_starts_with(line, left):
    return bool(line[:len(left)] == left)

def skip_this_section(line):
    text = line.strip(' *').lower()
    return text in SKIP_SECTIONS
    
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
    literal = '``'
    quote = '\''
    backslash = '\\'
    opening_literal_quote = literal + quote
    closing_literal_quote = quote + literal
    new_line = ''
    for i in range(len(line)):
        c = line[i]
        if c == quote:
            if not in_literal:
                new_line += opening_literal_quote
            else:
                new_line += closing_literal_quote
                finished_literal = True
            in_literal = not in_literal
        else:
            if finished_literal:
                if c.isalnum():
                    new_line += backslash
                finished_literal = False
            new_line += c
    return new_line

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
        f = open(input_file,'r')
        in_spaces = False
        in_literal = False
        skip_section = False
        for line in f:
            line = line.rstrip()
            if line_starts_with(line, SECTION_LINE) or line_starts_with(line, COMMAND_LINE):
                if line_starts_with(line, SECTION_LINE):
                    skip_section = skip_this_section(line)
                if skip_section:
                    continue             
                if in_spaces: # Insert a blank line
                    lines.append('')
                    in_spaces = False
                lines.append(create_parsed_line(line))
            elif skip_section:
                continue
            elif line_starts_with(line, SPACES): # Handle spaces: assume a literal
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
        return_code = 404
            
    output = open(output_file,'w')
    output.write(TABLE_HEADER)
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
    
    return create_table(args[0], args[1])

if __name__ == '__main__':
    return main()
