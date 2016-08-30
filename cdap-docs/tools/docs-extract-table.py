#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright © 2016 Cask Data, Inc.
#
# Used to generate a table in the CLI documentation from a table in a Markdown file.
#

import os
import sys
from optparse import OptionParser

VERSION = "0.0.1"

SPACES = ' ' * 3
PIPE = '|'
DOUBLE_QUOTE = '"'
TABLE_HEADER = """.. csv-table::
   :header: Command,Description
   :widths: 40, 60

"""
MISSING_FILE_TEMPLATE = "   **Missing Input File**,\"Missing input file %s\""


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

def _create_literal_line(pieces):
    pieces[0] = "``%s``" % pieces[0].strip()
    pieces = ["\"%s\"" % piece for piece in pieces]
    comma = ','
    return comma.join(pieces)

def create_parsed_line(line):
    """ Parses a line from a Markdown table and converts it to a reST CSV table line.
    """
    pieces = [piece.strip() for piece in line.split(PIPE)][1:-1]
    return _create_literal_line(pieces)

def create_from_lines(line1, line2):
    """ Combines two lines from a Markdown table and converts them into a single line for a reST CSV table line.
    """
    func = 'create_from_lines'
    pieces1 = [piece.strip() for piece in line1.split(PIPE)][1:-1]
    pieces2 = [piece.strip() for piece in line2.split(PIPE)][1:-1]
    
    if len(pieces1) != len(pieces2):
        raise Exception(func, 'Malformed Markdown table; length of pieces1 != length of pieces2:\n%s\n%s' 
                            % (line1, line2))
                            
    pieces = [pieces1[i] + ' ' + pieces2[i] for i in range(len(pieces1))]
    return _create_literal_line(pieces)

#
# Create the table
#

TABLE_RULE = '+=='
TABLE_TEXT = '| '
TABLE_TEXT_CONTINUED = '|  '

def create_table(input_file, output_file):
    """ Creates the table by reading in a file, building the table, outputing to a new file
    """
    lines = []
    return_code = 0
    if os.path.isfile(input_file):
        print "Reading in %s" % input_file
        with open(input_file,'r') as f:
            in_table = False
            in_literal = False
            skip_section = False
            
            raw_lines = []
            
            for line in f:
                line = line.strip()
                
                if not in_table and line.startswith(TABLE_RULE):
                    in_table = True
                    in_header = True
                    continue
                if in_table and in_header and line.startswith(TABLE_TEXT):
                    continue
                if in_table and in_header and line.startswith(TABLE_RULE):
                    in_header = False
                    continue
                if in_table and not in_header and line.startswith(TABLE_TEXT):
                    if line.startswith(TABLE_TEXT_CONTINUED):
                        # drop previous line
                        # add to previous line
                        lines.pop()
                        previous_line = raw_lines.pop()
                        new_line = create_from_lines(previous_line, line)
                    else:
                        raw_lines.append(line)
                        new_line = create_parsed_line(line)
                    lines.append(SPACES + new_line)
                    continue
                if in_table and not in_header and line.startswith(TABLE_RULE):
                    in_table = False
                    break                    
    else:
        print "Did not find %s" % input_file
        print "Wrote 'missing file' to table"
        lines.append(MISSING_FILE_TEMPLATE % input_file)
        return_code = 2
            
    with open(output_file,'w') as output:
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
    
    return_code = create_table(args[0], args[1])
    return return_code

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
