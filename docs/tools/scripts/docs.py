#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright 2014 Continuuity, Inc.
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
# Tests
#
# PDF generation
# python docs.py -g pdf -o ../../install-guide/build-pdf/install.pdf ../../install-guide/source/install.rst
#

VERSION = "0.0.1"
DEFAULT_OUTPUT_PDF_FILE = "output.pdf"
DEFAULT_GENERATE = "pdf"
TEMP_FILE_SUFFIX = "_temp"

REST_EDITOR             = ".. reST Editor: "
RST2PDF                 = ".. rst2pdf: "
RST2PDF_BUILD           = ".. rst2pdf: build "
RST2PDF_CONFIG          = ".. rst2pdf: config "
RST2PDF_STYLESHEETS     = ".. rst2pdf: stylesheets "
RST2PDF_CUT_START       = ".. rst2pdf: CutStart"
RST2PDF_CUT_STOP        = ".. rst2pdf: CutStop"
RST2PDF_PAGE_BREAK      = ".. rst2pdf: PageBreak"
RST2PDF_PAGE_BREAK_TEXT = """.. raw:: pdf

	PageBreak"""


from optparse import OptionParser
import os
import subprocess
import sys
import tempfile


def parse_options():
    """ Parses args options.
    """

    parser = OptionParser(
        usage="%prog [options] input.rst",
        description="Generates HTML or PDF from an reST-formatted file")

    parser.add_option(
        "-v", "--version",
        action="store_true",
        dest="version",
        help="Version of software",
        default=False)

    parser.add_option(
        "-o", "--output",
        dest="output_file",
        help="The path to the output file: .html or "
             ".pdf extensions allowed (default: %s)" % DEFAULT_OUTPUT_PDF_FILE,
        metavar="FILE",
        default=DEFAULT_OUTPUT_PDF_FILE)

    parser.add_option(
        "-g", "--generate",
        dest="generate",
        help="One of html, pdf, or slides "
             "(default %s)" % DEFAULT_GENERATE,
        default=DEFAULT_GENERATE)

    (options, args) = parser.parse_args()

    options.generate = options.generate.lower()
    
    if options.version:
        print "Version: %s" % VERSION
        sys.exit(1)

    if not args:
        parser.print_help()
        sys.exit(1)

    return options, args[0]


def log(message, type):
    """Basic logger, print output directly to stdout and errors to stderr.
    """
    (sys.stdout if type == 'notice' else sys.stderr).write(message + "\n")


def process_pdf(input_file, options):
    output = ""
    config = ""
    stylesheets = ""
    print "input_file: %s" % input_file
    f = open(input_file,'r')
    lines = []
    rst2pdf_cut = False
    for line in f:
        if line_starts_with(line, RST2PDF_CUT_START):
            rst2pdf_cut = True
        elif line_starts_with(line, RST2PDF_CUT_STOP):
            rst2pdf_cut = False
        elif rst2pdf_cut:
            continue
        elif line_starts_with(line, REST_EDITOR):
            # Ignore all reST Editor directives
            continue
        elif line_starts_with(line, RST2PDF_BUILD):
            print line
            build = line_right_end( line, RST2PDF_BUILD)
            print "output: %s" % output
        elif line_starts_with(line, RST2PDF_CONFIG):
            print line
            config = line_right_end( line, RST2PDF_CONFIG)
            print "config: %s" % config
        elif line_starts_with(line, RST2PDF_STYLESHEETS):
            print line
            stylesheets = line_right_end( line, RST2PDF_STYLESHEETS)
            print "stylesheets: %s" % stylesheets
        elif line_starts_with(line,  RST2PDF_PAGE_BREAK):
            lines.append(RST2PDF_PAGE_BREAK_TEXT)
        elif line_starts_with(line, RST2PDF):
            lines.append(line_right_end(line, RST2PDF))
        else:
            lines.append(line.strip('\n'))
            
    # Set paths
    source_path = os.path.dirname(os.path.abspath(__file__))
    
    # def get_absolute_path() Factor out duplicate code in this section
    
    if not os.path.isabs(input_file):
        input_file = os.path.join(source_path, input_file)
        if not os.path.isfile(input_file):
            raise Exception('process_pdf', 'input_file not a valid path: %s' % input_file)
            
    if options.output_file:
        output = options.output_file       
    if not os.path.isabs(output):
        output = os.path.join(os.path.dirname(input_file), output)
        if not os.path.isdir(os.path.dirname(output)):
            raise Exception('process_pdf', 'output not a valid path: %s' % output)
            
    if not os.path.isabs(config):
        config = os.path.join(os.path.dirname(input_file), config)
        if not os.path.isfile(config):
            raise Exception('process_pdf', 'config not a valid path: %s' % config)
            
    if not os.path.isabs(stylesheets):
        stylesheets = os.path.join(os.path.dirname(input_file), stylesheets)
        if not os.path.isfile(stylesheets):
            raise Exception('process_pdf', 'stylesheets not a valid path: %s' % stylesheets)
            
    # Write output to temp file
    temp_file = input_file+TEMP_FILE_SUFFIX
    if not os.path.isabs(temp_file):
        raise Exception('process_pdf', 'temp_file not a valid path: %s' % temp_file)    
    temp = open(temp_file,'w')    
    for line in lines:
        temp.write(line+'\n')
    temp.close()
    print "Completed parsing input file"

    # Generate PDF
#     /usr/local/bin/rst2pdf 
#     --config="/Users/john/Source/reactor_2.3.0_docs/docs/developer-guide/source/_templates/pdf-config" 
#     --stylesheets="/Users/john/Source/reactor_2.3.0_docs/docs/developer-guide/source/_templates/pdf-stylesheet" 
#     -o "/Users/john/Source/reactor_2.3.0_docs/docs/developer-guide/build-pdf/rest2.pdf" 
#     "/Users/john/Source/reactor_2.3.0_docs/docs/developer-guide/source/rest.rst_temp”

    command = '/usr/local/bin/rst2pdf --config="%s" --stylesheets="%s" -o "%s" %s' % (config, stylesheets, output, temp_file)
    print "command: %s" % command
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT,)
    except:
        raise Exception('process_pdf', 'output: %s' % output)
        
    if len(output)==0:
        os.remove(temp_file)
    else:
        print output
        
    print "Completed process_pdf"

#
# Utility functions
#

def line_starts_with(line, left):
    return bool(line[:len(left)] == left)


def line_right_end(line, left):
    # Given a line of text (that may end with a carriage return)
    # and a snip at the start, 
    # return everything from the end of snip onwards, except for the trailing return
    t = line[len(left):]
    return t.strip('\n')

# def get_absolute_path(


#
# Main function
#

def main():
    """ Main program entry point.
    """
    options, input_file = parse_options()

    try:
        options.logger = log
        if options.generate == "pdf":
            print "PDF generation..."
            process_pdf(input_file, options)
        elif options.generate == "html":
            print "HTML generation not implemented"
        elif options.generate == "slides":
            print "Slides generation not implemented"
        else:
            print "Unknown generation type: %s" % options.generate
            sys.exit(1)
    except Exception, e:
        sys.stderr.write("Error: %s\n" % e)
        sys.exit(1)


if __name__ == '__main__':
    main()
