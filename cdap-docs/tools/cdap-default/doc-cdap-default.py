#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright © 2015-2016 Cask Data, Inc.
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

# Builds an rst table to document the cdap-default.xml file
# Any property names listed in cdap-default-exclusions.txt are excluded.
# Any properties marked "final" are placed in second table after the first.
#
# python doc-cdap-default.py <source-filepath> <output-filepath>
# python doc-cdap-default.py <output-filepath>
#
# If <source-filepath> not provided, uses CDAP_DEFAULT_XML
# if <output-filepath> not provided, outputs to standard out

# Also, a tool for comparing XML and SDL files.
# Given a list of files in OTHER_CDAP_XML_FILES, can compare them
# with the XML file in CDAP_DEFAULT_XML


# Steps:
#
# Load source code cdap-default.xml
# keys: name of property
# values: Property class object (value and description)
# Defaults in source code are in no particular order.
# 
# Sort by key
# Output rst table to standard out
# If filepath provided, writes to file instead

import json
import os
import sys
import textwrap
import xml.etree.ElementTree as ET

from datetime import datetime
from optparse import OptionParser

SOURCE_PATH = os.path.dirname(os.path.abspath(__file__))
RELATIVE_PATH = '../../..'
CDAP_DEFAULT_XML = 'cdap-common/src/main/resources/cdap-default.xml'
CDAP_DEFAULT_EXCLUSIONS = 'cdap-default-exclusions.txt'

OTHER_CDAP_XML_FILES = 'cdap-xml-files.txt'

FIRST_TWO_SECTIONS = ['General Configuration', 'Global Configuration']

RST_ANCHOR_TEMPLATE = ".. _appendix-cdap-default-%s:"

RST_TABLE_HEADER = """
.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter Name
     - Default Value
     - Description"""

NAME_START  = '   * - '
VALUE_START = '     - '
BLOCK_START = '| ``'
BLOCK_JOIN  = '``\n       | ``'
DESC_START  = '     - '

SECTION_START = NAME_START + '|\n       | '

# The XML_HEADER is not ascii text due to the copyright symbol in the fourth line

XML_HEADER = u"""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Copyright © 2014-%d Cask Data, Inc.

  Licensed under the Apache License, Version 2.0 (the "License"); you may not
  use this file except in compliance with the License. You may obtain a copy of
  the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations under
  the License.
  -->
""" % datetime.now().year

XML_CONFIG_OPEN   = '<configuration>\n'
XML_SECTION_SUB   = "\n  <!-- %s -->\n\n"
XML_PROP_OPEN     = '  <property>\n'
XML_NAME_SUB      = "    <name>%s</name>\n"
XML_VALUE_SUB     = "    <value>%s</value>\n"
XML_DESCRIP_OPEN  = '    <description>\n'
XML_DESCRIP_SUB   = "      %s\n"
XML_DESCRIP_CLOSE = '    </description>\n'
XML_FINAL         = '    <final>true</final>\n'
XML_PROP_CLOSE    = '  </property>\n\n'
XML_CONFIG_CLOSE  = '</configuration>\n'

SDL_CONFIG_NAME   = "      \"configName\": \"%s\","
SDL_DESCRIPTION   = "      \"description\": \"%s\","

class Item:
    MAX_RST_VALUE_CHAR_LENGTH = 40

    @staticmethod
    def encode(value):
        import cgi
        if value:
            return cgi.escape(value).encode('ascii', 'xmlcharrefreplace')
        else:
            return ''

    @staticmethod
    def _split_text(text, length=70, delimiter='.'):
        # Splits text into blocks not exceeding length if possible, on delimiter
        if len(text) > length and text.count(delimiter):
            # Split text into multiple pieces
            split = text.rfind(delimiter, 0, length)
            left = text[:split+1]
            right = text[split+1:]
            if right:
                texts = Item._split_text(right, length, delimiter)
                if not texts[-1]:
                    texts = texts[:-1]
            return (left,) + texts
        else:
            return (text, '')

    @staticmethod
    def format_rst_block(text):
        # Tests if a block of text exceeds self.MAX_RST_VALUE_CHAR_LENGTH
        # and if so, splits it on the location of periods ('.')
        block = "%s" % text
        if len(block) > Item.MAX_RST_VALUE_CHAR_LENGTH and block.count('.'):
            # Split block into multiple pieces
            block = BLOCK_START + BLOCK_JOIN.join(Item._split_text(block, Item.MAX_RST_VALUE_CHAR_LENGTH)) + '``'
        else:
            block = "``%s``" % block
        return block

    def __init__(self, name='', value='', description ='', final=None):
        self.name = name
        self.value = self.encode(value)
        self.description = self.encode(description)
        self.final = final
    
    def __str__(self):
        if self.final != None:
            return "%s:\n%s (%s)\n%s" % (self.name, self.value, self.final, self.description)
        else:
            return "%s:\n%s\n%s" % (self.name, self.value, self.description)

    def _append(self, name, value):
        if self.__dict__[name]:
            self.__dict__[name] = "%s\n%s" % (self.__dict__[name], value)
        else:
            self.__dict__[name] = value
    
    def rst(self):
        FINAL_RST = ' :ref:`[Final] <cdap-site-xml-note-final>`'

        name = self.format_rst_block(self.name)
        if self.final:
            name += FINAL_RST
            
        if self.value.count(' '):
            value = BLOCK_START + BLOCK_JOIN.join(self.value.split()) + '``'
        elif not self.value:
            value = ''
        else:
            value = self.format_rst_block(self.value)

        description = self.description
        if description.count('@'):
            description = description.replace('@', '\@')
            
        rst = "%s%s\n%s%s\n%s%s\n" % (NAME_START, name,
                                      VALUE_START, value,
                                      DESC_START, description)
        return rst
    
    def set_attribute(self, name, value):
        v1 = "%s" % self.encode(value)
        if name == 'description' and v1 == 'None':
            v = ''
        elif name == 'description':
            v = " ".join(v1.split())
        elif name == 'final':
            v = (v1.lower() == 'true')
        else:
            v = v1
        self.__dict__[name] = v
            
    def get_attribute(self, name):
        if name == 'rst':
            return self._rst()
        else:
            return self.__dict__[name]
    
    def append_value(self, value):
        self._append("value", value)

    def append_description(self, description):
        self._append("description", description)
    
    def display(self):
        print self.name


class Section:

    def __init__(self, name=''):
        self.name = name.strip()
        self.final = None

    def rst(self):
        CONFIG = ' Configuration'
        name = self.name.strip()
        if name.endswith(CONFIG):
            name = name[0:-len(CONFIG)]
        underline = "-" * len(name)
        anchor = RST_ANCHOR_TEMPLATE % name.lower().replace(' ','-')
        rst = "\n%s\n\n%s\n%s\n%s\n" % (anchor, name, underline, RST_TABLE_HEADER)
        return rst

    def display(self):
        print "\nSection %s" % self.name
       

class PIParser(ET.XMLTreeBuilder):
    """ProcessingInstruction Parser from http://effbot.org/zone/element-pi.htm
       An XML parser that preserves processing instructions and comments
       
       Usage:
       def parse(source):
           return ET.parse(source, PIParser())
    """

    def __init__(self):
        ET.XMLTreeBuilder.__init__(self)
        # assumes ElementTree 1.2.X
        self._parser.CommentHandler = self.handle_comment
        self._parser.ProcessingInstructionHandler = self.handle_pi
        self._target.start("document", {})

    def close(self):
        self._target.end("document")
        return ET.XMLTreeBuilder.close(self)

    def handle_comment(self, data):
        self._target.start(ET.Comment, {})
        self._target.data(data)
        self._target.end(ET.Comment)

    def handle_pi(self, target, data):
        self._target.start(ET.PI, {})
        self._target.data(target + " " + data)
        self._target.end(ET.PI)


def parse_options():
    """Parses args options."""

    parser = OptionParser(
        usage="%prog [options]",
        description='Generates an RST file from an XML file such as cdap-default.xml')

    # Generation, source and target
    
    parser.add_option(
        '-a', '--all',
        dest='compare_all',
        action='store_true',
        help="Compares all files listed in '%s' to '%s'" % (OTHER_CDAP_XML_FILES, CDAP_DEFAULT_XML),
        default=False)

    parser.add_option(
        '-u', '--update',
        dest='compare_all_update',
        action='store_true',
        help="If 'comparing all files', displays the updates required to match '%s'" % CDAP_DEFAULT_XML,
        default=False)

    parser.add_option(
        '-c', '--compare',
        dest='compare',
        action='store_true',
        help='Compares two XML files',
        default=False)

    parser.add_option(
        '-g', '--generate',
        dest='generate',
        action='store_true',
        help='Loads an XML file and creates an RST file describing it',
        default=False)

    parser.add_option(
        '-i', '--ignore',
        dest='ignore',
        action='store_true',
        help="Ignores the %s file" % CDAP_DEFAULT_EXCLUSIONS,
        default=False)

    parser.add_option(
        '-s', '--source',
        dest='source',
        help="The XML to be loaded, if not the default '%s'" % CDAP_DEFAULT_XML,
        metavar='FILE',
        default='')

    parser.add_option(
        '-o', '--other',
        dest='other_source',
        help="The second XML to be loaded",
        metavar='FILE',
        default='')

    parser.add_option(
        '-t', '--target',
        dest='target',
        help='Where to write the RST, if not standard output',
        metavar='FILE',
        default='')

    # Displaying file and output to terminal
    
    parser.add_option(
        '-p', '--props',
        dest='load_props',
        action='store_true',
        help='Loads the existing default XML and writes the properties alphabetically and stats to standard output',
        default=False)

    parser.add_option(
        '-q', '--quick',
        dest='load_quick',
        action='store_true',
        help='Loads the existing default XML and writes a summary and stats to standard output',
        default=False)

    parser.add_option(
        '-x', '--xml',
        dest='load_xml',
        action='store_true',
        help='Loads the existing default XML and writes it out, rebuilt, to standard output',
        default=False)

    parser.add_option(
        '-r', '--rst',
        dest='load_xml_to_rst',
        action='store_true',
        help='Loads the existing default XML and writes the RST to standard output',
        default=False)

    # Editing: rebuilding the default XML file

    parser.add_option(
        '-b', '--build',
        dest='build',
        action='store_true',
        help='Loads the existing default XML and builds it in the correct order to '
             'a file at the same location with \'_revised.xml\'',
        default=False)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return parser.parse_args()

def log(message, type):
    """Basic logger, print output directly to stdout and errors to stderr."""
    (sys.stdout if type == 'notice' else sys.stderr).write(message + "\n")

def default_xml_filepath(extra_path=''):
    return os.path.join(SOURCE_PATH, RELATIVE_PATH, CDAP_DEFAULT_XML + extra_path)
    
def load_exclusions():
    func = 'load_exclusions'
    exclusions_path = os.path.join(SOURCE_PATH, CDAP_DEFAULT_EXCLUSIONS)
    if not os.path.isfile(exclusions_path):
        raise Exception(func, "'%s' not a valid path" % exclusions_path)
    lines = []
    for line in open(exclusions_path):
        if not line or line.startswith(' ') or line.startswith('#'):
            continue
        else:
            lines.append(line.rstrip('\n'))
    return lines
#     return [line.rstrip('\n') for line in open(exclusions_path)]

def load_xml_files():
    func = 'load_xml_files'
    xml_files_path = os.path.join(SOURCE_PATH, OTHER_CDAP_XML_FILES)
    if not os.path.isfile(xml_files_path):
        raise Exception(func, "'%s' not a valid path" % xml_files_path)
    return [line.rstrip('\n') for line in open(xml_files_path)]

def load_xml(source, include_exclusions=False, include_comments=True):
    func = 'load_xml'
    if source:
        print "Loading %s" % source
    else:
        print 'Loading cdap-default.xml'
        source = default_xml_filepath()
    try:
        if os.path.isfile(source):
            tree = ET.parse(source, PIParser())
        else:
            tree = ET.fromstring(source, PIParser())
    except:
        raise Exception(func, "'%s' not a valid source" % source)
    
    if include_exclusions:
        exclusions = []
    else:
        exclusions = load_exclusions()
    
    items = []
    root = tree.getroot()
    for outer_element in root:
        for inner_element in outer_element:
            if inner_element.tag == 'property':
                item = Item()
                for attribute in inner_element:
                    item.set_attribute(attribute.tag, attribute.text)                
                if item.name not in exclusions:
                    items.append(item) 
            elif include_comments:
                items.append(Section(inner_element.text))

    return items, tree

def create_rst(items, ignore):
    """Create rst from items"""
    table = ''    
    for item in items:
        table += item.rst()
    return table

def rebuild(filepath=''):
    """Loads the cdap-default.xml, and rebuilds it according to these rules:
    - First two sections: 
        General Configuration
        Global Configuration
    - Remaining sections alphabetical
    - Properties within a section alphabetical
    - Descriptions wrapped to a 70 character line-length"""
    
    print 'Rebuilding cdap-default.xml'
    items, tree = load_xml('', include_exclusions=True)
    exclusions = load_exclusions()
    in_section = True
    defaults = {}
    section = []
    section_counter = 0
    properties_counter = 0
    section_name = ''
    for item in items:
        if str(item.__class__) == '__main__.Section':
            if section: # End old section
                if not section_name:
                    section_name = "Section %s" % section_counter
                defaults[section_name] = section
                section = []
                
            # Start new section
            section_counter += 1
            section_name = item.name
        else:
            properties_counter += 1
            section.append(item)
    if section: # End old section
        defaults[section_name] = section
        section = []  
    
    print "Sections: %d Keys: %d Properties: %d" % (section_counter, len(defaults.keys()), properties_counter)

    # Build XML file
    prop_names = {}
    xml = XML_CONFIG_OPEN
    keys = defaults.keys()
    for key in FIRST_TWO_SECTIONS:
        if key in keys:
            keys.remove(key)
            
    keys.sort()
    FIRST_TWO_SECTIONS.reverse()
    
    for key in FIRST_TWO_SECTIONS:
        if key in defaults.keys():
            keys = [key] + keys

    for key in keys:
        xml += XML_SECTION_SUB % key
        props = defaults[key]
        props.sort(key = lambda p: p.name)
        for prop in props:
            if prop_names.has_key(prop.name):
                print ("WARNING: Duplicate entry for property \"%s\" in sections \"%s\" and \"%s\"" 
                    % (prop.name, key, prop_names[prop.name]))
            else:
                prop_names[prop.name] = key
            xml += XML_PROP_OPEN
            xml += XML_NAME_SUB % prop.name
            xml += XML_VALUE_SUB % prop.value
            xml += XML_DESCRIP_OPEN
            if prop.description:
                for line in textwrap.wrap(prop.description):
                    xml += XML_DESCRIP_SUB % line
            else:
                print "WARNING: No description for property \"%s\"" % prop.name
                if prop.name in exclusions:
                    print "but it is in the list of exclusions"
            xml += XML_DESCRIP_CLOSE
            if prop.final:
                xml += XML_FINAL
            xml += XML_PROP_CLOSE
    xml += XML_CONFIG_CLOSE
    
    if filepath:
        f = open(filepath, 'wb')
        f.write(XML_HEADER.encode('utf8'))
        f.write(xml)
        f.close()
        print "New XML file in %s" % filepath
    else:
        print XML_HEADER.encode('utf8')
        for line in xml.split('\n'):
            print line

def load_create_rst(filesource='', filepath='', ignore=False):
    defaults, tree = load_xml(filesource, include_exclusions=ignore)
    table = create_rst(defaults, ignore)
    if filepath:
        f = open(filepath, 'w')
        f.write(table)
        f.close()
    else:
        print table
        
def load_summary(source='', props_only=False, ignore=False):
    items, tree = load_xml(source, include_exclusions=ignore)
    section_counter = 0
    properties_counter = 0
    for item in items:
        if str(item.__class__) == '__main__.Section':
            section_counter += 1
            if not props_only:
                item.display()                
        else:
            properties_counter += 1
            item.display()
    print "Sections: %d\nProperties: %d" % (section_counter, properties_counter)

# python doc-cdap-default.py \
#        --compare -s ~/Source/cdap_3.1/cdap-common/src/main/resources/cdap-default.xml \
#        -o ~/Source/cdap_3.2/cdap-common/src/main/resources/cdap-default.xml -t diff.txt

def load_xml_source(source_title, source):
    # Loads XML from source, described as source_title
    print "Loading '%s': %s" % (source_title, source)
    items, tree = load_xml(source, include_exclusions=True, include_comments=False)
    print "  Items: %s" % len(items)
    items.sort(key = lambda p: p.name)
    items_keys = []
    items_dict = {}
    for item in items:
        items_keys.append(item.name)
        items_dict[item.name] = item
    return (source_title, items, items_keys, items_dict)

def compare_xml_files(source, other_source, target=None, update=False):
    # Compares two XML files
    items = load_xml_source('source', source)    
    other_items = load_xml_source('other_source', other_source)
    mis_matches_list = compare_items(items, other_items, update)
    if update:
        print "Updates:\n"
        for name, item_description in mis_matches_list:
            print name
            xml = XML_DESCRIP_OPEN
            for line in textwrap.wrap(item_description):
                xml += XML_DESCRIP_SUB % line
            xml += XML_DESCRIP_CLOSE
            print xml
            
def compare_items(items_list, other_items_list, update=False):
    # Compares two lists of source_title, items, items_keys, items_dict
    in_both = []
    items_source_title, items, items_keys, items_dict = items_list
    other_items_source_title, other_items, other_items_keys, other_items_dict = other_items_list
    
    print "Looking in '%s' for each item in '%s'" % (other_items_source_title, items_source_title)
    only_in_source = []
    for item in items:
        if item.name in other_items_keys:
            if not item.name in in_both:
                in_both.append(item.name)
        else:
            only_in_source.append(item.name)
    print "  Only in '%s': %d" % (items_source_title, len(only_in_source))
            
    print "Looking in '%s' for each item in '%s'" % (items_source_title, other_items_source_title)
    only_in_other_source = []
    for other_item in other_items:
        if other_item.name in items_keys:
            if not other_item.name in in_both:
                in_both.append(other_item.name)
        else:
            only_in_other_source.append(other_item.name)
    print "  Only in '%s': %d" % (other_items_source_title, len(only_in_other_source))
    for name in only_in_other_source:
        print "    %s" % name

    print "In both: %d" % len(in_both)
    # Check if the ones in both match descriptions
    matches = 0
    mis_matches_list = []
    for name in in_both:
        item_description = items_dict[name].description
        other_item_description = other_items_dict[name].description
        if item_description != other_item_description:
            print "  Item '%s' does not match" % name
            mis_matches_list.append((name, item_description))
        else:
            matches += 1
    print "Description matches:     %d" % matches
    print "Description mis-matches: %d" % len(mis_matches_list)
    if len(in_both) + len(only_in_other_source) < len(other_items_keys):
        # other_items_keys has a duplicate key
        other_items_copy = []
        other_items_dupes = []
        for other_item in other_items:
            if other_item.name not in other_items_copy:
                other_items_copy.append(other_item.name)
            else:
                other_items_dupes.append(other_item.name)
        if other_items_dupes:
            print "Duplicated items: %d" % len(other_items_dupes)
            for item in other_items_dupes:
                print "  %s" % item
    print
    return mis_matches_list

def compare_xml_sdl(source, other_source, target=None, update=False):
    # Compares an XML source to an SDL source
    items = load_xml_source('XML source', source)
    
    sdl_source_title = 'SDL other source'
    print "Loading '%s': %s" % (sdl_source_title, other_source)
    f = open(other_source)
    sdl = json.load(f)
    f.close()
    # "parameters"
    # "roles": a list; each element has a "parameters"
    # some parameters have a configName and description
    other_items = []
    for p in sdl["parameters"]:
        if "configName" in p:
            other_items.append(Item(name=p["configName"], description=p["description"]))
    for r in sdl["roles"]:
        if "parameters" in r:
            for p in r["parameters"]:
                if "configName" in p:
                    other_items.append(Item(name=p["configName"], description=p["description"]))
    print "  items: %s" % len(other_items)
    other_items.sort(key = lambda p: p.name)
    other_items_keys = []
    other_items_dict = {}
    for item in other_items:
        other_items_keys.append(item.name)
        other_items_dict[item.name] = item
    other_items = (sdl_source_title, other_items, other_items_keys, other_items_dict)
    mis_matches_list = compare_items(items, other_items)
    if update:
        print "Updates:\n"
        for config_name, item_description in mis_matches_list:
            print SDL_DESCRIPTION % item_description
            print SDL_CONFIG_NAME % config_name
            print
            
def download_to_file(url):
    # Downloads from a URL to a temp file and returns
    # a filepath and the data downloaded
    import urllib
    import tempfile
    f = tempfile.NamedTemporaryFile(delete=False)
    sock = urllib.urlopen(url)
    source = sock.read()
    sock.close()
    f.write(source)
    f.close()
    return f, source

def compare_all_xml(options):
    # Compares each file listed in OTHER_CDAP_XML_FILES
    # to CDAP_DEFAULT_XML
    update = options.compare_all_update
    files = load_xml_files()
    for file in files:
        if not file or file.startswith(' ') or file.startswith('#'):
            continue
        elif file.startswith("http"):
            f, f_source = download_to_file(file)
            if file.endswith(".xml"):
                print "XML file from: %s" % file
                compare_xml_files(default_xml_filepath(), f.name, update=update)
            if file.endswith(".sdl"):
                print "SDL file from: %s" % file
                compare_xml_sdl(default_xml_filepath(), f.name, update=update)
        else:
            f = os.path.join(SOURCE_PATH, RELATIVE_PATH, file)
            if file.endswith(".xml") or file.endswith(".xml.example"):
                print "XML file: %s" % file
                compare_xml_files(default_xml_filepath(), f, update=update)
            elif file.endswith(".sdl"):
                print "SDL file: %s" % file
                compare_xml_sdl(default_xml_filepath(), f, update=update)
            else:
                print "Unknown filetype for: %s" % file
            
def main():
    """ Main program entry point."""

    options, args = parse_options()

    try:
        options.logger = log
        if options.compare_all:
            print "\nComparing all XML Files (update=%s) to '%s'...\n" % (options.compare_all_update, CDAP_DEFAULT_XML)
            compare_all_xml(options)
        if options.compare:
            print "\nComparing XML Files... "
            compare_xml(options.source, options.other_source, options.target)
        if options.generate:
            print "Generating RST..."
            load_create_rst(options.source, options.target, options.ignore)
        if options.build:
            print "Building XML file..."
            rebuild(filepath = default_xml_filepath('_revised.xml'))
        if options.load_props:
            print "Loading XML file and printing properties..."
            load_summary(source=options.source, props_only=True, ignore=options.ignore)
        if options.load_quick:
            print "Loading XML file and printing summary..."
            load_summary()
        if options.load_xml:
            print "Loading XML file, rebuilding, and printing XML..."
            rebuild()
        if options.load_xml_to_rst:
            print "Loading default XML file and printing RST..."
            load_create_rst('', '')

    except Exception, e:
        sys.stderr.write("Error: %s\n" % e)
        sys.exit(1)
            
if __name__ == '__main__':
    main()
