#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright © 2015 Cask Data, Inc.
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

import os
import sys
import textwrap
import xml.etree.ElementTree as ET

from datetime import datetime

RELATIVE_PATH = '../..'
CDAP_DEFAULT_XML = 'cdap-common/src/main/resources/cdap-default.xml'
CDAP_DEFAULT_EXCLUSIONS = 'cdap-default-exclusions.txt'

FIRST_TWO_SECTIONS = ['General Configuration', 'Global Configuration']


RST_TABLE_HEADER = """
.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter Name
     - Default Value
     - Description"""

RST_TABLE_HEADER_FINAL = """

Final Configurations
--------------------
These properties are marked as ``final``, meaning that their value cannot be changed, even
with a setting in the ``cdap-site.xml``:
""" + RST_TABLE_HEADER + '\n'

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


class Item:
    @staticmethod
    def encode(value):
        import cgi
        if value:
            return cgi.escape(value).encode('ascii', 'xmlcharrefreplace')
        else:
            return ''

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
        name = "``%s``" % self.name
        if self.value.find(' ') != -1:
            value = BLOCK_START + BLOCK_JOIN.join(self.value.split()) + '``'
        elif not self.value:
            value = ''
        else:
            value = "``%s``" % self.value
        rst = "%s%s\n%s%s\n%s%s" % (NAME_START, name,
                                    VALUE_START, value,
                                    DESC_START, self.description)
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
        rst = "\n%s\n%s\n%s" % (name, underline, RST_TABLE_HEADER)
        return rst

    def display(self):
        print "\nSection %s" % self.name
       

class PIParser(ET.XMLTreeBuilder):
    """An XML parser that preserves processing instructions and comments"""

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


def default_xml_filepath(extra_path=''):
    source_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(source_path, RELATIVE_PATH, CDAP_DEFAULT_XML + extra_path)
    
def load_exclusions():
    source_path = os.path.dirname(os.path.abspath(__file__))
    exclusions_path = os.path.join(source_path, CDAP_DEFAULT_EXCLUSIONS)
    if not os.path.isfile(exclusions_path):
        raise Exception(func, "'%s' not a valid path" % exclusions_path)
    return [line.rstrip('\n') for line in open(exclusions_path)]

def load_defaults(filesource, include_exclusions=False):
    func = 'load_defaults'

    if filesource:
        xml_path = filesource    
    else:
        xml_path = default_xml_filepath()
    if not os.path.isfile(xml_path):
        raise Exception(func, "'%s' not a valid path" % xml_path)
    
    if include_exclusions:
        exclusions = []
    else:
        exclusions = load_exclusions()
    
    items = []
    parser = PIParser()
    tree = ET.parse(xml_path, parser=parser)
    root = tree.getroot()
    for outer_element in root:
        for inner_element in outer_element:
            if inner_element.tag == 'property':
                item = Item()
                for attribute in inner_element:
                    item.set_attribute(attribute.tag, attribute.text)                
                if item.name not in exclusions:
                    items.append(item)                
            else:
                items.append(Section(inner_element.text))

    return items

def create_rst(items):
    """Create rst from items"""
    table = ''
    final_items =[]
    
    for item in items:
        if not item.final:
            table += item.rst() + '\n'
        else:
            final_items.append(item)
            
    if final_items:
        table += RST_TABLE_HEADER_FINAL
        for item in final_items:
            table += item.rst() + '\n'

    return table

def rebuild():
    """Loads the cdap-default.xml, and rebuilds it according to these rules:
    - First two sections: 
        General Configuration
        Global Configuration
    - Remaining sections alphabetical
    - Properties within a section alphabetical
    - Descriptions wrapped to a 70 character line-length"""
    
    print 'Rebuilding cdap-default.xml'
    items = load_defaults('', include_exclusions=True)
    exclusions = load_exclusions()
    in_section = True
    defaults = {}
    section = []
    section_counter = 0
    properties_counter = 0
    for item in items:
        if str(item.__class__) == '__main__.Section':
            if section: # End old section
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
    XML_CONFIG_OPEN   = '<configuration>\n'
    XML_SECTION_SUB   = "\n    <!-- %s -->\n\n"
    XML_PROP_OPEN     = '    <property>\n'
    XML_NAME_SUB      = "        <name>%s</name>\n"
    XML_VALUE_SUB     = "        <value>%s</value>\n"
    XML_DESCRIP_OPEN  = '        <description>\n'
    XML_DESCRIP_SUB   = "            %s\n"
    XML_DESCRIP_CLOSE = '        </description>\n'
    XML_FINAL         = '        <final>true</final>\n'
    XML_PROP_CLOSE    = '    </property>\n\n'
    XML_CONFIG_CLOSE  = '</configuration>\n'

    xml = XML_CONFIG_OPEN
    keys = defaults.keys()
    for key in FIRST_TWO_SECTIONS:
        keys.remove(key)
    keys.sort()        
    keys = FIRST_TWO_SECTIONS + keys
    for key in keys:
        xml += XML_SECTION_SUB % key
        props = defaults[key]
        props.sort(key = lambda p: p.name)
        for prop in props:
            xml += XML_PROP_OPEN
            xml += XML_NAME_SUB % prop.name
            xml += XML_VALUE_SUB % prop.value
            xml += XML_DESCRIP_OPEN
            if prop.description:
                for line in textwrap.wrap(prop.description):
                    xml += XML_DESCRIP_SUB % line
            else:
                print "WARNING: No description for property %s" % prop.name
                if prop.name in exclusions:
                    print "but in the list of exclusions"
            xml += XML_DESCRIP_CLOSE
            if prop.final:
                xml += XML_FINAL
            xml += XML_PROP_CLOSE
    xml += XML_CONFIG_CLOSE
    
    filepath = default_xml_filepath('_revised.xml')
    f = open(filepath, 'wb')
    f.write(XML_HEADER.encode('utf8'))
    f.write(xml)
    f.close()

def load_create_rst(filesource='', filepath=''):
    defaults = load_defaults(filesource)
    table = create_rst(defaults)
    if filepath:
        f = open(filepath, 'w')
        f.write(table)
        f.close()
    else:
        print table
        
def load_xml(props_only=False):
    print 'Loading cdap-default.xml'
    items = load_defaults('')
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

def print_usage():
    print """Usage:
    --rst [source] [target]  Loads the existing default XML and creates an RST file from it
                             source: the XML to be loaded, if not the default
                             [optional, but required if target specified]
                             target: where to write the rst, if not standard output [optional]

      If both options are not specified, reads from default and writes to standard output                      

    --rebuild  Loads the existing default XML and rewrites it in the correct order to
               a file at the same location with '_revised.xml'

    --load-props  Loads the existing default XML and writes the properties to standard output
    --load-xml    Loads the existing default XML and writes it to standard output
    --print-rst   Loads the existing default XML and writes the RST to standard output

    --help  This help statement
"""

def main():
    """ Main program entry point.
    """
    if len(sys.argv) == 1:
        print_usage()
        return
    elif sys.argv[1] == '--rebuild':
        rebuild()
        return
    elif sys.argv[1] == '--load-props':
        load_xml(props_only=True)
        return
    elif sys.argv[1] == '--load-xml':
        load_xml()
        return
    elif sys.argv[1] == '--print-rst':
        load_create_rst('', '')
        return
    elif sys.argv[1] == '--help':
        print_usage()
        return
    elif sys.argv[1] == '--rst':
        filesource = ''
        filepath = ''
        if len(sys.argv) > 3:
            filesource = sys.argv[2]
            filepath = sys.argv[3]
        elif len(sys.argv) > 2:
            filepath = sys.argv[2]
        load_create_rst(filesource, filepath)
    else:
        print_usage()
        return    
    
if __name__ == '__main__':
    main()
