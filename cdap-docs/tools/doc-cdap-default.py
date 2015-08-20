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
import xml.etree.ElementTree as ET

RELATIVE_PATH = '../..'
CDAP_DEFAULT_XML = 'cdap-common/src/main/resources/cdap-default.xml'
CDAP_DEFAULT_EXCLUSIONS = 'cdap-default-exclusions.txt'

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


class Item:

    def __init__(self, name='', value='', description ='', final=None):
        self.name = name
        self.value = value
        self.description = description
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
        if self.value.find('\n') != -1:
            value = BLOCK_START + BLOCK_JOIN.join(self.value.split()) + '``'
        else:
            value = "``%s``" % self.value
        rst = "%s%s\n%s%s\n%s%s" % (NAME_START, name,
                                    VALUE_START, value,
                                    DESC_START, self.description)
        return rst
    
    def set_attribute(self, name, value):
        v1 = "%s" % value
        if name == 'value':
            v = "\n".join(v1.split())
        elif name == 'description' and v1 == 'None':
            v = ''
        elif name == 'final':
            v = (v1.lower() == 'true')
        else:
            v = " ".join(v1.split())
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
    
    
class Section:

    def __init__(self, name=''):
        self.name = name
        self.final = None

    def rst(self):
        CONFIG = ' Configuration'
        name = self.name.strip()
        if name.endswith(CONFIG):
            name = name[0:-len(CONFIG)]
        underline = "-" * len(name)
        rst = "\n%s\n%s\n%s" % (name, underline, RST_TABLE_HEADER)
        return rst
       

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
       
       
def load_defaults(filesource):
    func = 'load_defaults'
    # Set paths
    source_path = os.path.dirname(os.path.abspath(__file__))
    
    if filesource:
        xml_path = filesource    
    else:
        xml_path = os.path.join(source_path, RELATIVE_PATH, CDAP_DEFAULT_XML)
    if not os.path.isfile(xml_path):
        raise Exception(func, "'%s' not a valid path" % xml_path)
    
    exclusions_path = os.path.join(source_path, CDAP_DEFAULT_EXCLUSIONS)
    if not os.path.isfile(exclusions_path):
        raise Exception(func, "'%s' not a valid path" % exclusions_path)
    exclusions = [line.rstrip('\n') for line in open(exclusions_path)]
    
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
                section = Section()
                section.name = inner_element.text
                items.append(section)

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
    
def main():
    """ Main program entry point.
    """
    filesource = ''
    filepath = ''
    if len(sys.argv) > 2:
        filesource = sys.argv[1]
        filepath = sys.argv[2]
    elif len(sys.argv) > 1:
        filepath = sys.argv[1]
            
    defaults = load_defaults(filesource)
    table = create_rst(defaults)
    
    if filepath:
        f = open(filepath, 'w')
        f.write(table)
        f.close()
    else:
        print table
    
    
if __name__ == '__main__':
    main()

