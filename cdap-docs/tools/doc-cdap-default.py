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

RST_HEADER = """
.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter name
     - Default Value
     - Description
"""


class Item:

    def __init__(self, name='', value='', description ='', final=''):
        self.name = name
        self.value = value
        self.description = description
        self.final = final
    
    def __str__(self):
        return "%s:\n%s\n%s" % (self.name, self.value, self.description)

    def _append(self, name, value):
        if self.__dict__[name]:
            self.__dict__[name] = "%s\n%s" % (self.__dict__[name], value)
        else:
            self.__dict__[name] = value
    
    def rst(self):
        NAME_START  = '   * - '
        VALUE_START = '     - '
        BLOCK_START = '| ``'
        BLOCK_JOIN  = '``\n       | ``'
        DESC_START  = '     - '
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
        if name == 'description' and v1 == 'None':
            v = ''
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
    

def load_defaults(filesource):
    func = 'load_defaults'
    # Set paths
    if filesource:
        xml_path = filesource    
    else:
        source_path = os.path.dirname(os.path.abspath(__file__))
        xml_path = os.path.join(source_path, RELATIVE_PATH, CDAP_DEFAULT_XML)
    if not os.path.isfile(xml_path):
        raise Exception(func, "'%s' not a valid path" % xml_path)
    
    tree = ET.parse(xml_path)
    root = tree.getroot()
    items = []
    for property in root:
        item = Item()
        for attribute in property:
            item.set_attribute(attribute.tag, attribute.text)
        items.append(item)

    return sorted(items, key=lambda item: item.name)


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

    # Create an rst table from items
    
    table = RST_HEADER
    for item in defaults:
        table = table + item.rst() + '\n'
    
    if filepath:
        f = open(filepath, 'w')
        f.write(table)
        f.close()
    else:
        print table
    
    
if __name__ == '__main__':
    main()
    