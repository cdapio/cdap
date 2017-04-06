# -*- coding: utf-8 -*-

# Copyright © 2016-2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from docutils.writers.html4css1 import HTMLTranslator as BaseTranslator
from sphinx.jinja2glue import BuiltinTemplateLoader
from sphinx.locale import _
from sphinx.writers.html import HTMLTranslator

import xml.etree.ElementTree as ET
import sys

"""
    CustomHTMLTranslator
    ~~~~~~~~~~~~~~~~~~~~
"""

class CustomHTMLTranslator(HTMLTranslator):
    """
    A custom HTML translator.

    A Docutils translator that revises the handling of title nodes.
    It reverses the structure so that the permalink precedes the headline text.

    :copyright: Copyright 2016-2017 by Cask Data, Inc.
    :license: Apache License, Version 2.0, see http://www.apache.org/licenses/LICENSE-2.0
    :version: 0.3

    """

    def depart_title(self, node):
        h_level = 0
        close_tag = self.context[-1]
        close = close_tag.rfind('>')
        if close != -1:
            h_level = close_tag[close-1]
        if (h_level and h_level.isdigit() and (close == close_tag.rfind('h%s>' % h_level) + 2) and self.permalink_text
                and self.builder.add_permalinks and node.parent.hasattr('ids') and node.parent['ids']):
            aname = node.parent['ids'][0]
            tags = []
            open_tag = "<h%s>" % h_level
            # Walk back to find open_tag in body
            for i in reversed(range(len(self.body))):
                tags.append(self.body.pop())
                if tags[-1] == open_tag:
                    self.body.append(tags.pop())
                    tags.reverse()
                    break

            # <h1>Manual Installation using Packages<a class="headerlink" href="#manual-installation-using-packages"
            # title="Permalink to this headline">¶</a></h1>
            # becomes
            # <h1><a class="headerlink" href="#manual-installation-using-packages"
            # title="Permalink to this headline">¶</a>Manual Installation using Packages</h1>

            if close_tag.startswith('</h'):
                self.body.append(u'<a class="headerlink" href="#%s" ' % aname +
                                 u'title="%s">%s</a>' % (_('Perma-link to this heading'), self.permalink_text))
            elif close_tag.startswith('</a></h'):
                self.body.append(u'</a><a class="headerlink" href="#%s" ' % aname +
                                 u'title="%s">%s' % (_('Perma-link to this heading'), self.permalink_text))
            self.body = self.body + tags

        BaseTranslator.depart_title(self, node)


"""
    CustomTemplateBridge
    ~~~~~~~~~~~~~~~~~~~~
"""

def _getcurrentchildren(root):
    """
    For a given root, finds the first child with a class attribute with "current",
    and for that child, returns the text of its first 'a' child node and first 'ul' child node.
    """
    current = None
    current_link = None
    current_text = None
    current_ul = None
    for child in root.getchildren():
        if 'current' in child.attrib['class'].split():
            current = child
            current_children = child.getchildren()
            break
    if current and current_children:
        for child in current_children:
            if child.tag == 'a':
                if not current_link and 'href' in child.keys():
                    current_link = child.attrib['href']
                if not current_text:
                    current_text = child.text
            if not current_ul and child.tag == 'ul':
                current_ul = child
    return (current_text, current_link, current_ul)

def _walktoc(html):
    """
    Walks down given HTML, and finds all the "current" class items.
    """
    breadcrumbs = list()
    current_text = None
    current_link = None
    try:
        html_encoded = html.encode('utf-8', 'replace')
        root = ET.fromstring(html_encoded)
        while root:
            current_text, current_link, root = _getcurrentchildren(root)
            if current_text or root:
                breadcrumbs.append((current_text, current_link))
        del root
    except Exception, e:
        sys.stderr.write("Error in _walktoc (current_text: %s current_link: %s): %s\n%s\n" %
            (current_text, current_link, e, html_encoded))
    return breadcrumbs


class CustomTemplateBridge(BuiltinTemplateLoader):
    """
    CustomTemplateBridge
    ~~~~~~~~~~~~~~~~~~~~

    Interfaces the rendering environment of jinja2 for use in Sphinx.
    Adds a custom template bridge class so that custom methods can be added to the Jinja Template environment.

    :copyright: Copyright 2017 by Cask Data, Inc.
    :license: Apache License, Version 2.0, see http://www.apache.org/licenses/LICENSE-2.0
    :version: 0.1

    """

    def init(self, builder, theme=None, dirs=None):
        # Note that the init method is not an __init__ constructor method.
        BuiltinTemplateLoader.init(self, builder, theme, dirs)
        # Add filters to the Jinja environment
        self.environment.filters['walktoc'] = _walktoc
