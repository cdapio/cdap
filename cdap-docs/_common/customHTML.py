# -*- coding: utf-8 -*-

# Copyright © 2016 Cask Data, Inc.
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

"""
    customHTML
    ~~~~~~~~~~

    Docutils writer that revises the handling of title nodes.
    It reverses the structure so that the permalink precedes the headline text.

    :copyright: Copyright 2016 by Cask Data, Inc.
    :license: Apache License, Version 2.0, see http://www.apache.org/licenses/LICENSE-2.0
    :version: 0.2

"""

from docutils.writers.html4css1 import HTMLTranslator as BaseTranslator

from sphinx.locale import _
from sphinx.writers.html import HTMLTranslator

class CustomHTMLTranslator(HTMLTranslator):
    """
    Our custom, custom HTML translator.
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
