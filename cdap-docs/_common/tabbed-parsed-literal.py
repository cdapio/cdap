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

"""Simple, inelegant Sphinx extension which adds a directive for a
tabbed parsed-literals that may be switched between in HTML.  

The directive takes an optional language parameter and an optional tabs parameter:

:tabs: comma-separated list of tabs; default 'Linux,Windows'
:language: comma-separated list of pygments languages; default 'console`
:classes: class of container; default `highlight`

Separate the code blocks with matching comment lines. Tabs must follow in order of :tabs:
option. Comment labels are for convenience, and don't need to match. Note example uses a
tab label with a space in it, and is enclosed in quotes.

Example:

.. tabbed-parsed-literal::
    :tabs: "Linux or OS/X",Windows
    :languages: console,shell-session
    
    .. Linux

    $ cdap-cli.sh start flow HelloWorld.WhoFlow
    Successfully started flow 'WhoFlow' of application 'HelloWorld' with stored runtime arguments '{}'
    
    $ curl -o /etc/yum.repos.d/cask.repo http://repository.cask.co/centos/6/x86_64/cdap/|short-version|/cask.repo
    
    .. Windows
    
    > cdap-cli.bat start flow HelloWorld.WhoFlow
    Successfully started flow 'WhoFlow' of application 'HelloWorld' with stored runtime arguments '{}'

    > <CDAP-SDK-HOME>\libexec\bin\curl.exe -X POST 'http://repository.cask.co/centos/6/x86_64/cdap/|short-version|/cask.repo'

"""

import ast

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from docutils.parsers.rst.directives.body import ParsedLiteral
from docutils.parsers.rst.roles import set_classes

TPL_COUNTER = 0

JS_TPL = """\
<script type="text/javascript">

function changeExampleTab(example) {
  return function(e) {
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $(".tab-pane").removeClass("active");
    $(".tab-pane-" + example).addClass("active");
    $(".example-tab").removeClass("active");
    $(".example-tab-" + example).addClass("active");
    $(document).scrollTop($(this).offset().top - scrollOffset);
  }
}

$(function() {
  var examples = %s;
  for (var i = 0; i < examples.length; i++) {
    var example = examples[i];
    $(".example-tab-" + example).click(changeExampleTab(example));
  }
});

</script>
"""

DIV_START = """
<div id="{div_name}" >
"""

NAV_TABS = """
<ul class="nav nav-tabs">
%s
</ul>
"""

NAV_TABS_ENTRY = """
<li class="example-tab example-tab-{tab_link} {active}"><a href="#">{tab_name}</a></li>
"""

TAB_CONTENT_START = """
<div class="tab-content">
"""

DIV_END = """
</div>
"""

TAB_CONTENT_ENTRY_START = """
<div class="tab-pane tab-pane-{tab_link} {active}">
<div class="code code-tab">
"""

DIV_DIV_END = """
</div>
</div>
"""


def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.
    """
    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]
    return s
    
    
class TabbedParsedLiteralNode(nodes.literal_block):
    pass


class TabbedParsedLiteral(ParsedLiteral):
    """TabbedParsedLiteral is a set of different blocks"""

    option_spec = dict(languages=str, tabs=directives.unchanged_required, **ParsedLiteral.option_spec)
    has_content = True

    def cleanup_content(self):
        """Parses content, looks for comment markers, removes them, prepares backslashes.
           Calculates size for each block.
        """
    
        line_sets = []
        line_set = []
        for line in self.content:
            if line.startswith('.. '):
                if line_set:
                    line_sets.append(line_set)
                    line_set = []
            else:
                line_set.append(line)
        line_sets.append(line_set)
    
        line_counts = []
        lines = []
        for line_set in line_sets:
            block = '\n'.join(line_set).strip().replace('\\', '\\\\')
            if not block.endswith('\n'):
                block += '\n'
            lines.append(block)
            line_counts.append(block.count('\n') +1)
    
        return line_counts, lines
    
    def cleanup_options(self, option, default):
        """Removes leading and trailing quotes and double-quotes from a string."""
        _option = self.options.get(option,'')
        if not _option:
            return default
        else:
            _options = []
            for s in _option.split(","):
                _options.append(dequote(s))         
            return _options
    
    def run(self):
        set_classes(self.options)
        self.assert_has_content()

        line_counts, lines = self.cleanup_content()
        text = '\n'.join(lines)
        text_nodes, messages = self.state.inline_text(text, self.lineno)
        node = TabbedParsedLiteralNode(text, '', *text_nodes, **self.options)
        node.line = self.content_offset + 1
        self.add_name(node)
        
        node['languages'] = self.cleanup_options('languages', ['console', 'shell-session'])
        node['line_counts'] = line_counts
        node['linenos'] = self.cleanup_options('linenos', '')
        node['tabs'] = self.cleanup_options('tabs', ['Linux', 'Windows'])
        return [node] + messages
        
def visit_tpl_html(self, node):
    """Visit a Tabbed Parsed Literal node"""
    global TPL_COUNTER
    TPL_COUNTER += 1

    def _highlighter(node, text, lang='console'):
        linenos = text.count('\n') >= \
            self.highlightlinenothreshold - 1
        highlight_args = node.get('highlight_args', {})
        if lang:
            # code-block directives
            highlight_args['force'] = True
        if 'linenos' in node:
            linenos = node['linenos']
        if lang is self.highlightlang_base:
            # only pass highlighter options for original language
            opts = self.highlightopts
        else:
            opts = {}
        def warner(msg):
            self.builder.warn(msg, (self.builder.current_docname, node.line))
        highlighted = self.highlighter.highlight_block(
            text, lang, opts=opts, warn=warner, linenos=linenos,
            **highlight_args)
        if lang in ['console', 'shell-session', 'ps1', 'powershell']:
            for p in ['$', '#', '>', '&gt;', 'cdap >','cdap &gt;',]:
                highlighted = highlighted.replace("\n%s" % p, "\n<span class=\"gp\">%s</span>" % p)
                highlighted = highlighted.replace("<pre>%s" % p, "<pre><span class=\"gp\">%s</span>" % p)
                highlighted = highlighted.replace("<span class=\"go\">%s" % p, 
                                                  "<span class=\"gp\">%s</span><span class=\"go\">" % p)
        return highlighted
    
    nav_tabs_html = ''
    tab_content_html = ''
    languages = node.get('languages')
    line_counts = node.get('line_counts')
    tabs = node.get('tabs')
    clean_tabs = [str(tab) for tab in tabs]
    clean_tab_links = [tab.replace(' ', '-').lower() for tab in clean_tabs]
    fill_div = {'div_name': 'tabbedparsedliteral{0}'.format(TPL_COUNTER)}
    
    start_html = JS_TPL % clean_tab_links + DIV_START.format(**fill_div)
    text_list = node.astext().split('\n')
    offset = 0
    for index in range(len(tabs)):
        lang, lines = languages[index], line_counts[index]
        tab_name, tab_link = clean_tabs[index], clean_tab_links[index]
        start_tag = self.starttag(node, 'div', suffix='', CLASS='highlight-%s' % lang)
        tab_text = text_list[offset:offset + lines]
        offset += lines
        # Strip any leading empty lines
        text = ''
        for line in tab_text:
            if not line and not text:
                continue
            elif not text:
                text = line
            else:
                text += '\n' + line
        highlighted = _highlighter(node, text, lang)
        tab_options = {'active': 'active' if not index else '',
                       'tab_link': tab_link,
                       'tab_name': tab_name,}

        nav_tabs_html += NAV_TABS_ENTRY.format(**tab_options)
        tab_entry_start = TAB_CONTENT_ENTRY_START.format(**tab_options)
        tab_content_html += tab_entry_start + start_tag + highlighted + DIV_END + DIV_DIV_END
                                                    
    nav_tabs_html = NAV_TABS % nav_tabs_html
    tab_content_html = TAB_CONTENT_START + tab_content_html + DIV_END
    self.body.append(start_html + nav_tabs_html + tab_content_html + DIV_END)
    raise nodes.SkipNode

def depart_tpl_html(self, node):
    """Depart a Tabbed Parsed Literal node"""
    # Stub because of SkipNode in visit

def setup(app):
    app.add_directive('tabbed-parsed-literal', TabbedParsedLiteral)
    app.add_node(TabbedParsedLiteralNode, html=(visit_tpl_html, depart_tpl_html))
