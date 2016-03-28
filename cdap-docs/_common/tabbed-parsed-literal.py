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

The directive adds these parameters, both optional:

    :language: comma-separated list of pygments languages; default 'console`

    :tabs: comma-separated list of tabs; default 'Linux,Windows'

Separate the code blocks with matching comment lines. Tabs must follow in order of :tabs:
option. Comment labels are for convenience, and don't need to match. Note example uses a
tab label with a space in it, and is enclosed in quotes.

Note that slightly different rule operate for replacements: a replacement such as
"\|replace|" will work, and the backslash will be interpreted as a single backslash rather
than as escaping the "|".

It is best if all tabs on a page are identical. If not, changing an active tab will cause other
tabs in other sets to become "inactive" (ie, disappear).

FIXME: Add a concept of "tab labels" versus "tab keys", and control so they can be independent.

Examples:

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

    > <CDAP-SDK-HOME>\libexec\bin\curl.exe -d c:\|release| -X POST 'http://repository.cask.co/centos/6/x86_64/cdap/|short-version|/cask.repo'

If you pass a single set of commands, without comments, the directive will create a
two-tabbed "Linux" and "Windows" with a generated Windows-equivalent command set. Check
the results carefully, and file an issue if it is unable to create the correct command.
Worst-case is that you have to use the full format and enter the two commands.

.. tabbed-parsed-literal::

    $ cdap-cli.sh start flow HelloWorld.WhoFlow
    Successfully started flow 'WhoFlow' of application 'HelloWorld' with stored runtime arguments '{}'
    
    $ curl -o /etc/yum.repos.d/cask.repo http://repository.cask.co/centos/6/x86_64/cdap/|short-version|/cask.repo

JavaScript and design of tabs was taken from the Apache Spark Project:
http://spark.apache.org/examples.html

"""

import ast

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from docutils.parsers.rst.directives.body import ParsedLiteral
from docutils.parsers.rst.roles import set_classes

DEFAULT_TABS = ['Linux', 'Windows']

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
    localStorage.setItem("cdap-documentation-tab", example);
  }
}

$(function() {
  var examples = %s;
  for (var i = 0; i < examples.length; i++) {
    var example = examples[i];
    $(".example-tab-" + example).click(changeExampleTab(example));
  }
});

$(document).ready(function() {
  var example = localStorage.getItem("cdap-documentation-tab");
  var tabs = $(".example-tab-" + example);
  if (example && tabs) {
    try {
      $(".example-tab-" + example)[0].click(changeExampleTab(example));
    } catch (e) {
      console.log("Unable to set using Cookie: " + example);
    }
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
    
def convert(c):
    """
    Converts a Linux command to a Windows-equivalent following a few simple rules:

    - Converts a starting '$' to '>'
    - Forward-slashes in 'http[s]' and 'localhost:10000' URIs are preserved
    - Other forward-slashes become backslashes
    - A lone backslash (the Linux line continuation character) becomes a '^'
    - '.sh' commands become '.bat' commands
    - removes a "-w'\n'" option from curl commands

    """
    w = []
    text_list = c.split()
    CLI = 'cdap-cli.sh'
    CURL = 'curl'
    IN_CLI = False
    IN_CURL = False
#     print "convert: %s" % c
    for i, v in enumerate(c.split()):
#         print "v:%s" % v
        if v == CLI:
            IN_CLI = True
        if v == CURL:
            IN_CURL = True
        if i == 0 and v == '$':
            w.append('>')
#             print "w.append('>')"
            continue
        if v.endswith('.sh'):
            w.append(v.replace('.sh', '.bat'))
#             print "w.append(v.replace('.sh', '.bat'))"
            continue
        if v == '\\':
            w.append('^')
#             print "w.append('^')"
            continue
        if IN_CURL and (v in ["-w'\\n'", '-w"\\n"']):
            continue
        if (IN_CLI or IN_CURL) and v.startswith('"'):
#             print "v.startswith('\"')"
            w.append(v)
            continue
        if (not IN_CLI) and v.find('/') != -1:
#             print "found slash: v: %s" % v
#             print "converting: %s" % c
            if IN_CURL and v.startswith('localhost'):
#                 print "IN_CURL and v.startswith('localhost')"
                w.append(v)
            else:
                w.append(v.replace('/', '\\'))
        else:
#             print "didn't find slash"
            w.append(v)
    return ' '.join(w)


class TabbedParsedLiteralNode(nodes.literal_block):
    """TabbedParsedLiteralNode is an extended literal_block that supports replacements."""

    def cleanup(self):
        for i, v in enumerate(self.traverse()):
            if isinstance(v, nodes.Text):
                t = v.astext()
                if t.endswith('.\ ') or t.endswith('=\ '):
                    t = t[:-2]
                if t.find('\`') != -1:
                    t = t.replace('\`', '`')
                if t != v.astext():
                    self.replace(v, nodes.Text(t))

class TabbedParsedLiteral(ParsedLiteral):
    """TabbedParsedLiteral is a set of different blocks"""

    option_spec = dict(languages=str, tabs=directives.unchanged_required, **ParsedLiteral.option_spec)
    has_content = True

    def cleanup_content(self):
        """Parses content, looks for comment markers, removes them, prepares backslashes.
           Calculates size for each block.
        """
        content = self.content
        text_block = '\n'.join(content)
        
        if not text_block.startswith('.. ') or text_block.index('\n.. ') == -1:
            # There are no comments... generating a Windows-equivalent code
            LINUX = ['.. Linux', '']
            WINDOWS = ['', '.. Windows', '']
            old_content = []
            new_content = []
            for line in self.content:
                old_content.append(line)
                new_content.append(convert(line))
            content = LINUX + old_content + WINDOWS + new_content
#             print "old_content:\n%s\nnew_content:\n%s\n" % (old_content, new_content)

        line_sets = []
        line_set = []
        for line in content:
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
            block = '\n'.join(line_set).strip()
            block = block.replace('\\', '\\\\')
            block = block.replace('\\|', '\\\ |')
            if not block.endswith('\n'):
                block += '\n'
            lines.append(block)
            line_counts.append(block.count('\n') +1)
    
        return line_counts, lines
    
    def cleanup_options(self, option, default):
        """Removes leading or trailing quotes or double-quotes from a string option."""
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
#         print "text:\n%s" % text
        # Sending text to state machine for inline text replacement
        text_nodes, messages = self.state.inline_text(text, self.lineno)
        
#         print "text_nodes:\n%s" % text_nodes
#         for n in text_nodes:
#             print "n:\n%s" % n
#              n['classes'].append('snippet')
        
        node = TabbedParsedLiteralNode(text, '', *text_nodes, **self.options)
        node.cleanup()
        node.line = self.content_offset + 1
        self.add_name(node)

       
        node['languages'] = self.cleanup_options('languages', ['console', 'shell-session'])
        node['line_counts'] = line_counts
        node['linenos'] = self.cleanup_options('linenos', '')
        node['tabs'] = self.cleanup_options('tabs', DEFAULT_TABS)
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

#             print "highlighted (before):\n%s" % highlighted
            
            # Console-specific highlighting
            new_highlighted = ['<!-- tabbed-parsed-literal start -->']
            continuing_line = False # Indicates current line continues to next
            continued_line = False # Indicates current line was continued from previous
            copyable_text = False # Indicates that the line (or the previous) now has copyable text in it
            for l in highlighted.splitlines():
                continuing_line = False
                if l:
                    continuing_line = l.endswith('\\</span>') or l.endswith('^</span>')    
#                 print "continuing_line: %s continued_line: %s l: %s" % (continuing_line, continued_line, l)
                
                for p in ['$', '#', '>', '&gt;', 'cdap >','cdap &gt;',]:
                    if l.startswith(p):
                        l = "<span class=\"gp\">%s</span><span \"copyable copyable-text\">%s" % (p, l[1:])
                        copyable_text = True
                        break
                        
                    t = "<pre>%s " % p
                    i = l.find(t)
                    if i != -1:
                        l = "%s<pre class=\"copyable\"><span class=\"gp\">%s </span><span \"copyable-text\">%s" % (l[:i], p, l[len(t)+i:])
                        copyable_text = True
                        break
                                                
                    t = "<pre><span class=\"go\">%s " % p
                    i = l.find(t)
                    if i != -1:
                        l = "%s<pre class=\"copyable\"><span class=\"gp\">%s </span><span class=\"copyable-text\"><span class=\"go\">%s" % (l[:i], p, l[len(t)+i:])
                        copyable_text = True
                        break
                    
                    t = "<pre><span class=\"gp\">%s</span> " % p
                    i = l.find(t)
                    if i != -1:
                        l = "%s<pre class=\"copyable\"><span class=\"gp\">%s </span><span class=\"copyable-text\">%s" % (l[:i], p, l[len(t)+i:])
                        copyable_text = True
                        break

                    t = "<span class=\"go\">%s " % p
                    if l.startswith(t):
                        if continued_line:
                            l = "<span class=\"gp\">%s </span><span class=\"go\">%s" % (p, l[len(t):])
                        else:
                            l = "<span class=\"gp\">%s </span><span class=\"copyable-text\"><span class=\"go\">%s" % (p, l[len(t):])
                            copyable_text = True
                        break
                        
                    t = "<span class=\"gp\">%s</span> " % p
                    if l.startswith(t):
                        if continued_line:                    
                            l = "<span class=\"gp\">%s </span>%s" % (p, l[len(t):])
                        else:
                            l = "<span class=\"gp\">%s </span><span class=\"copyable-text\">%s" % (p, l[len(t):])
                            copyable_text = True
                        break

#                 print "continuing_line: %s continued_line: %s copyable_text: %s l: %s" % (continuing_line, continued_line, copyable_text, l)
                if continued_line and (not continuing_line) or (not continued_line and not continuing_line and copyable_text):
                    # End the copyable-text
                    l += "</span>"
                    copyable_text = False

                new_highlighted.append(l)
                # Set next line status
                continued_line = continuing_line
            
            new_highlighted.append('<!-- tabbed-parsed-literal end -->')                
#             print "\nhighlighted (after):\n%s\n\n" % '\n'.join(new_highlighted)
                      
        return '\n'.join(new_highlighted)
    
    nav_tabs_html = ''
    tab_content_html = ''
    languages = node.get('languages')
    line_counts = node.get('line_counts')
    tabs = node.get('tabs')
    clean_tabs = [str(tab) for tab in tabs]
    clean_tab_links = [tab.replace(' ', '-').lower() for tab in clean_tabs]
    fill_div = {'div_name': 'tabbedparsedliteral{0}'.format(TPL_COUNTER)}
    
    start_html = JS_TPL % clean_tab_links + DIV_START.format(**fill_div)
#     start_html = DIV_START.format(**fill_div)
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
