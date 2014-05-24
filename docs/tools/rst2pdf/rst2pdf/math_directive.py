# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms

from docutils.parsers import rst
from docutils.parsers.rst import directives
from docutils.nodes import General, Inline, Element
from docutils import utils
from docutils.parsers.rst import roles

import basenodehandler, math_flowable

if 'Directive' in rst.__dict__:

    class Math(rst.Directive):
        has_content = True
        required_arguments = 0
        optional_arguments = 1
        final_argument_whitespace = True
        option_spec = {
            'label': directives.unchanged,
            'fontsize': directives.nonnegative_int,
            'color': directives.unchanged,
            'nowrap': directives.flag,
        }

        def run(self):

            latex = '\n'.join(self.content)
            if self.arguments and self.arguments[0] and latex:
                latex = self.arguments[0] + '\n\n' + latex
            if self.arguments and self.arguments[0] and not latex:
                latex = self.arguments[0]
            label=self.options.get('label', None)
            fontsize=self.options.get('fontsize', None)
            color=self.options.get('color', None)
            return [math_node(latex=latex,
                              label=label,
                              fontsize=fontsize,
                              color=color,
                              rawsource=''.join(self.content))]

        def __repr__(self):
            return u''.join(self.content)

else:

    def Math(name, arguments, options, content, lineno,
            content_offset, block_text, state, state_machine):
        return [math_node(latex=''.join(content), rawsource=''.join(content))]

    Math.content = True

directives.register_directive('math', Math)


class math_node(General, Inline, Element):
    children = ()

    def __init__(self, rawsource='', label=None, fontsize=12, color='black', *children, **attributes):
        self.rawsource = rawsource
        self.math_data = attributes['latex']
        self.label = label
        self.fontsize = fontsize
        self.color = color
        Element.__init__(self, rawsource, *children, **attributes)


def math_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    latex = utils.unescape(text, restore_backslashes=True)
    return [math_node(latex, latex=latex)], []

roles.register_local_role('math', math_role)

class eq_node(Inline, Element):
    def __init__(self, rawsource='', label=None, *children, **attributes):
        self.label=label
        Element.__init__(self, rawsource, *children, **attributes)

def eq_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    return [eq_node(label=text)],[]

roles.register_local_role('eq', eq_role)


class HandleMath(basenodehandler.NodeHandler, math_node):
    def gather_elements(self, client, node, style):
        if not node.fontsize:
            node.fontsize=style.fontSize
        if not node.color:
            node.color=style.textColor.rgb()
        return [math_flowable.Math(node.math_data,node.label,node.fontsize,node.color)]

    def get_text(self, client, node, replaceEnt):
        #get style for current node
        sty=client.styles.styleForNode(node)
        node_fontsize=sty.fontSize
        node_color='#'+sty.textColor.hexval()[2:]
        mf = math_flowable.Math(node.math_data,label=node.label,fontsize=node_fontsize,color=node_color)
        w, h = mf.wrap(0, 0)
        descent = mf.descent()
        img = mf.genImage()
        client.to_unlink.append(img)
        return '<img src="%s" width=%f height=%f valign=%f/>' % (
            img, w, h, -descent)

class HandleEq(basenodehandler.NodeHandler, eq_node):

    def get_text(self, client, node, replaceEnt):
        return '<a href="equation-%s" color="%s">%s</a>'%(node.label,
            client.styles.linkColor, node.label)

