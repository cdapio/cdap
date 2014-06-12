# -*- coding: utf-8 -*-

"""A custom directive that allows alternative contents to be generated
on odd and even pages."""

from docutils.parsers import rst
from docutils.nodes import Admonition, Element
from docutils.parsers.rst import directives

class OddEvenNode(Admonition, Element):
    pass

class OddEven(rst.Directive):
    """A custom directive that allows alternative contents to be generated
    on odd and even pages. It can contain only two children, so use containers
    to group them. The first one is odd, the second is even."""

    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = True
    option_spec = {}
    has_content = True

    def run(self):
        self.assert_has_content()
        text = '\n'.join(self.content)
        node = OddEvenNode()
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]
    

directives.register_directive("oddeven", OddEven)
    