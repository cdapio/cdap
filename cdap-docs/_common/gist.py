#-*- coding:utf-8 -*-

from docutils import nodes

from docutils.parsers import rst


class gist(nodes.General, nodes.Element):
    pass



def visit(self, node):

    tag = u'''<script src="{0}.js">&nbsp;</script>'''.format(node.url)

    self.body.append(tag)



def depart(self, node):
    pass



class GistDirective(rst.Directive):

    name = 'gist'
    node_class = gist

    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {}


    def run(self):

        node = self.node_class()

        node.url = self.arguments[0]

        return [node]

