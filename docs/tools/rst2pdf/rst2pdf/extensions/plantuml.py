'''
A rst2pdf extension to implement something similar to sphinx's plantuml extension
(see http://pypi.python.org/pypi/sphinxcontrib-plantuml)

Therefore, stuff may be copied from that code.
Ergo:

    :copyright: Copyright 2010 by Yuya Nishihara <yuya@tcha.org>.
    :license: BSD, (he says see LICENSE but the file is not there ;-)

'''

import errno
from docutils import nodes
from docutils.parsers import rst
from docutils.parsers.rst import directives
import rst2pdf.genelements as genelements
from rst2pdf.image import MyImage
import tempfile
import subprocess

class plantuml(nodes.General, nodes.Element):
    pass


class UmlDirective(rst.Directive):
    """Directive to insert PlantUML markup

    Example::

        .. uml::
           :alt: Alice and Bob

           Alice -> Bob: Hello
           Alice <- Bob: Hi


    You can use a :format: option to change between SVG and PNG diagrams, however,
    the SVG plantuml generates doesn't look very good to me.
    """
    has_content = True
    option_spec = {'alt': directives.unchanged}
    option_spec = {'format': directives.unchanged}

    def run(self):
        node = plantuml()
        node['uml'] = '\n'.join(self.content)
        node['alt'] = self.options.get('alt', None)
        node['format'] = self.options.get('format', 'png')
        return [node]


class UMLHandler(genelements.NodeHandler, plantuml):
    """Class to handle UML nodes"""

    def gather_elements(self, client, node, style):
        # Create image calling plantuml
        tfile = tempfile.NamedTemporaryFile(dir='.', delete=False, suffix='.'+node['format'])
        args = 'plantuml -pipe -charset utf-8'
        if node['format'].lower() == 'svg':
            args+=' -tsvg'
        client.to_unlink.append(tfile.name)
        try:
            p = subprocess.Popen(args.split(), stdout=tfile,
                                 stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
            raise PlantUmlError('plantuml command %r cannot be run'
                                % self.builder.config.plantuml)
        serr = p.communicate(node['uml'].encode('utf-8'))[1]
        if p.returncode != 0:
            raise PlantUmlError('error while running plantuml\n\n' + serr)
        
        # Add Image node with the right image
        return [MyImage(tfile.name, client=client)]

directives.register_directive("uml", UmlDirective)
