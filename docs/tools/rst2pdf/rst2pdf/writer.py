# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms

from StringIO import StringIO

from docutils import writers

from rst2pdf import createpdf


class PdfWriter(writers.Writer):

    def __init__(self, builder):
        writers.Writer.__init__(self)
        self.builder = builder
        self.output = u''

    supported = ('pdf')
    """Formats this writer supports."""

    config_section = 'pdf writer'
    config_section_dependencies = ('writers')
    """Final translated form of `document`."""

    def translate(self):
        sio = StringIO('')
        createpdf.RstToPdf(sphinx=True).createPdf(
            doctree=self.document, output=sio, compressed=False)
        self.output = unicode(sio.getvalue(), 'utf-8', 'ignore')

    def supports(self, format):
        """This writer supports all format-specific elements."""
        return 1
