# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms

'''
inkscape.py is an rst2pdf extension (e.g. rst2pdf -e inkscape xxx xxxx)
which uses the inkscape program to convert an svg to a PDF, then uses
the vectorpdf code to process the PDF.

.. NOTE::

    The initial version is a proof of concept; uses subprocess in a naive way,
    and doesn't check return from inkscape for errors.
'''

import sys, os, tempfile, subprocess
from weakref import WeakKeyDictionary
from rst2pdf.log import log

from vectorpdf_r2p import VectorPdf
import rst2pdf.image


if sys.platform.startswith('win'):
    # note: this is the default "all users" install location,
    # we might want to provide an option for this
    progname = os.path.expandvars(r'$PROGRAMFILES\Inkscape\inkscape.exe')
else:
    progname = 'inkscape'

class InkscapeImage(VectorPdf):

    # The filecache allows us to only read a given PDF file once
    # for every RstToPdf client object.  This allows this module
    # to usefully cache, while avoiding being the cause of a memory
    # leak in a long-running process.

    source_filecache = WeakKeyDictionary()

    @classmethod
    def available(self):
        return True

    def __init__(self, filename, width=None, height=None, kind='direct',
                                 mask=None, lazy=True, srcinfo=None):
        client, uri = srcinfo
        cache = self.source_filecache.setdefault(client, {})
        pdffname = cache.get(filename)
        if pdffname is None:
            tmpf, pdffname = tempfile.mkstemp(suffix='.pdf')
            os.close(tmpf)
            client.to_unlink.append(pdffname)
            cache[filename] = pdffname
            cmd = [progname, os.path.abspath(filename), '-A', pdffname]
            try:
                subprocess.call(cmd)
            except OSError, e:
                log.error("Failed to run command: %s", ' '.join(cmd))
                raise
            self.load_xobj((client, pdffname))

        pdfuri = uri.replace(filename, pdffname)
        pdfsrc = client, pdfuri
        VectorPdf.__init__(self, pdfuri, width, height, kind, mask, lazy, pdfsrc)

    @classmethod
    def raster(self, filename, client):
        """Returns a URI to a rasterized version of the image"""
        cache = self.source_filecache.setdefault(client, {})
        pngfname = cache.get(filename+'_raster')
        if pngfname is None:
            tmpf, pngfname = tempfile.mkstemp(suffix='.png')
            os.close(tmpf)
            client.to_unlink.append(pngfname)
            cache[filename+'_raster'] = pngfname
            cmd = [progname, os.path.abspath(filename), '-e', pngfname, '-d', str(client.def_dpi)]
            try:
                subprocess.call(cmd)
                return pngfname
            except OSError, e:
                log.error("Failed to run command: %s", ' '.join(cmd))
                raise
        return None


def install(createpdf, options):
    ''' Monkey-patch our class in to image as a replacement class for SVGImage.
    '''
    rst2pdf.image.SVGImage = InkscapeImage
