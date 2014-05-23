# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms

import sys
import os
from weakref import WeakKeyDictionary
from copy import copy

try:
    from reportlab.rl_config import _FUZZ
    from reportlab.platypus import Flowable
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

    import pdfrw
    from pdfrw.toreportlab import makerl
    from pdfrw.buildxobj import CacheXObj

    from rst2pdf.log import log
    import rst2pdf.image
    from rst2pdf.opt_imports import LazyImports
except ImportError:
    # This is just to make nosetest happy on the CI server
    class Flowable:
        pass

        # TODO:  Looks the same as for other images, because I
        #        stole it from other image handlers.  Common base class???


class AnyCache(object):
    ''' This is a memory leak waiting to happen.
        It is used by the raster method.  Not yet
        sure how to define scope on these cached items.
    '''

# This is monkey-patched into reportlab IFF we are using
# PDF files inside paragraphs.

def drawImage(self, image, x, y, width=None, height=None, mask=None, 
            preserveAspectRatio=False, anchor='c'):
    if not isinstance(image, VectorPdf):
        return self._drawImageNotVectorPDF(image, x, y, width, height, mask,
                  preserveAspectRatio, anchor)
    image.drawOn(self, x, y, width=width, height=height)

class VectorPdf(Flowable):

    # The filecache allows us to only read a given PDF file once
    # for every RstToPdf client object.  This allows this module
    # to usefully cache, while avoiding being the cause of a memory
    # leak in a long-running process.

    filecache = WeakKeyDictionary()

    @classmethod
    def load_xobj(cls, srcinfo):
        client, uri = srcinfo
        loader = cls.filecache.get(client)
        if loader is None:
            loader = cls.filecache[client] = CacheXObj().load
        return loader(uri)

    def __init__(self, filename, width=None, height=None, kind='direct',
                                     mask=None, lazy=True, srcinfo=None):
        Flowable.__init__(self)
        self._kind = kind
        self.xobj = xobj = self.load_xobj(srcinfo)
        self.imageWidth, self.imageHeight = imageWidth, imageHeight = xobj.w, xobj.h
        width = width or imageWidth
        height = height or imageHeight
        if kind in ['bound','proportional']:
            factor = min(float(width)/imageWidth,float(height)/imageHeight)
            width = factor * imageWidth
            height = factor * imageHeight
        self.drawWidth = width
        self.drawHeight = height

    def wrap(self, aW, aH):
        return self.drawWidth, self.drawHeight

    def drawOn(self, canv, x, y, _sW=0, width=0, height=0):
        if _sW > 0 and hasattr(self, 'hAlign'):
            a = self.hAlign
            if a in ('CENTER', 'CENTRE', TA_CENTER):
                x += 0.5*_sW
            elif a in ('RIGHT', TA_RIGHT):
                x += _sW
            elif a not in ('LEFT', TA_LEFT):
                raise ValueError("Bad hAlign value " + str(a))

        xobj = self.xobj
        xobj_name = makerl(canv._doc, xobj)

        xscale = (width or self.drawWidth) / xobj.w
        yscale = (height or self.drawHeight) / xobj.h
        x -= xobj.x * xscale
        y -= xobj.y * yscale

        canv.saveState()
        canv.translate(x, y)
        canv.scale(xscale, yscale)
        canv.doForm(xobj_name)
        canv.restoreState()

    def _restrictSize(self,aW,aH):
        if self.drawWidth>aW+_FUZZ or self.drawHeight>aH+_FUZZ:
            self._oldDrawSize = self.drawWidth, self.drawHeight
            factor = min(float(aW)/self.drawWidth,float(aH)/self.drawHeight)
            self.drawWidth *= factor
            self.drawHeight *= factor
        return self.drawWidth, self.drawHeight

    def getSize(self):
        return self.drawWidth, self.drawHeight

    @staticmethod
    def SleazyPDFCheck(fname):
        return fname.split('#',1)[0].rsplit('.',1)[1].lower() == 'pdf'

    OldImageReader = None

    @classmethod
    def NewImageReader(cls, fname):
        if cls.SleazyPDFCheck(fname):
            return cls(None, srcinfo=(AnyCache, fname))
        else:
            return cls.OldImageReader(fname)

    @classmethod
    def raster(cls, fname, client):
        '''  We don't REALLY generate a raster image.
             Instead, we attempt to monkey-patch reportlab
             to call us with the filename again later.
        '''
        if cls.OldImageReader is None:
            import reportlab.platypus.paraparser as p
            cls.OldImageReader = p.ImageReader
            p.ImageReader = cls.NewImageReader
            from reportlab.pdfgen.canvas import Canvas as c
            c._drawImageNotVectorPDF = c.drawImage
            c.drawImage = drawImage
        return fname

    def __deepcopy__(self, *whatever):
        # VectorPDF class is not deep copyable.  Stop the copy at this
        # class.  Related to issue 126, but cropped up later when
        # we added fake raster stuff for reportlab <img> tag.
        return copy(self)


def install(createpdf, options):
    ''' Monkey-patch this PDF handling into rst2pdf
    '''
    LazyImports.pdfinfo = pdfrw
    rst2pdf.image.VectorPdf = VectorPdf
