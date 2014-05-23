# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms

import tempfile
import os
import re

from reportlab.platypus import *
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics

from opt_imports import mathtext


from log import log

HAS_MATPLOTLIB = mathtext is not None

if HAS_MATPLOTLIB:
    from matplotlib.font_manager import FontProperties
    from matplotlib.colors import ColorConverter
fonts = {}

def enclose(s):
    """Enclose the string in $...$ if needed"""
    if not re.match(r'.*\$.+\$.*', s, re.MULTILINE | re.DOTALL):
        s = u"$%s$" % s
    return s

class Math(Flowable):

    def __init__(self, s, label=None, fontsize=12,color='black'):
        self.s = s
        self.label = label
        self.fontsize = fontsize
        self.color = color
        if HAS_MATPLOTLIB:
            self.parser = mathtext.MathTextParser("Pdf")
        else:
            log.error("Math support not available,"
                " some parts of this document will be rendered incorrectly."
                " Install matplotlib.")
        Flowable.__init__(self)
        self.hAlign='CENTER'

    def wrap(self, aW, aH):
        if HAS_MATPLOTLIB:
            try:
                width, height, descent, glyphs, \
                rects, used_characters = self.parser.parse(
                    enclose(self.s), 72, prop=FontProperties(size=self.fontsize))
                return width, height
            except:
                pass
                # FIXME: report error
        return 10, 10

    def drawOn(self, canv, x, y, _sW=0):
        if _sW and hasattr(self,'hAlign'):
            from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
            a = self.hAlign
            if a in ('CENTER','CENTRE', TA_CENTER):
                x = x + 0.5*_sW
            elif a in ('RIGHT',TA_RIGHT):
                x = x + _sW
            elif a not in ('LEFT',TA_LEFT):
                raise ValueError, "Bad hAlign value "+str(a)
        height = 0
        if HAS_MATPLOTLIB:
            global fonts
            canv.saveState()
            canv.translate(x, y)
            try:
                width, height, descent, glyphs, \
                rects, used_characters = self.parser.parse(
                    enclose(self.s), 72, prop=FontProperties(size=self.fontsize))
                for ox, oy, fontname, fontsize, num, symbol_name in glyphs:
                    if not fontname in fonts:
                        fonts[fontname] = fontname
                        pdfmetrics.registerFont(TTFont(fontname, fontname))
                    canv.setFont(fontname, fontsize)
                    col_conv=ColorConverter()
                    rgb_color=col_conv.to_rgb(self.color)
                    canv.setFillColorRGB(rgb_color[0],rgb_color[1],rgb_color[2])
                    canv.drawString(ox, oy, unichr(num))

                canv.setLineWidth(0)
                canv.setDash([])
                for ox, oy, width, height in rects:
                    canv.rect(ox, oy+2*height, width, height, fill=1)
            except:
                # FIXME: report error
                col_conv=ColorConverter()
                rgb_color=col_conv.to_rgb(self.color)
                canv.setFillColorRGB(rgb_color[0],rgb_color[1],rgb_color[2])
                canv.drawString(0,0,self.s)
            canv.restoreState()
        else:
            canv.saveState()
            canv.drawString(x, y, self.s)
            canv.restoreState()
        if self.label:
            log.info('Drawing equation-%s'%self.label)
            canv.bookmarkHorizontal('equation-%s'%self.label,0,height)

    def descent(self):
        """Return the descent of this flowable,
        useful to align it when used inline."""
        if HAS_MATPLOTLIB:
            width, height, descent, glyphs, rects, used_characters = \
            self.parser.parse(enclose(self.s), 72, prop=FontProperties(size=self.fontsize))
            return descent
        return 0

    def genImage(self):
        """Create a PNG from the contents of this flowable.

        Required so we can put inline math in paragraphs.
        Returns the file name.
        The file is caller's responsability.

        """

        dpi = 72
        scale = 10

        try:
            import Image
            import ImageFont
            import ImageDraw
            import ImageColor
        except ImportError:
            from PIL import (
                Image,
                ImageFont,
                ImageDraw,
                ImageColor,
            )

        if not HAS_MATPLOTLIB:
            img = Image.new('RGBA', (120, 120), (255,255,255,0))
        else:
            width, height, descent, glyphs,\
            rects, used_characters = self.parser.parse(
                enclose(self.s), dpi, prop=FontProperties(size=self.fontsize))
            img = Image.new('RGBA', (int(width*scale), int(height*scale)),(255,255,255,0))
            draw = ImageDraw.Draw(img)
            for ox, oy, fontname, fontsize, num, symbol_name in glyphs:
                font = ImageFont.truetype(fontname, int(fontsize*scale))
                tw, th = draw.textsize(unichr(num), font=font)
                # No, I don't understand why that 4 is there.
                # As we used to say in the pure math
                # department, that was a numerical solution.
                col_conv=ColorConverter()
                fc=col_conv.to_rgb(self.color)
                rgb_color=(int(fc[0]*255),int(fc[1]*255),int(fc[2]*255))
                draw.text((ox*scale, (height - oy - fontsize + 4)*scale),
                           unichr(num), font=font,fill=rgb_color)
            for ox, oy, w, h in rects:
                x1 = ox*scale
                x2 = x1 + w*scale
                y1 = (height - oy)*scale
                y2 = y1 + h*scale
                draw.rectangle([x1, y1, x2, y2],(0,0,0))

        fh, fn = tempfile.mkstemp(suffix=".png")
        os.close(fh)
        img.save(fn)
        return fn


if __name__ == "__main__":
    doc = SimpleDocTemplate("mathtest.pdf")
    Story = [Math(r'\mathcal{R}\prod_{i=\alpha\mathcal{B}}'\
                  r'^\infty a_i\sin(2 \pi f x_i)')]
    doc.build(Story)
