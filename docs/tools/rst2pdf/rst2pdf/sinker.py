# -*- coding: utf-8 -*-

from reportlab.platypus.flowables import _listWrapOn, _FUZZ, Flowable


class Sinker(Flowable):
    '''A flowable that always takes the rest of the frame.
    It then draws its contents (a list of sub-flowables)
    at the bottom of that space'''

    def __init__(self, content):
        self.content = content

    def wrap (self, aW, aH):
        self.width, self.height = _listWrapOn(self.content, aW, None)
        return self.width, aH

    def draw (self):
        canv = self.canv
        canv.saveState()
        x = canv._x
        y = canv._y
        y += self.height
        aW = self.width
        for c in self.content:
            w, h = c.wrapOn(canv, aW, 0xfffffff)
            if (w < _FUZZ or h < _FUZZ) and not getattr(c, '_ZEROSIZE', None):
                continue
            y -= h
            canv.saveState()
            c.drawOn(canv, x, y, _sW=aW - w)
            canv.restoreState()
        canv.restoreState()
