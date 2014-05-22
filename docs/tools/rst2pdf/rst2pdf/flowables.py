# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/flowables.py $
#$Date: 2012-12-06 10:59:55 -0300 (Thu, 06 Dec 2012) $
#$Revision: 2540 $

__docformat__ = 'reStructuredText'

from copy import copy

from reportlab.platypus import *
from reportlab.platypus.doctemplate import *
from reportlab.lib.enums import *

from opt_imports import Paragraph, NullDraw

from reportlab.lib.units import *
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus.flowables import _listWrapOn, _FUZZ
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.styles import ParagraphStyle

import styles
from log import log

import re
from xml.sax.saxutils import unescape, escape

class XXPreformatted(XPreformatted):
    """An extended XPreformattedFit"""
    def __init__(self, *args, **kwargs):
        XPreformatted.__init__(self, *args, **kwargs)

    def split (self, aW, aH):

        # Figure out a nice range of splits
        #
        # Assume we would prefer 5 lines (at least) on
        # a splitted flowable before a break, and 4 on
        # the last flowable after a break.
        # So, the minimum wrap height for a fragment
        # will be 5*leading

        rW, rH = self.wrap(aW, aH)
        if rH > aH:

            minH1=getattr(self.style, 'allowOrphans', 5)*self.style.leading
            minH2=getattr(self.style, 'allowWidows', 4)*self.style.leading

            # If there's no way to fid a decent fragment,
            # refuse to split
            if aH < minH1:
                return []

            # Now, don't split too close to the end either
            pw, ph = self.wrap(aW, aH)
            if ph - aH < minH2:
                aH = ph - minH2

        return XPreformatted.split(self, aW, aH)

class MyIndenter(Indenter):
    """An indenter that has a width, because otherwise you get crashes
    if added inside tables"""

    width = 0
    height = 0

    def draw(self):
        pass

class TocEntry(NullDraw):
    """A flowable that adds a TOC entry but draws nothing"""
    def __init__(self,level,label):
        self.level=level
        self.label=label
        self.width=0
        self.height=0
        self.keepWithNext=True

    def draw(self):
        # Add outline entry
        self.canv.bookmarkHorizontal(self.label,0,0+self.height)
        self.canv.addOutlineEntry(self.label,
                                  self.label,
                                  max(0,int(self.level)), False)

class Heading(Paragraph):
    """A paragraph that also adds an outline entry in
    the PDF TOC."""

    def __init__(self, text, style, bulletText=None, caseSensitive=1, level=0,
                 snum=None, parent_id=None, node=None, section_header_depth=2):
        # Issue 114: need to convert "&amp;" to "&" and such.
        # Issue 140: need to make it plain text
        self.stext=re.sub(r'<[^>]*?>', '', unescape(text))
        self.stext = self.stext.strip()
        self.level = int(level)
        self.snum = snum
        self.parent_id=parent_id
        self.node=node
        self.section_header_depth = section_header_depth
        Paragraph.__init__(self, text, style, bulletText)

    def draw(self):

        # Add outline entry
        self.canv.bookmarkHorizontal(self.parent_id,0,0+self.height)
        # self.section_header_depth is for Issue 391
        if self.canv.firstSect and self.level < self.section_header_depth:
            self.canv.sectName = self.stext
            self.canv.firstSect=False
            if self.snum is not None:
                self.canv.sectNum = self.snum
            else:
                self.canv.sectNum = ""

        self.canv.addOutlineEntry(self.stext.encode('utf-8','replace'),
                                  self.parent_id.encode('utf-8','replace'),
                                  int(self.level), False)
        Paragraph.draw(self)

class Separation(Flowable):
    """A simple <hr>-like flowable"""

    def wrap(self, w, h):
        self.w = w
        return w, 1*cm

    def draw(self):
        self.canv.line(0, 0.5*cm, self.w, 0.5*cm)


class Reference(Flowable):
    """A flowable to insert an anchor without taking space"""

    def __init__(self, refid):
        self.refid = refid
        self.keepWithNext=True
        Flowable.__init__(self)

    def wrap(self, w, h):
        """This takes no space"""
        return 0, 0

    def draw(self):
        self.canv.bookmarkPage(self.refid)

    def repr(self):
        return "Reference: %s" % self.refid

    def __str__(self):
        return "Reference: %s" % self.refid

class OddEven(Flowable):
    """This flowable takes two lists of flowables as arguments, odd and even.
    If will draw the "odd" list when drawn in odd pages and the "even" list on
    even pages.


    wrap() will always return a size large enough for both lists, and this flowable
    **cannot** be split, so use with care.
    """

    def __init__(self, odd, even, style=None):
        self.odd=DelayedTable([[odd]],['100%'], style)
        self.even=DelayedTable([[even]],['100%'], style)

    def wrap(self, w, h):
        """Return a box large enough for both odd and even"""
        w1,h1=self.odd.wrap(w,h)
        w2,h2=self.even.wrap(w,h)
        return max(w1,w2), max (h1,h2)

    def drawOn(self, canvas, x, y, _sW=0):
        if canvas._pagenum %2 == 0:
            self.even.drawOn(canvas, x, y, _sW)
        else:
            self.odd.drawOn(canvas, x, y, _sW)

    def split(self):
        """Makes no sense to split this..."""
        return []

class DelayedTable(Table):
    """A flowable that inserts a table for which it has the data.

    Needed so column widths can be determined after we know on what frame
    the table will be inserted, thus making the overal table width correct.

    """

    def __init__(self, data, colWidths, style=None, repeatrows=False, splitByRow=True):
        self.data = data
        self._colWidths = colWidths
        if style is None:
            style = TableStyle([
                ('LEFTPADDING', (0,0), (-1,-1), 0),
                ('RIGHTPADDING', (0,0), (-1,-1), 0),
                ('TOPPADDING', (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 0),
                ])
        self.style = style
        self.t = None
        self.repeatrows = repeatrows
        self.hAlign = TA_CENTER
        self.splitByRow=splitByRow

        ## Try to look more like a Table
        #self._ncols = 2
        #self._nosplitCmds= []
        #self._nrows= 1
        #self._rowHeights= [None]
        #self._spanCmds= []
        #self.ident= None
        #self.repeatCols= 0
        #self.repeatRows= 0
        #self.splitByRow= 1
        #self.vAlign= 'MIDDLE'

    def wrap(self, w, h):
        # Create the table, with the widths from colWidths reinterpreted
        # if needed as percentages of frame/cell/whatever width w is.

        #_tw = w/sum(self.colWidths)
        def adjust(*args, **kwargs):
            kwargs['total']=w
            return styles.adjustUnits(*args, **kwargs)
        #adjust=functools.partial(styles.adjustUnits, total=w)
        self.colWidths=map(adjust, self._colWidths)
        #colWidths = [_w * _tw for _w in self.colWidths]
        self.t = Table(self.data, colWidths=self.colWidths,
            style=self.style, repeatRows=self.repeatrows,
            splitByRow=True)
            #splitByRow=self.splitByRow)
        self.t.hAlign = self.hAlign
        return self.t.wrap(w, h)

    def split(self, w, h):
        if self.splitByRow:
            if not self.t:
                self.wrap(w,h)
            return self.t.split(w, h)
        else:
            return []

    def drawOn(self, canvas, x, y, _sW=0):
        self.t.drawOn(canvas, x, y, _sW)

    def identity(self, maxLen=None):
        return "<%s at %s%s%s> containing: %s" % (self.__class__.__name__,
            hex(id(self)), self._frameName(),
            getattr(self, 'name', '')
                and (' name="%s"' % getattr(self, 'name', '')) or '',
                unicode(self.data[0])[:180])

def tablepadding(padding):
    if not isinstance(padding,(list,tuple)):
        padding=[padding,]*4
    return padding, ('TOPPADDING',[0,0],[-1,-1],padding[0]),\
                    ('RIGHTPADDING',[-1,0],[-1,-1],padding[1]),\
                    ('BOTTOMPADDING',[0,0],[-1,-1],padding[2]),\
                    ('LEFTPADDING',[1,0],[1,-1],padding[3])

class SplitTable(DelayedTable):
    def __init__(self, data, colWidths, style, padding=3):
        if len(data) <>1 or len(data[0]) <>2:
            log.error('SplitTable can only be 1 row and two columns!')
            sys.exit(1)
        DelayedTable.__init__(self,data,colWidths,style)
        self.padding, p1, p2, p3, p4=tablepadding(padding)
        self.style._cmds.insert(0,p1)
        self.style._cmds.insert(0,p2)
        self.style._cmds.insert(0,p3)
        self.style._cmds.insert(0,p4)

    def identity(self, maxLen=None):
        return "<%s at %s%s%s> containing: %s" % (self.__class__.__name__,
            hex(id(self)), self._frameName(),
            getattr(self, 'name', '')
                and (' name="%s"' % getattr(self, 'name', '')) or '',
                unicode(self.data[0][1])[:180])

    def split(self,w,h):
        _w,_h=self.wrap(w, h)

        if _h > h: # Can't split!
            # The right column data mandates the split
            # Find which flowable exceeds the available height

            dw=self.colWidths[0]+self.padding[1]+self.padding[3]
            dh=self.padding[0]+self.padding[2]

            bullet=self.data[0][0]
            text=self.data[0][1]
            for l in range(0,len(text)):
                _,fh = _listWrapOn(text[:l+1],w-dw,None)
                if fh+dh > h:
                    # The lth flowable is the guilty one
                    # split it

                    _,lh=_listWrapOn(text[:l],w-dw,None)
                    # Workaround for Issue 180
                    text[l].wrap(w-dw,h-lh-dh)
                    l2=text[l].split(w-dw,h-lh-dh)
                    if l2==[]: # Not splittable, push some to next page
                        if l==0: # Can't fit anything, push all to next page
                            return l2

                        # We reduce the number of items we keep on the
                        # page for two reasons:
                        #    1) If an item is associated with the following
                        #       item (getKeepWithNext() == True), we have
                        #       to back up to a previous one.
                        #    2) If we miscalculated the size required on
                        #       the first page (I dunno why, probably not
                        #       counting borders properly, but we do
                        #       miscalculate occasionally).  Seems to
                        #       have to do with nested tables, so it might
                        #       be the extra space on the border on the
                        #       inner table.

                        while l > 0:
                            if not text[l-1].getKeepWithNext():
                                first_t = Table([
                                                [bullet,
                                                text[:l]]
                                            ],
                                            colWidths=self.colWidths,
                                            style=self.style)
                                _w,_h = first_t.wrap(w, h)
                                if _h <= h:
                                    break
                            l -= 1

                        if l>0:
                            # Workaround for Issue 180 with wordaxe:
                            #if wordaxe is not None:
                                #l3=[Table([
                                            #[bullet,
                                             #text[:l]]
                                           #],
                                        #colWidths=self.colWidths,
                                        #style=self.style),
                                        #Table([['',text[l:]]],
                                        #colWidths=self.colWidths,
                                        #style=self.style)]
                            #else:
                            l3=[first_t,
                                    SplitTable([['',text[l:]]],
                                    colWidths=self.colWidths,
                                    style=self.style,
                                    padding=self.padding)]
                        else: # Everything flows
                            l3=[]
                    else:
                        l3=[Table([[bullet,text[:l]+[l2[0]]]],
                                colWidths=self.colWidths,
                                rowHeights=[h],
                                style=self.style)]
                        if l2[1:]+text[l+1:]:
                            ## Workaround for Issue 180 with wordaxe:
                            #if wordaxe is not None:
                                #l3.append(
                                    #Table([['',l2[1:]+text[l+1:]]],
                                    #colWidths=self.colWidths,
                                    #style=self.style))
                            #else:
                            l3.append(
                                SplitTable([['',l2[1:]+text[l+1:]]],
                                colWidths=self.colWidths,
                                style=self.style,
                                padding=self.padding))
                    return l3
            log.debug("Can't split splittable")
            return self.t.split(w, h)
        else:
            return DelayedTable.split(self,w,h)


class MySpacer(Spacer):
    def wrap (self, aW, aH):
        w, h = Spacer.wrap(self, aW, aH)
        self.height = min(aH, h)
        return w, self.height



class MyPageBreak(FrameActionFlowable):

    def __init__(self, templateName=None, breakTo='any'):
        '''templateName switches the page template starting in the
        next page.

        breakTo can be 'any' 'even' or 'odd'.

        'even' will break one page if the current page is odd
        or two pages if it's even. That way the next flowable
        will be in an even page.

        'odd' is the opposite of 'even'

        'any' is the default, and means it will always break
        only one page.

        '''

        self.templateName = templateName
        self.breakTo=breakTo
        self.forced=False
        self.extraContent=[]

    def frameAction(self, frame):
        frame._generated_content = []
        if self.breakTo=='any': # Break only once. None if at top of page
            if not frame._atTop:
                frame._generated_content.append(SetNextTemplate(self.templateName))
                frame._generated_content.append(PageBreak())
        elif self.breakTo=='odd': #Break once if on even page, twice
                                  #on odd page, none if on top of odd page
            if frame._pagenum % 2: #odd pageNum
                if not frame._atTop:
                    # Blank pages get no heading or footer
                    frame._generated_content.append(SetNextTemplate(self.templateName))
                    frame._generated_content.append(SetNextTemplate('emptyPage'))
                    frame._generated_content.append(PageBreak())
                    frame._generated_content.append(ResetNextTemplate())
                    frame._generated_content.append(PageBreak())
            else: #even
                frame._generated_content.append(SetNextTemplate(self.templateName))
                frame._generated_content.append(PageBreak())
        elif self.breakTo=='even': #Break once if on odd page, twice
                                   #on even page, none if on top of even page
            if frame._pagenum % 2: #odd pageNum
                frame._generated_content.append(SetNextTemplate(self.templateName))
                frame._generated_content.append(PageBreak())
            else: #even
                if not frame._atTop:
                    # Blank pages get no heading or footer
                    frame._generated_content.append(SetNextTemplate(self.templateName))
                    frame._generated_content.append(SetNextTemplate('emptyPage'))
                    frame._generated_content.append(PageBreak())
                    frame._generated_content.append(ResetNextTemplate())
                    frame._generated_content.append(PageBreak())

class SetNextTemplate(Flowable):
    """Set canv.templateName when drawing.

    rst2pdf uses that to switch page templates.

    """

    def __init__(self, templateName=None):
        self.templateName = templateName
        Flowable.__init__(self)

    def draw(self):
        if self.templateName:
            try:
                self.canv.oldTemplateName = self.canv.templateName
            except:
                self.canv.oldTemplateName = 'oneColumn'
            self.canv.templateName = self.templateName

class ResetNextTemplate(Flowable):
    """Go back to the previous template.

    rst2pdf uses that to switch page templates back when
    temporarily it needed to switch to another template.

    For example, after a OddPageBreak, there can be a totally
    blank page. Those have to use coverPage as a template,
    because they must not have headers or footers.

    And then we need to switch back to whatever was used.

    """

    def __init__(self):
        Flowable.__init__(self)

    def draw(self):
        self.canv.templateName, self.canv.oldTemplateName = \
            self.canv.oldTemplateName, self.canv.templateName

    def wrap(self, aW, aH):
        return 0,0

class Transition(Flowable):
    """Wrap canvas.setPageTransition.

    Sets the transition effect from the current page to the next.

    """

    PageTransitionEffects = dict(
        Split=['direction', 'motion'],
        Blinds=['dimension'],
        Box=['motion'],
        Wipe=['direction'],
        Dissolve=[],
        Glitter=['direction'])

    def __init__(self, *args):
        if len(args) < 1:
            args = [None, 1] # No transition
        # See if we got a valid transition effect name
        if args[0] not in self.PageTransitionEffects:
            log.error('Unknown transition effect name: %s' % args[0])
            args[0] = None
        elif len(args) == 1:
            args.append(1)

        # FIXME: validate more
        self.args = args

    def wrap(self, aw, ah):
        return 0, 0

    def draw(self):
        kwargs = dict(
            effectname=None,
            duration=1,
            direction=0,
            dimension='H',
            motion='I')
        ceff = ['effectname', 'duration'] +\
            self.PageTransitionEffects[self.args[0]]
        for argname, argvalue in zip(ceff, self.args):
            kwargs[argname] = argvalue
        kwargs['duration'] = int(kwargs['duration'])
        kwargs['direction'] = int(kwargs['direction'])
        self.canv.setPageTransition(**kwargs)


class SmartFrame(Frame):
    """A (Hopefully) smarter frame object.

    This frame object knows how to handle a two-pass
    layout procedure (someday).

    """

    def __init__(self, container, x1, y1, width, height,
            leftPadding=6, bottomPadding=6, rightPadding=6, topPadding=6,
            id=None, showBoundary=0, overlapAttachedSpace=None, _debug=None):
        self.container = container
        self.onSidebar = False
        self.__s = '[%s, %s, %s, %s, %s, %s, %s, %s,]'\
            %(x1,y1,width,height,
              leftPadding, bottomPadding,
              rightPadding, topPadding)
        Frame.__init__(self, x1, y1, width, height,
            leftPadding, bottomPadding, rightPadding, topPadding,
            id, showBoundary, overlapAttachedSpace, _debug)

    def add (self, flowable, canv, trySplit=0):
        flowable._atTop=self._atTop
        return Frame.add(self, flowable, canv, trySplit)

    def __repr__(self):
        return self.__s

    def __deepcopy__(self, *whatever):
        return copy(self)

class FrameCutter(FrameActionFlowable):

    def __init__(self, dx, width, flowable, padding, lpad, floatLeft=True):
        self.width = width
        self.dx = dx
        self.f = flowable
        self.padding = padding
        self.lpad = lpad
        self.floatLeft = floatLeft

    def frameAction(self, frame):
        idx = frame.container.frames.index(frame)
        if self.floatLeft:
            # Don't bother inserting a silly thin frame
            if self.width-self.padding > 30:
                f1 = SmartFrame(frame.container,
                    frame._x1 + self.dx - 2*self.padding,
                    frame._y2 - self.f.height - 3*self.padding,
                    self.width + 2*self.padding,
                    self.f.height + 3*self.padding,
                    bottomPadding=0, topPadding=0, leftPadding=self.lpad)
                f1._atTop = frame._atTop
                # This is a frame next to a sidebar.
                f1.onSidebar = True
                frame.container.frames.insert(idx + 1, f1)
            # Don't add silly thin frame
            if frame._height-self.f.height - 2*self.padding > 30:
                frame.container.frames.insert(idx + 2,
                    SmartFrame(frame.container,
                        frame._x1,
                        frame._y1p,
                        self.width + self.dx,
                        frame._height - self.f.height - 3*self.padding,
                        topPadding=0))
        else:
            # Don't bother inserting a silly thin frame
            if self.width-self.padding > 30:
                f1 = SmartFrame(frame.container,
                    frame._x1 - self.width,
                    frame._y2 - self.f.height - 2*self.padding,
                    self.width,
                    self.f.height + 2*self.padding,
                    bottomPadding=0, topPadding=0, rightPadding=self.lpad)
                f1._atTop = frame._atTop
                # This is a frame next to a sidebar.
                f1.onSidebar = True
                frame.container.frames.insert(idx + 1, f1)
            if frame._height - self.f.height - 2*self.padding > 30:
                frame.container.frames.insert(idx + 2,
                    SmartFrame(frame.container,
                        frame._x1 - self.width,
                        frame._y1p,
                        self.width + self.dx,
                        frame._height - self.f.height - 2*self.padding,
                        topPadding=0))


class Sidebar(FrameActionFlowable):

    def __init__(self, flowables, style):
        self.style = style
        self.width = self.style.width
        self.flowables = flowables

    def frameAction(self, frame):
        if self.style.float not in ('left', 'right'):
            return
        if frame.onSidebar: # We are still on the frame next to a sidebar!
            frame._generated_content = [FrameBreak(), self]
        else:
            w = frame.container.styles.adjustUnits(self.width, frame.width)
            idx = frame.container.frames.index(frame)
            padding = self.style.borderPadding
            width = self.style.width
            self.style.padding = frame.container.styles.adjustUnits(
                str(padding), frame.width)
            self.style.width = frame.container.styles.adjustUnits(
                str(width), frame.width)
            self.kif = BoxedContainer(self.flowables, self.style)
            if self.style.float == 'left':
                self.style.lpad = frame.leftPadding
                f1 = SmartFrame(frame.container,
                    frame._x1,
                    frame._y1p,
                    w - 2*self.style.padding,
                    frame._y - frame._y1p,
                    leftPadding=self.style.lpad, rightPadding=0,
                    bottomPadding=0, topPadding=0)
                f1._atTop = frame._atTop
                frame.container.frames.insert(idx+1, f1)
                frame._generated_content = [
                    FrameBreak(),
                    self.kif,
                    FrameCutter(w,
                        frame.width - w,
                        self.kif,
                        padding,
                        self.style.lpad,
                        True),
                    FrameBreak()]
            elif self.style.float == 'right':
                self.style.lpad = frame.rightPadding
                frame.container.frames.insert(idx + 1,
                    SmartFrame(frame.container,
                        frame._x1 + frame.width - self.style.width,
                        frame._y1p,
                        w, frame._y-frame._y1p,
                        rightPadding=self.style.lpad, leftPadding=0,
                        bottomPadding=0, topPadding=0))
                frame._generated_content = [
                    FrameBreak(),
                    self.kif,
                    FrameCutter(w,
                        frame.width - w,
                        self.kif,
                        padding,
                        self.style.lpad,
                        False),
                    FrameBreak()]


class BoundByWidth(Flowable):
    """Limit a list of flowables by width.

    This still lets the flowables break over pages and frames.

    """

    def __init__(self, maxWidth, content=[], style=None, mode=None, scale = None):
        self.maxWidth = maxWidth
        self.content = content
        self.style = style
        self.mode = mode
        self.pad = None
        self.scale = scale
        Flowable.__init__(self)

    def border_padding(self, useWidth, additional):
        sdict = self.style
        sdict = sdict.__dict__ or {}
        bp = sdict.get("borderPadding", 0)
        if useWidth:
            additional += sdict.get("borderWidth", 0)
        if not isinstance(bp, list):
            bp = [bp] * 4
        return [x + additional for x in bp]

    def identity(self, maxLen=None):
        return "<%s at %s%s%s> containing: %s" % (self.__class__.__name__,
            hex(id(self)), self._frameName(),
            getattr(self, 'name', '')
                and (' name="%s"' % getattr(self, 'name', '')) or '',
                unicode([c.identity() for c in self.content])[:80])

    def wrap(self, availWidth, availHeight):
        """If we need more width than we have, complain, keep a scale"""
        self.pad = self.border_padding(True, 0.1)
        maxWidth = float(min(
            styles.adjustUnits(self.maxWidth, availWidth) or availWidth,
                               availWidth))
        self.maxWidth = maxWidth
        maxWidth -= (self.pad[1]+self.pad[3])
        self.width, self.height = _listWrapOn(self.content, maxWidth, None)
        if self.width > maxWidth:
            if self.mode <> 'shrink':
                self.scale = 1.0
                log.warning("BoundByWidth too wide to fit in frame (%s > %s): %s",
                    self.width,maxWidth,self.identity())
            if self.mode == 'shrink' and not self.scale:
                self.scale = (maxWidth + self.pad[1]+self.pad[3])/\
                    (self.width + self.pad[1]+self.pad[3])
        else:
            self.scale = 1.0
        self.height *= self.scale
        self.width *= self.scale
        return self.width, self.height + (self.pad[0]+self.pad[2])*self.scale

    def split(self, availWidth, availHeight):
        if not self.pad:
            self.wrap(availWidth, availHeight)
        content = self.content
        if len(self.content) == 1:
            # We need to split the only element we have
            content = content[0].split(
                availWidth - (self.pad[1]+self.pad[3]),
                availHeight - (self.pad[0]+self.pad[2]))
        result =  [BoundByWidth(self.maxWidth, [f],
                             self.style, self.mode, self.scale) for f in content]
        return result

    def draw(self):
        """we simulate being added to a frame"""
        canv = self.canv
        canv.saveState()
        x = canv._x
        y = canv._y
        _sW = 0
        scale = self.scale
        content = None
        #, canv, x, y, _sW=0, scale=1.0, content=None, aW=None):
        pS = 0
        aW = self.width
        aW = scale*(aW + _sW)
        if content is None:
            content = self.content
        y += (self.height + self.pad[2])/scale
        x += self.pad[3]
        for c in content:
            w, h = c.wrapOn(canv, aW, 0xfffffff)
            if (w < _FUZZ or h < _FUZZ) and not getattr(c, '_ZEROSIZE', None):
                continue
            if c is not content[0]:
                h += max(c.getSpaceBefore() - pS, 0)
            y -= h
            canv.saveState()
            if self.mode == 'shrink':
                canv.scale(scale, scale)
            elif self.mode == 'truncate':
                p = canv.beginPath()
                p.rect(x-self.pad[3],
                       y-self.pad[2],
                       self.maxWidth,
                       self.height + self.pad[0]+self.pad[2])
                canv.clipPath(p, stroke=0)
            c.drawOn(canv, x, y, _sW=aW - w)
            canv.restoreState()
            if c is not content[-1]:
                pS = c.getSpaceAfter()
                y -= pS
        canv.restoreState()


class BoxedContainer(BoundByWidth):

    def __init__(self, content, style, mode='shrink'):
        try:
            w=style.width
        except AttributeError:
            w='100%'
        BoundByWidth.__init__(self, w, content, mode=mode, style=None)
        self.style = style
        self.mode = mode

    def identity(self, maxLen=None):
        return unicode([u"BoxedContainer containing: ",
            [c.identity() for c in self.content]])[:80]

    def draw(self):
        canv = self.canv
        canv.saveState()
        x = canv._x
        y = canv._y
        _sW = 0
        lw = 0
        if self.style and self.style.borderWidth > 0:
            lw = self.style.borderWidth
            canv.setLineWidth(self.style.borderWidth)
            if self.style.borderColor: # This could be None :-(
                canv.setStrokeColor(self.style.borderColor)
                stroke=1
            else:
                stroke=0
        else:
            stroke=0
        if self.style and self.style.backColor:
            canv.setFillColor(self.style.backColor)
            fill=1
        else:
            fill=0


        padding = self.border_padding(False, lw)
        xpadding = padding[1] + padding[3]
        ypadding = padding[0] + padding[2]
        p = canv.beginPath()
        p.rect(x, y, self.width + xpadding, self.height + ypadding)
        canv.drawPath(p, stroke=stroke, fill=fill)
        canv.restoreState()
        BoundByWidth.draw(self)

    def split(self, availWidth, availHeight):
        self.wrap(availWidth, availHeight)
        padding = (self.pad[1]+self.pad[3])*self.scale
        if self.height + padding <= availHeight:
            return [self]
        else:
            # Try to figure out how many elements
            # we can put in the available space
            candidate = None
            remainder = None
            for p in range(1, len(self.content)):
                b = BoxedContainer(self.content[:p], self.style, self.mode)
                w, h = b.wrap(availWidth, availHeight)
                if h < availHeight:
                    candidate = b
                    if self.content[p:]:
                        remainder = BoxedContainer(self.content[p:],
                                                   self.style,
                                                   self.mode)
                else:
                    break
            if not candidate or not remainder: # Nothing fits, break page
                return []
            if not remainder: # Everything fits?
                return [self]
            return [candidate, remainder]


if reportlab.Version == '2.1':
    import reportlab.platypus.paragraph as pla_para

    ################Ugly stuff below
    def _do_post_text(i, t_off, tx):
        """From reportlab's paragraph.py, patched to avoid underlined links"""
        xs = tx.XtraState
        leading = xs.style.leading
        ff = 0.125*xs.f.fontSize
        y0 = xs.cur_y - i*leading
        y = y0 - ff
        ulc = None
        for x1, x2, c in xs.underlines:
            if c != ulc:
                tx._canvas.setStrokeColor(c)
                ulc = c
            tx._canvas.line(t_off + x1, y, t_off + x2, y)
        xs.underlines = []
        xs.underline = 0
        xs.underlineColor = None

        ys = y0 + 2*ff
        ulc = None
        for x1, x2, c in xs.strikes:
            if c != ulc:
                tx._canvas.setStrokeColor(c)
                ulc = c
            tx._canvas.line(t_off + x1, ys, t_off + x2, ys)
        xs.strikes = []
        xs.strike = 0
        xs.strikeColor = None

        yl = y + leading
        for x1, x2, link in xs.links:
            # This is the bad line
            # tx._canvas.line(t_off+x1, y, t_off+x2, y)
            _doLink(tx, link, (t_off + x1, y, t_off + x2, yl))
        xs.links = []
        xs.link = None

    # Look behind you! A three-headed monkey!
    pla_para._do_post_text.func_code = _do_post_text.func_code
    ############### End of the ugly

class MyTableOfContents(TableOfContents):
    """
    Subclass of reportlab.platypus.tableofcontents.TableOfContents
    which supports hyperlinks to corresponding sections.
    """

    def __init__(self, *args, **kwargs):

        # The parent argument is to define the locality of
        # the TOC. If it's none, it's a global TOC and
        # any heading it's notified about is accepted.

        # If it's a node, then the heading needs to be "inside"
        # that node. This can be figured out because
        # the heading flowable keeps a reference to the title
        # node it was creatd from.
        #
        # Yes, this is gross.

        self.parent=kwargs.pop('parent')
        TableOfContents.__init__(self, *args, **kwargs)
        # reference ids for which this TOC should be notified
        self.refids = []
        # revese lookup table from (level, text) to refid
        self.refid_lut = {}
        self.linkColor = "#0000ff"

    def notify(self, kind, stuff):
        # stuff includes (level, text, pagenum, label)
        level, text, pageNum, label, node = stuff
        rlabel='-'.join(label.split('-')[:-1])

        def islocal(_node):
            '''See if this node is "local enough" for this TOC.
            This is for Issue 196'''
            if self.parent is None:
                return True
            while _node.parent:
                if _node.parent == self.parent:
                    return True
                _node=_node.parent
            return False

        if rlabel in self.refids and islocal(node):
            self.addEntry(level, text, pageNum)
            self.refid_lut[(level, text, pageNum)] = label

    def wrap(self, availWidth, availHeight):
        """Adds hyperlink to toc entry."""

        widths = (availWidth - self.rightColumnWidth,
                  self.rightColumnWidth)

        # makes an internal table which does all the work.
        # we draw the LAST RUN's entries!  If there are
        # none, we make some dummy data to keep the table
        # from complaining
        if len(self._lastEntries) == 0:
            if reportlab.Version <= '2.3':
                _tempEntries = [(0, 'Placeholder for table of contents', 0)]
            else:
                _tempEntries = [(0, 'Placeholder for table of contents',
                                 0, None)]
        else:
            _tempEntries = self._lastEntries

        if _tempEntries:
            base_level = _tempEntries[0][0]
        else:
            base_level = 0
        tableData = []
        for entry in _tempEntries:
            level, text, pageNum = entry[:3]
            left_col_level = level - base_level
            if reportlab.Version > '2.3': # For ReportLab post-2.3
                leftColStyle=self.getLevelStyle(left_col_level)
            else: # For ReportLab <= 2.3
                leftColStyle = self.levelStyles[left_col_level]
            label = self.refid_lut.get((level, text, pageNum), None)
            if label:
                pre = u'<a href="%s" color="%s">' % (label, self.linkColor)
                post = u'</a>'
                if not isinstance(text, unicode):
                    text = unicode(text, 'utf-8')
                text = pre + text + post
            else:
                pre = ''
                post = ''
            #right col style is right aligned
            rightColStyle = ParagraphStyle(name='leftColLevel%d' % left_col_level,
                parent=leftColStyle, leftIndent=0, alignment=TA_RIGHT)
            leftPara = Paragraph(text, leftColStyle)
            rightPara = Paragraph(pre+str(pageNum)+post, rightColStyle)
            tableData.append([leftPara, rightPara])

        self._table = Table(tableData, colWidths=widths, style=self.tableStyle)

        self.width, self.height = self._table.wrapOn(self.canv, availWidth, availHeight)
        return self.width, self.height

    def split(self, aW, aH):
        # Make sure _table exists before splitting.
        # This was only triggered in rare cases using sphinx.
        if not self._table:
            self.wrap(aW,aH)
        return TableOfContents.split(self, aW, aH)

    def isSatisfied(self):
        if self._entries == self._lastEntries:
            log.debug('Table Of Contents is stable')
            return True
        else:
            if len(self._entries) != len(self._lastEntries):
                log.info('Number of items in TOC changed '\
                'from %d to %d, not satisfied'%\
                (len(self._lastEntries),len(self._entries)))
                return False

            log.info('TOC entries that moved in this pass:')
            for i in xrange(len(self._entries)):
                if self._entries[i] != self._lastEntries[i]:
                    log.info(str(self._entries[i]))
                    log.info(str(self._lastEntries[i]))

        return False

