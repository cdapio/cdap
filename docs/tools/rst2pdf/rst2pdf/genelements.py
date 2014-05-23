# -*- coding: utf-8 -*-

#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/genelements.py $
#$Date: 2012-12-13 21:39:59 -0300 (Thu, 13 Dec 2012) $
#$Revision: 2615 $

# See LICENSE.txt for licensing terms

# Some fragments of code are copied from Reportlab under this license:
#
#####################################################################################
#
#       Copyright (c) 2000-2008, ReportLab Inc.
#       All rights reserved.
#
#       Redistribution and use in source and binary forms, with or without modification,
#       are permitted provided that the following conditions are met:
#
#               *       Redistributions of source code must retain the above copyright notice,
#                       this list of conditions and the following disclaimer.
#               *       Redistributions in binary form must reproduce the above copyright notice,
#                       this list of conditions and the following disclaimer in the documentation
#                       and/or other materials provided with the distribution.
#               *       Neither the name of the company nor the names of its contributors may be
#                       used to endorse or promote products derived from this software without
#                       specific prior written permission.
#
#       THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#       ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#       WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
#       IN NO EVENT SHALL THE OFFICERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
#       INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
#       TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
#       OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
#       IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
#       IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#       SUCH DAMAGE.
#
#####################################################################################


import os
import tempfile
import re
from copy import copy

from basenodehandler import NodeHandler

import docutils.nodes
from oddeven_directive import OddEvenNode
import reportlab

from aafigure_directive import Aanode

from log import log, nodeid
from utils import log, parseRaw, parseHTML
from reportlab.platypus import Paragraph, TableStyle
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from flowables import Table, DelayedTable, SplitTable, Heading, \
              MyIndenter, MyTableOfContents, MySpacer, \
              Separation, BoxedContainer, BoundByWidth, \
              MyPageBreak, Reference, tablepadding, OddEven, \
              XPreformatted

from opt_imports import wordaxe, Paragraph, ParagraphStyle


class TocBuilderVisitor(docutils.nodes.SparseNodeVisitor):

    def __init__(self, document):
        docutils.nodes.SparseNodeVisitor.__init__(self, document)
        self.toc = None
        # For some reason, when called via sphinx,
        # .. contents:: ends up trying to call
        # visitor.document.reporter.debug
        # so we need a valid document here.
        self.document=docutils.utils.new_document('')

    def visit_reference(self, node):
        refid = node.attributes.get('refid')
        if refid:
            self.toc.refids.append(refid)


class HandleDocument(NodeHandler, docutils.nodes.document):
    pass

class HandleTable(NodeHandler, docutils.nodes.table):
    def gather_elements(self, client, node, style):
        if node['classes']:
            style = client.styles.combinedStyle(['table']+node['classes'])
        else:
            style = client.styles['table']
        return [MySpacer(0, client.styles['table'].spaceBefore)] + \
                    client.gather_elements(node, style=style) +\
                    [MySpacer(0, client.styles['table'].spaceAfter)]

class HandleTGroup(NodeHandler, docutils.nodes.tgroup):
    def gather_elements(self, client, node, style):

        # Take the style from the parent "table" node
        # because sometimes it's not passed down.

        if node.parent['classes']:
            style = client.styles.combinedStyle(['table']+node.parent['classes'])
        else:
            style = client.styles['table']
        rows = []
        colWidths = []
        hasHead = False
        headRows = 0
        for n in node.children:
            if isinstance(n, docutils.nodes.thead):
                hasHead = True
                for row in n.children:
                    r = []
                    for cell in row.children:
                        r.append(cell)
                    rows.append(r)
                headRows = len(rows)
            elif isinstance(n, docutils.nodes.tbody):
                for row in n.children:
                    r = []
                    for cell in row.children:
                        r.append(cell)
                    rows.append(r)
            elif isinstance(n, docutils.nodes.colspec):
                colWidths.append(int(n['colwidth']))

        # colWidths are in no specific unit, really. Maybe ems.
        # Convert them to %
        colWidths=map(int, colWidths)
        tot=sum(colWidths)
        colWidths=["%s%%"%((100.*w)/tot) for w in colWidths]

        if 'colWidths' in style.__dict__:
            colWidths[:len(style.colWidths)]=style.colWidths

        spans = client.filltable(rows)

        data = []
        cellStyles = []
        rowids = range(0, len(rows))
        for row, i in zip(rows, rowids):
            r = []
            j = 0
            for cell in row:
                if isinstance(cell, str):
                    r.append("")
                else:
                    if i < headRows:
                        st = client.styles['table-heading']
                    else:
                        st = client.styles['table-body']
                    ell = client.gather_elements(cell, style=st)
                    r.append(ell)
                j += 1
            data.append(r)

        st = TableStyle(spans)
        if 'commands' in style.__dict__:
            for cmd in style.commands:
                st.add(*cmd)
        else:
            # Only use the commands from "table" if the
            # specified class has no commands.

            for cmd in client.styles['table'].commands:
                st.add(*cmd)

        if hasHead:
            for cmd in client.styles.tstyleHead(headRows):
                st.add(*cmd)
        rtr = client.repeat_table_rows

        t=DelayedTable(data, colWidths, st, rtr)
        if style.alignment == TA_LEFT:
            t.hAlign='LEFT'
        elif style.alignment == TA_CENTER:
            t.hAlign='CENTER'
        elif style.alignment == TA_RIGHT:
            t.hAlign='RIGHT'
        return [t]

class HandleParagraph(NodeHandler, docutils.nodes.paragraph):
    def gather_elements(self, client, node, style):
        return [Paragraph(client.gen_pdftext(node), style)]

    def get_pre_post(self, client, node, replaceEnt):
        pre=''
        targets=set(node.get('ids',[])+client.pending_targets)
        client.pending_targets=[]
        for _id in targets:
            if _id not in client.targets:
                pre+='<a name="%s"/>'%(_id)
                client.targets.append(_id)
        return pre, '\n'


class HandleTitle(HandleParagraph, docutils.nodes.title):
    def gather_elements(self, client, node, style):
        # Special cases: (Not sure this is right ;-)
        if isinstance(node.parent, docutils.nodes.document):
            #node.elements = [Paragraph(client.gen_pdftext(node),
                                        #client.styles['title'])]
            # The visible output is now done by the cover template
            node.elements = []
            client.doc_title = node.rawsource
            client.doc_title_clean = node.astext().strip()
        elif isinstance(node.parent, docutils.nodes.topic):
            node.elements = [Paragraph(client.gen_pdftext(node),
                                        client.styles['topic-title'])]
        elif isinstance(node.parent, docutils.nodes.Admonition):
            node.elements = [Paragraph(client.gen_pdftext(node),
                                        client.styles['admonition-title'])]
        elif isinstance(node.parent, docutils.nodes.table):
            node.elements = [Paragraph(client.gen_pdftext(node),
                                        client.styles['table-title'])]
        elif isinstance(node.parent, docutils.nodes.sidebar):
            node.elements = [Paragraph(client.gen_pdftext(node),
                                        client.styles['sidebar-title'])]
        else:
            # Section/Subsection/etc.
            text = client.gen_pdftext(node)
            fch = node.children[0]
            if isinstance(fch, docutils.nodes.generated) and \
                fch['classes'] == ['sectnum']:
                snum = fch.astext()
            else:
                snum = None
            key = node.get('refid')
            maxdepth=4
            if reportlab.Version > '2.1':
                maxdepth=6

            # The parent ID is the refid + an ID to make it unique for Sphinx
            parent_id=(node.parent.get('ids', [None]) or [None])[0]+u'-'+unicode(id(node))
            node.elements = [ Heading(text,
                    client.styles['heading%d'%min(client.depth, maxdepth)],
                    level=client.depth-1,
                    parent_id=parent_id,
                    node=node,
                    section_header_depth=client.section_header_depth
                    )]
            if client.depth <= client.breaklevel:
                node.elements.insert(0, MyPageBreak(breakTo=client.breakside))
        return node.elements

class HandleSubTitle(HandleParagraph, docutils.nodes.subtitle):
    def gather_elements(self, client, node, style):
        if isinstance(node.parent, docutils.nodes.sidebar):
            elements = [Paragraph(client.gen_pdftext(node),
                client.styles['sidebar-subtitle'])]
        elif isinstance(node.parent, docutils.nodes.document):
            #elements = [Paragraph(client.gen_pdftext(node),
                #client.styles['subtitle'])]
            # The visible output is now done by the cover template
            elements = []
            # FIXME: looks like subtitles don't have a rawsource like
            # titles do.
            # That means that literals and italics etc in subtitles won't
            # work.
            client.doc_subtitle = getattr(node,'rawtext',node.astext()).strip()
        else:
            elements = node.elements  # FIXME Can we get here???
        return elements

class HandleDocInfo(NodeHandler, docutils.nodes.docinfo):
    # A docinfo usually contains several fields.
    # We'll render it as a series of elements, one field each.
    pass

class HandleField(NodeHandler, docutils.nodes.field):
    def gather_elements(self, client, node, style):
        # A field has two child elements, a field_name and a field_body.
        # We render as a two-column table, left-column is right-aligned,
        # bold, and much smaller

        fn = Paragraph(client.gather_pdftext(node.children[0]) + ":",
            style=client.styles['fieldname'])
        fb = client.gen_elements(node.children[1],
                style=client.styles['fieldvalue'])
        t_style=TableStyle(client.styles['field-list'].commands)
        return [DelayedTable([[fn, fb]], style=t_style,
            colWidths=client.styles['field-list'].colWidths)]

class HandleDecoration(NodeHandler, docutils.nodes.decoration):
    pass

class HandleHeader(NodeHandler, docutils.nodes.header):
    stylename = 'header'
    def gather_elements(self, client, node, style):
        client.decoration[self.stylename] = client.gather_elements(node,
            style=client.styles[self.stylename])
        return []

class HandleFooter(HandleHeader, docutils.nodes.footer):
    stylename = 'footer'

class HandleAuthor(NodeHandler, docutils.nodes.author):
    def gather_elements(self, client, node, style):
        if isinstance(node.parent, docutils.nodes.authors):
            # Is only one of multiple authors. Return a paragraph
            node.elements = [Paragraph(client.gather_pdftext(node),
                style=style)]
            if client.doc_author:
                client.doc_author += client.author_separator(style=style) \
                    + node.astext().strip()
            else:
                client.doc_author = node.astext().strip()
        else:
            # A single author: works like a field
            fb = client.gather_pdftext(node)

            t_style=TableStyle(client.styles['field-list'].commands)
            colWidths=map(client.styles.adjustUnits,
                client.styles['field-list'].colWidths)

            node.elements = [Table(
                [[Paragraph(client.text_for_label("author", style)+":",
                    style=client.styles['fieldname']),
                    Paragraph(fb, style)]],
                style=t_style, colWidths=colWidths)]
            client.doc_author = node.astext().strip()
        return node.elements

class HandleAuthors(NodeHandler, docutils.nodes.authors):
    def gather_elements(self, client, node, style):
        # Multiple authors. Create a two-column table.
        # Author references on the right.
        t_style=TableStyle(client.styles['field-list'].commands)
        colWidths = client.styles['field-list'].colWidths

        td = [[Paragraph(client.text_for_label("authors", style)+":",
                    style=client.styles['fieldname']),
                client.gather_elements(node, style=style)]]
        return [DelayedTable(td, style=t_style,
            colWidths=colWidths)]

class HandleFList(NodeHandler):
    adjustwidths = False
    TableType = DelayedTable
    def gather_elements(self, client, node, style):
        fb = client.gather_pdftext(node)
        t_style=TableStyle(client.styles['field-list'].commands)
        colWidths=client.styles['field-list'].colWidths
        if self.adjustwidths:
            colWidths = map(client.styles.adjustUnits, colWidths)
        label=client.text_for_label(self.labeltext, style)+":"
        t = self.TableType([[Paragraph(label, style=client.styles['fieldname']),
                    Paragraph(fb, style)]],
                    style=t_style, colWidths=colWidths)
        return [t]

class HandleOrganization(HandleFList, docutils.nodes.organization):
    labeltext = "organization"

class HandleContact(HandleFList, docutils.nodes.contact):
    labeltext = "contact"

class HandleAddress(HandleFList, docutils.nodes.address):
    labeltext = "address"
    def gather_elements(self, client, node, style):
        fb = client.gather_pdftext(node)
        t_style=TableStyle(client.styles['field-list'].commands)
        colWidths=client.styles['field-list'].colWidths
        if self.adjustwidths:
            colWidths = map(client.styles.adjustUnits, colWidths)
        label=client.text_for_label(self.labeltext, style)+":"
        t = self.TableType([[Paragraph(label, style=client.styles['fieldname']),
                             XPreformatted(fb, style)]
                    ], style=t_style, colWidths=colWidths)
        return [t]

class HandleVersion(HandleFList, docutils.nodes.version):
    labeltext = "version"

class HandleRevision(HandleFList, docutils.nodes.revision):
    labeltext = "revision"
    adjustwidths = True
    TableType = Table

class HandleStatus(HandleFList, docutils.nodes.status):
    labeltext = "status"

class HandleDate(HandleFList, docutils.nodes.date):
    labeltext = "date"

class HandleCopyright(HandleFList, docutils.nodes.copyright):
    labeltext = "copyright"

class HandleTopic(NodeHandler, docutils.nodes.topic):
    def gather_elements(self, client, node, style):
        # toc
        node_classes = node.attributes.get('classes', [])
        cstyles = client.styles
        if 'contents' in node_classes:
            toc_visitor = TocBuilderVisitor(node.document)
            if 'local' in node_classes:
                toc_visitor.toc = MyTableOfContents(parent=node.parent)
            else:
                toc_visitor.toc = MyTableOfContents(parent=None)
            toc_visitor.toc.linkColor = cstyles.tocColor or cstyles.linkColor
            node.walk(toc_visitor)
            toc = toc_visitor.toc
            toc.levelStyles=[cstyles['toc%d'%l] for l in range(1,15)]
            for s in toc.levelStyles:
                # FIXME: awful slimy hack!
                s.__class__=reportlab.lib.styles.ParagraphStyle
            ## Issue 117: add extra TOC levelStyles.
            ## 9-deep should be enough.
            #for i in range(4):
                #ps = toc.levelStyles[-1].__class__(name='Level%d'%(i+5),
                        #parent=toc.levelStyles[-1],
                        #leading=toc.levelStyles[-1].leading,
                        #firstlineIndent=toc.levelStyles[-1].firstLineIndent,
                        #leftIndent=toc.levelStyles[-1].leftIndent+1*cm)
                #toc.levelStyles.append(ps)

            ## Override fontnames (defaults to Times-Roman)
            #for levelStyle in toc.levelStyles:
                #levelStyle.__dict__['fontName'] = \
                    #client.styles['tableofcontents'].fontName
            if 'local' in node_classes:
                node.elements = [toc]
            else:
                node.elements = \
                    [Paragraph(client.gen_pdftext(node.children[0]),
                    cstyles['heading1']), toc]
        else:
            node.elements = client.gather_elements(node, style=style)
        return node.elements

class HandleFieldBody(NodeHandler, docutils.nodes.field_body):
    pass

class HandleSection(NodeHandler, docutils.nodes.section):
    def gather_elements(self, client, node, style):
        #XXX: should style be passed down here?
        client.depth+=1
        elements = client.gather_elements(node)
        client.depth-=1
        return elements

class HandleBulletList(NodeHandler, docutils.nodes.bullet_list):
    def gather_elements(self, client, node, style):

        if node ['classes']:
            style = client.styles[node['classes'][0]]
        else:
            style = client.styles["bullet-list"]

        node.elements = client.gather_elements(node,
            style=style)

        # Here we need to separate the list from the previous element.
        # Calculate by how much:

        sb=style.spaceBefore # list separation
        sa=style.spaceAfter # list separation

        node.elements.insert(0, MySpacer(0, sb))
        node.elements.append(MySpacer(0, sa))
        return node.elements

class HandleDefOrOptList(NodeHandler, docutils.nodes.definition_list,
                                docutils.nodes.option_list):
    pass

class HandleFieldList(NodeHandler, docutils.nodes.field_list):
    def gather_elements(self, client, node, style):
        return [MySpacer(0,client.styles['field-list'].spaceBefore)]+\
                client.gather_elements(node, style=style)

class HandleEnumeratedList(NodeHandler, docutils.nodes.enumerated_list):
    def gather_elements(self, client, node, style):
        if node ['classes']:
            style = client.styles[node['classes'][0]]
        else:
            style = client.styles["item-list"]

        node.elements = client.gather_elements(node,
            style = style)

        # Here we need to separate the list from the previous element.
        # Calculate by how much:

        sb=style.spaceBefore # list separation
        sa=style.spaceAfter # list separation

        node.elements.insert(0, MySpacer(0, sb))
        node.elements.append(MySpacer(0, sa))
        return node.elements

class HandleDefinition(NodeHandler, docutils.nodes.definition):
    def gather_elements(self, client, node, style):
        return client.gather_elements(node,
                       style = style)

class HandleOptionListItem(NodeHandler, docutils.nodes.option_list_item):
    def gather_elements(self, client, node, style):
        optext = ', '.join([client.gather_pdftext(child)
                for child in node.children[0].children])

        desc = client.gather_elements(node.children[1], style)

        t_style = TableStyle(client.styles['option-list'].commands)
        colWidths = client.styles['option-list'].colWidths
        node.elements = [DelayedTable([[client.PreformattedFit(
            optext, client.styles["literal"]), desc]], style = t_style,
            colWidths = colWidths)]
        return node.elements

class HandleDefListItem(NodeHandler, docutils.nodes.definition_list_item):
    def gather_elements(self, client, node, style):
        # I need to catch the classifiers here
        tt = []
        dt = []
        ids = []
        for n in node.children:
            if isinstance(n, docutils.nodes.term):
                for i in n['ids']: # Used by sphinx glossary lists
                    if i not in client.targets:
                        ids.append('<a name="%s"/>' % i)
                        client.targets.append(i)
                o, c = client.styleToTags("definition-list-term")
                tt.append(o + client.gather_pdftext(n) + c)
            elif isinstance(n, docutils.nodes.classifier):
                o, c = client.styleToTags("definition-list-classifier")
                tt.append(o + client.gather_pdftext(n) + c)
            else:
                dt.extend(client.gen_elements(n, style))

        # FIXME: make this configurable from the stylesheet
        t_style = TableStyle (client.styles['definition'].commands)
        cw = getattr(client.styles['definition'],'colWidths',[])

        if client.splittables:
            node.elements = [
                Paragraph(''.join(ids)+' : '.join(tt), client.styles['definition-list-term']),
                SplitTable([['',dt]] , colWidths=cw, style = t_style )]
        else:
            node.elements = [
                Paragraph(''.join(ids)+' : '.join(tt), client.styles['definition-list-term']),
                DelayedTable([['',dt]] , colWidths=[10,None], style = t_style )]

        return node.elements

class HandleListItem(NodeHandler, docutils.nodes.list_item):
    def gather_elements(self, client, node, style):
        b, t = client.bullet_for_node(node)

        bStyle = copy(style)
        bStyle.alignment = 2

        # FIXME: use different unicode bullets depending on b
        if b and b in "*+-":
            b = getattr(bStyle, 'bulletText', u'\u2022')

        # The style has information about the bullet:
        #
        # bulletFontSize
        # bulletFont
        # This is so the baselines of the bullet and the text align
        extra_space= bStyle.bulletFontSize-bStyle.fontSize

        bStyle.fontSize=bStyle.bulletFontSize
        bStyle.fontName=bStyle.bulletFontName

        if t == 'bullet':
            item_st=client.styles['bullet-list-item']
        else:
            item_st=client.styles['item-list-item']

        el = client.gather_elements(node, item_st)
        # FIXME: this is really really not good code
        if not el:
            el = [Paragraph(u"<nobr>\xa0</nobr>", item_st)]


        idx=node.parent.children.index(node)
        if idx==0:
            # The first item in the list, so doesn't need
            # separation (it's provided by the list itself)
            sb=0
            # It also doesn't need a first-line-indent
            fli=0
        else:
            # Not the first item, so need to separate from
            # previous item. Account for space provided by
            # the item's content, too.
            sb=item_st.spaceBefore-item_st.spaceAfter
            fli=item_st.firstLineIndent

        bStyle.spaceBefore=0

        t_style = TableStyle(style.commands)
        # The -3 here is to compensate for padding, 0 doesn't work :-(
        t_style._cmds.extend([
            #["GRID", [ 0, 0 ], [ -1, -1 ], .25, "black" ],
            ["BOTTOMPADDING", [ 0, 0 ], [ -1, -1 ], -3 ]]
        )
        if extra_space >0:
            # The bullet is larger, move down the item text
            sb += extra_space
            sbb = 0
        else:
            # The bullet is smaller, move down the bullet
            sbb = -extra_space

        #colWidths = map(client.styles.adjustUnits,
            #client.styles['item_list'].colWidths)
        colWidths = getattr(style,'colWidths',[])
        while len(colWidths) < 2:
            colWidths.append(client.styles['item_list'].colWidths[len(colWidths)])

        if client.splittables:
            node.elements = [MySpacer(0,sb),
                                SplitTable([[Paragraph(b, style = bStyle), el]],
                                style = t_style,
                                colWidths = colWidths)
                                ]
        else:
            node.elements = [MySpacer(0,sb),
                                DelayedTable([[Paragraph(b, style = bStyle), el]],
                                style = t_style,
                                colWidths = colWidths)
                                ]
        return node.elements

class HandleTransition(NodeHandler, docutils.nodes.transition):
    def gather_elements(self, client, node, style):
        return [Separation()]


class HandleBlockQuote(NodeHandler, docutils.nodes.block_quote):
    def gather_elements(self, client, node, style):
        # This should work, but doesn't look good inside of
        # table cells (see Issue 173)
        #node.elements = [MyIndenter(left=client.styles['blockquote'].leftIndent)]\
            #+ client.gather_elements( node, style) + \
            #[MyIndenter(left=-client.styles['blockquote'].leftIndent)]
        # Workaround for Issue 173 using tables
        leftIndent=client.styles['blockquote'].leftIndent
        rightIndent=client.styles['blockquote'].rightIndent
        spaceBefore=client.styles['blockquote'].spaceBefore
        spaceAfter=client.styles['blockquote'].spaceAfter
        s=copy(client.styles['blockquote'])
        s.leftIndent=style.leftIndent
        data=[['',client.gather_elements( node, s)]]
        if client.splittables:
            node.elements=[MySpacer(0,spaceBefore),SplitTable(data,
                colWidths=[leftIndent,None],
                style=TableStyle([["TOPPADDING",[0,0],[-1,-1],0],
                        ["LEFTPADDING",[0,0],[-1,-1],0],
                        ["RIGHTPADDING",[0,0],[-1,-1],rightIndent],
                        ["BOTTOMPADDING",[0,0],[-1,-1],0],
                ])), MySpacer(0,spaceAfter)]
        else:
            node.elements=[MySpacer(0,spaceBefore),DelayedTable(data,
                colWidths=[leftIndent,None],
                style=TableStyle([["TOPPADDING",[0,0],[-1,-1],0],
                        ["LEFTPADDING",[0,0],[-1,-1],0],
                        ["RIGHTPADDING",[0,0],[-1,-1],rightIndent],
                        ["BOTTOMPADDING",[0,0],[-1,-1],0],
                ])), MySpacer(0,spaceAfter)]
        return node.elements

class HandleAttribution(NodeHandler, docutils.nodes.attribution):
    def gather_elements(self, client, node, style):
        return [
                Paragraph(client.gather_pdftext(node),
                          client.styles['attribution'])]

class HandleComment(NodeHandler, docutils.nodes.comment):
    def gather_elements(self, client, node, style):
        # Class that generates no output
        return []

class HandleLineBlock(NodeHandler, docutils.nodes.line_block):
    def gather_elements(self, client, node, style):
        if isinstance(node.parent,docutils.nodes.line_block):
            qstyle = copy(style)
            qstyle.leftIndent += client.styles.adjustUnits("1.5em")
        else:
            qstyle = copy(client.styles['lineblock'])
        # Fix Issue 225: no space betwen line in a lineblock, but keep
        # space before the lineblock itself
        # Fix Issue 482: nested lineblocks don't need spacing before/after
        if not isinstance(node.parent, docutils.nodes.line_block):
            return [MySpacer(0,client.styles['lineblock'].spaceBefore)]+client.gather_elements(node, style=qstyle)+[MySpacer(0,client.styles['lineblock'].spaceAfter)]
        else:
            return client.gather_elements(node, style=qstyle)

class HandleLine(NodeHandler, docutils.nodes.line):
    def gather_elements(self, client, node, style):
        # line nodes have no classes, they have to inherit from the outermost lineblock (sigh)
        # For more info see Issue 471 and its test case.
        
        parent = node
        while isinstance(parent.parent, (docutils.nodes.line, docutils.nodes.line_block)):
            parent=parent.parent
        p_class = (parent.get('classes') or  ['line'])[0]
        print node, p_class
        qstyle = copy(client.styles[p_class])
        # Indent .5em per indent unit
        i=node.__dict__.get('indent',0)
        #qstyle = copy(client.styles['line'])
        qstyle.leftIndent += client.styles.adjustUnits("0.5em")*i
        text = client.gather_pdftext(node)
        if not text: # empty line
            text=u"<nobr>\xa0</nobr>"
        return [Paragraph(text, style=qstyle)]

class HandleLiteralBlock(NodeHandler, docutils.nodes.literal_block,
                               docutils.nodes.doctest_block):
    def gather_elements(self, client, node, style):
        if node['classes']:
            style = client.styles.combinedStyle(['code']+node['classes'])
        else:
            style = client.styles['code']

        return [client.PreformattedFit(
                client.gather_pdftext(node, replaceEnt = True),
                                style )]


class HandleFigure(NodeHandler, docutils.nodes.figure):
    def gather_elements(self, client, node, style):

        # Either use the figure style or the class
        # selected by the user
        st_name = 'figure'
        if node.get('classes'):
            st_name = node.get('classes')[0]
        style=client.styles[st_name]
        cmd=getattr(style,'commands',[])
        image=node.children[0]
        if len(node.children) > 1:
            caption = node.children[1]
        else:
            caption=None

        if len(node.children) > 2:
            legend = node.children[2:]
        else:
            legend=[]

        w=node.get('width',client.styles['figure'].colWidths[0])
        cw=[w,]
        sub_elems = client.gather_elements(node, style=None)
        t_style=TableStyle(cmd)
        table = DelayedTable([[e,] for e in sub_elems],style=t_style,
            colWidths=cw)
        table.hAlign = node.get('align','CENTER').upper()
        return [MySpacer(0, style.spaceBefore),table,
            MySpacer(0, style.spaceAfter)]


class HandleCaption(NodeHandler, docutils.nodes.caption):
    def gather_elements(self, client, node, style):
        return [Paragraph(client.gather_pdftext(node),
                                style=client.styles['figure-caption'])]

class HandleLegend(NodeHandler, docutils.nodes.legend):
    def gather_elements(self, client, node, style):
        return client.gather_elements(node,
            style=client.styles['figure-legend'])

class HandleSidebar(NodeHandler, docutils.nodes.sidebar):
    def gather_elements(self, client, node, style):
        return [BoxedContainer(client.gather_elements(node, style=None),
                                  client.styles['sidebar'])]

class HandleRubric(NodeHandler, docutils.nodes.rubric):
    def gather_elements(self, client, node, style):
        # Sphinx uses a rubric as footnote container
        if self.sphinxmode and len(node.children) == 1 \
            and node.children[0].astext() == 'Footnotes':
                return []
        else:
            return [Paragraph(client.gather_pdftext(node),
                                client.styles['rubric'])]

class HandleCompound(NodeHandler, docutils.nodes.compound):
    # FIXME think if this is even implementable
    pass

class HandleContainer(NodeHandler, docutils.nodes.container):

    def getelements(self, client, node, style):
        parent = node.parent
        if not isinstance(parent, (docutils.nodes.header, docutils.nodes.footer)):
            return NodeHandler.getelements(self, client, node, style)
        return self.gather_elements(client, node, style)

class HandleSubstitutionDefinition(NodeHandler, docutils.nodes.substitution_definition):
    def gather_elements(self, client, node, style):
        return []

class HandleTBody(NodeHandler, docutils.nodes.tbody):
    def gather_elements(self, client, node, style):
        rows = [client.gen_elements(n) for n in node.children]
        t = []
        for r in rows:
            if not r:
                continue
            t.append(r)
        t_style = TableStyle(client.styles['table'].commands)
        colWidths = client.styles['table'].colWidths
        return [DelayedTable(t, style=t_style, colWidths=colWidths)]

class HandleFootnote(NodeHandler, docutils.nodes.footnote,
                                  docutils.nodes.citation):
    def gather_elements(self, client, node, style):
        # It seems a footnote contains a label and a series of elements
        ltext = client.gather_pdftext(node.children[0])
        label = None
        ids=''
        for i in node.get('ids',[]):
            ids+='<a name="%s"/>'%(i)
        client.targets.extend(node.get('ids',[ltext]))

        if len(node['backrefs']) > 1 and client.footnote_backlinks:
            backrefs = []
            i = 1
            for r in node['backrefs']:
                backrefs.append('<a href="#%s" color="%s">%d</a>' % (
                    r, client.styles.linkColor, i))
                i += 1
            backrefs = '(%s)' % ', '.join(backrefs)
            if ltext not in client.targets:
                label = Paragraph(ids+'%s'%(ltext + backrefs),
                                client.styles["endnote"])
                client.targets.append(ltext)
        elif len(node['backrefs'])==1 and client.footnote_backlinks:
            if ltext not in client.targets:
                label = Paragraph(ids+'<a href="%s" color="%s">%s</a>' % (
                                    node['backrefs'][0],
                                    client.styles.linkColor,
                                    ltext), client.styles["endnote"])
                client.targets.append(ltext)
        else:
            if ltext not in client.targets:
                label = Paragraph(ids+ltext,
                    client.styles["endnote"])
                client.targets.append(ltext)
        if not label:
            label = Paragraph(ids+ltext,
                    client.styles["endnote"])
        contents = client.gather_elements(node, client.styles["endnote"])[1:]
        if client.inline_footnotes:
            st=client.styles['endnote']
            t_style = TableStyle(st.commands)
            colWidths = client.styles['endnote'].colWidths
            node.elements = [MySpacer(0, st.spaceBefore),
                                DelayedTable([[label, contents]],
                                style=t_style, colWidths=colWidths),
                                MySpacer(0, st.spaceAfter)]
            if client.real_footnotes:
                client.mustMultiBuild = True
                for e in node.elements:
                    e.isFootnote=True
        else:
            client.decoration['endnotes'].append([label, contents])
            node.elements = []
        return node.elements

class HandleLabel(NodeHandler, docutils.nodes.label):
    def gather_elements(self, client, node, style):
        return [Paragraph(client.gather_pdftext(node), style)]

class HandleEntry(NodeHandler, docutils.nodes.entry):
    pass

class HandleRaw(NodeHandler, docutils.nodes.raw):
    def gather_elements(self, client, node, style):
        # Not really raw, but what the heck
        if node.get('format','NONE').lower()=='pdf':
            return parseRaw(str(node.astext()), node)
        elif client.raw_html and node.get('format','NONE').lower()=='html':
            x = parseHTML(str(node.astext()), node)
            return x
        else:
            return []


class HandleOddEven (NodeHandler, OddEvenNode):
    def gather_elements(self, client, node, style):
        odd=[]
        even=[]
        #from pudb import set_trace; set_trace()
        if node.children:
            if isinstance (node.children[0], docutils.nodes.paragraph):
                if node.children[0].get('classes'):
                    s = client.styles[node.children[0].get('classes')[0]]
                else:
                    s = style
                odd=[Paragraph(client.gather_pdftext(node.children[0]),
                    s)]
            else:
                # A compound element
                odd=client.gather_elements(node.children[0])
        if len(node.children)>1:
            if isinstance (node.children[1], docutils.nodes.paragraph):
                if node.children[1].get('classes'):
                    s = client.styles[node.children[1].get('classes')[0]]
                else:
                    s = style
                even=[Paragraph(client.gather_pdftext(node.children[1]),
                     s)]
            else:
                even=client.gather_elements(node.children[1])

        return [OddEven(odd=odd, even=even)]

class HandleAanode(NodeHandler, Aanode):
    def gather_elements(self, client, node, style):
        style_options = {
            'font': client.styles['aafigure'].fontName,
            }
        return [node.gen_flowable(style_options)]

class HandleAdmonition(NodeHandler, docutils.nodes.attention,
                docutils.nodes.caution, docutils.nodes.danger,
                docutils.nodes.error, docutils.nodes.hint,
                docutils.nodes.important, docutils.nodes.note,
                docutils.nodes.tip, docutils.nodes.warning,
                docutils.nodes.Admonition):

    def gather_elements(self, client, node, style):
        if node.children and isinstance(node.children[0], docutils.nodes.title):
            title=[]
        else:
            title= [Paragraph(client.text_for_label(node.tagname, style),
                style=client.styles['%s-heading'%node.tagname])]
        rows=title + client.gather_elements(node, style=style)
        st=client.styles[node.tagname]
        if 'commands' in dir(st):
            t_style = TableStyle(st.commands)
        else:
            t_style = TableStyle()
        t_style.add("ROWBACKGROUNDS", [0, 0], [-1, -1],[st.backColor])
        t_style.add("BOX", [ 0, 0 ], [ -1, -1 ], st.borderWidth , st.borderColor)

        if client.splittables:
            node.elements = [MySpacer(0,st.spaceBefore),
                                SplitTable([['',rows]],
                                style=t_style,
                                colWidths=[0,None],
                                padding=st.borderPadding),
                                MySpacer(0,st.spaceAfter)]
        else:
            padding, p1, p2, p3, p4=tablepadding(padding=st.borderPadding)
            t_style.add(*p1)
            t_style.add(*p2)
            t_style.add(*p3)
            t_style.add(*p4)
            node.elements = [MySpacer(0,st.spaceBefore),
                                DelayedTable([['',rows]],
                                style=t_style,
                                colWidths=[0,None]),
                                MySpacer(0,st.spaceAfter)]
        return node.elements
