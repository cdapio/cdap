# -*- coding: utf-8 -*-

#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/genpdftext.py $
#$Date: 2012-12-14 13:41:35 -0300 (Fri, 14 Dec 2012) $
#$Revision: 2621 $

# See LICENSE.txt for licensing terms

import os
from xml.sax.saxutils import escape
from log import log, nodeid
from basenodehandler import NodeHandler
import docutils.nodes
from urlparse import urljoin, urlparse
from reportlab.lib.units import cm
from opt_imports import Paragraph

from image import MyImage, missing
from flowables import MySpacer

class FontHandler(NodeHandler):
    def get_pre_post(self, client, node, replaceEnt):
        return self.get_font_prefix(client, node, replaceEnt), '</font>'

    def get_font_prefix(self, client, node, replaceEnt):
        return client.styleToFont(self.fontstyle)

class HandleText(NodeHandler, docutils.nodes.Text):
    def gather_elements(self, client, node, style):
        return [Paragraph(client.gather_pdftext(node), style)]

    def get_text(self, client, node, replaceEnt):
        text = node.astext()
        if replaceEnt:
            text = escape(text)
        return text

class HandleStrong(NodeHandler, docutils.nodes.strong):
    pre = "<b>"
    post = "</b>"

class HandleEmphasis(NodeHandler, docutils.nodes.emphasis):
    pre = "<i>"
    post = "</i>"

class HandleLiteral(NodeHandler, docutils.nodes.literal):
    def get_pre_post(self, client, node, replaceEnt):
        
        if node['classes']:
            pre = client.styleToFont(node['classes'][0])
        else:
            pre = client.styleToFont('literal')
        post = "</font>"
        if not client.styles['literal'].hyphenation:
            pre = '<nobr>' + pre
            post += '</nobr>'
        return pre, post

    def get_text(self, client, node, replaceEnt):
        text = node.astext()
        text = escape(node.astext())
        text = text.replace(' ', '&nbsp;')
        return text
        
class HandleSuper(NodeHandler, docutils.nodes.superscript):
    pre = '<super>'
    post = "</super>"

class HandleSub(NodeHandler, docutils.nodes.subscript):
    pre = '<sub>'
    post = "</sub>"

class HandleTitleReference(FontHandler, docutils.nodes.title_reference):
    fontstyle = 'title_reference'

class HandleReference(NodeHandler, docutils.nodes.reference):
    def get_pre_post(self, client, node, replaceEnt):
        pre, post = '', ''
        uri = node.get('refuri')
        if uri:
            # Issue 366: links to "#" make no sense in a PDF
            if uri =="#":
                return "", ""
            if uri.startswith ('#'):
                pass
            elif client.baseurl: # Need to join the uri with the base url
                uri = urljoin(client.baseurl, uri)

            if urlparse(uri)[0] and client.inlinelinks:
                # external inline reference
                if uri in [node.astext(),"mailto:"+node.astext()]:
                    # No point on repeating it
                    post = u''
                elif uri.startswith('http://') or uri.startswith('ftp://'):
                    post = u' (%s)' % uri
                elif uri.startswith('mailto:'):
                    #No point on showing "mailto:"
                    post = u' (%s)' % uri[7:]
            else:
                # A plain old link
                pre += u'<a href="%s" color="%s">' %\
                    (uri, client.styles.linkColor)
                post = '</a>' + post
        else:
            uri = node.get('refid')
            if uri:
                pre += u'<a href="#%s" color="%s">' %\
                    (uri, client.styles.linkColor)
                post = '</a>' + post
        return pre, post

class HandleOptions(HandleText, docutils.nodes.option_string, docutils.nodes.option_argument):
    pass

class HandleSysMessage(HandleText, docutils.nodes.system_message, docutils.nodes.problematic):
    pre = '<font color="red">'
    post = "</font>"

    def gather_elements(self, client, node, style):
        # FIXME show the error in the document, red, whatever
        # log.warning("Problematic node %s", node.astext())
        return []


class HandleGenerated(HandleText, docutils.nodes.generated):
    pass
#    def get_text(self, client, node, replaceEnt):
#        if 'sectnum' in node['classes']:
#            # This is the child of a title with a section number
#            # Send the section number up to the title node
#            node.parent['_sectnum'] = node.astext()
#        return node.astext()

class HandleImage(NodeHandler, docutils.nodes.image):
    def gather_elements(self, client, node, style):
        # FIXME: handle alt

        target = None
        if isinstance(node.parent, docutils.nodes.reference):
            target = node.parent.get('refuri', None)
        st_name = 'image'
        if node.get('classes'):
            st_name = node.get('classes')[0]
        style=client.styles[st_name]
        uri = str(node.get("uri"))
        if uri.split("://")[0].lower() not in ('http','ftp','https'):
            imgname = os.path.join(client.basedir,uri)
        else:
            imgname = uri
        try:
            w, h, kind = MyImage.size_for_node(node, client=client)
        except ValueError:
            # Broken image, return arbitrary stuff
            imgname=missing
            w, h, kind = 100, 100, 'direct'
        node.elements = [
            MyImage(filename=imgname, height=h, width=w,
                    kind=kind, client=client, target=target)]
        alignment = node.get('align', '').upper()
        if not alignment:
            # There is no JUSTIFY for flowables, of course, so 4:LEFT
            alignment = {0:'LEFT', 1:'CENTER', 2:'RIGHT', 4:'LEFT'}[style.alignment]
        if not alignment:
            alignment = 'CENTER'
        node.elements[0].image.hAlign = alignment
        node.elements[0].spaceBefore = style.spaceBefore
        node.elements[0].spaceAfter = style.spaceAfter
        
        # Image flowables don't support valign (makes no sense for them?)
        # elif alignment in ('TOP','MIDDLE','BOTTOM'):
        #    i.vAlign = alignment
        return node.elements

    def get_text(self, client, node, replaceEnt):
        # First see if the image file exists, or else,
        # use image-missing.png
        imgname = os.path.join(client.basedir,str(node.get("uri")))
        try:
            w, h, kind = MyImage.size_for_node(node, client=client)
        except ValueError:
            # Broken image, return arbitrary stuff
            imgname=missing
            w, h, kind = 100, 100, 'direct'

        alignment=node.get('align', 'CENTER').lower()
        if alignment in ('top', 'middle', 'bottom'):
            align='valign="%s"'%alignment
        else:
            align=''
        # TODO: inline images don't support SVG, vectors and PDF,
        #       which may be surprising. So, work on converting them
        #       previous to passing to reportlab.
        # Try to rasterize using the backend
        w, h, kind = MyImage.size_for_node(node, client=client)
        uri=MyImage.raster(imgname, client)
        return '<img src="%s" width="%f" height="%f" %s/>'%\
            (uri, w, h, align)

class HandleFootRef(NodeHandler, docutils.nodes.footnote_reference,docutils.nodes.citation_reference):
    def get_text(self, client, node, replaceEnt):
        # TODO: when used in Sphinx, all footnotes are autonumbered
        anchors=''
        for i in node.get('ids'):
            if i not in client.targets:
                anchors+='<a name="%s"/>' % i
                client.targets.append(i)
        return u'%s<super><a href="%s" color="%s">%s</a></super>'%\
            (anchors, '#' + node.get('refid',node.astext()),
                client.styles.linkColor, node.astext())

class HandleTarget(NodeHandler, docutils.nodes.target):
    def gather_elements(self, client, node, style):
        if 'refid' in node:
            client.pending_targets.append(node['refid'])
        return client.gather_elements(node, style)

    def get_text(self, client, node, replaceEnt):
        text = client.gather_pdftext(node)
        if replaceEnt:
            text = escape(text)
        return text

    def get_pre_post(self, client, node, replaceEnt):
        pre = ''
        if node['ids'][0] not in client.targets:
            pre = u'<a name="%s"/>' % node['ids'][0]
            client.targets.append(node['ids'][0])
        return pre, ''

class HandleInline(NodeHandler, docutils.nodes.inline):
    def get_pre_post(self, client, node, replaceEnt):
        r = client.styleToTags(node['classes'][0])
        if r:
            return r
        return '', ''
