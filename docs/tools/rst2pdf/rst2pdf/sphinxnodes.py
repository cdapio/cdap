# -*- coding: utf-8 -*-

#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/sphinxnodes.py $
#$Date: 2012-02-28 21:07:21 -0300 (Tue, 28 Feb 2012) $
#$Revision: 2443 $

# See LICENSE.txt for licensing terms

'''
This module contains sphinx-specific node handlers.  An import
of this module will apparently fail if sphinx.roles hasn't been
imported.

This module creates a sphinx-specific dispatch dictionary,
which is kept separate from the regular one.

When the SphinxHandler class is instantiated, the two dictionaries
are combined into the instantiated object.
'''

from copy import copy

from log import nodeid, log
from flowables import  MySpacer, MyIndenter, Reference, DelayedTable, Table
from image import MyImage, VectorPdf

from opt_imports import Paragraph, sphinx

from nodehandlers import NodeHandler, FontHandler, HandleEmphasis
import math_flowable
from reportlab.platypus import Paragraph, TableStyle
import sphinx
import docutils

################## NodeHandler subclasses ###################

class SphinxHandler(NodeHandler):
    sphinxmode = True
    dispatchdict = {}

    def __init__(self):
        ''' This is where the magic happens.  Make a copy of the elements
            in the non-sphinx dispatch dictionary, setting sphinxmode on
            every element, and then overwrite that dictionary with any
            sphinx-specific handlers.
        '''
        mydict = {}
        for key, value in self._baseclass.dispatchdict.iteritems():
            value = copy(value)
            value.sphinxmode = True
            mydict[key] = value
        mydict.update(self.dispatchdict)
        self.dispatchdict = mydict


class SphinxFont(SphinxHandler, FontHandler):
    pass

class HandleSphinxDefaults(SphinxHandler, sphinx.addnodes.glossary,
                                        sphinx.addnodes.start_of_file,
                                        sphinx.addnodes.compact_paragraph,
                                        sphinx.addnodes.pending_xref):
    pass

class SphinxListHandler(SphinxHandler):
    def get_text(self, client, node, replaceEnt):
        t = client.gather_pdftext(node)
        while t and t[0] in ', ':
            t=t[1:]
        return t

class HandleSphinxDescAddname(SphinxFont,  sphinx.addnodes.desc_addname):
    fontstyle = "descclassname"

class HandleSphinxDescName(SphinxFont, sphinx.addnodes.desc_name):
    fontstyle = "descname"

class HandleSphinxDescReturn(SphinxFont, sphinx.addnodes.desc_returns):
    def get_font_prefix(self, client, node, replaceEnt):
        return ' &rarr; ' + client.styleToFont("returns")

class HandleSphinxDescType(SphinxFont, sphinx.addnodes.desc_type):
    fontstyle = "desctype"

class HandleSphinxDescParamList(SphinxListHandler, sphinx.addnodes.desc_parameterlist):
    pre=' ('
    post=')'

class HandleSphinxDescParam(SphinxFont, sphinx.addnodes.desc_parameter):
    fontstyle = "descparameter"
    def get_pre_post(self, client, node, replaceEnt):
        pre, post = FontHandler.get_pre_post(self, client, node, replaceEnt)
        if node.hasattr('noemph'):
            pre = ', ' + pre
        else:
            pre = ', <i>' + pre
            post += '</i>'
        return pre, post

class HandleSphinxDescOpt(SphinxListHandler, SphinxFont, sphinx.addnodes.desc_optional):
    fontstyle = "optional"
    def get_pre_post(self, client, node, replaceEnt):
        prepost = FontHandler.get_pre_post(self, client, node, replaceEnt)
        return '%s[%s, ' % prepost, '%s]%s' % prepost

class HandleDescAnnotation(SphinxHandler, HandleEmphasis, sphinx.addnodes.desc_annotation):
    pass

class HandleSphinxIndex(SphinxHandler, sphinx.addnodes.index):
    def gather_elements(self, client, node, style):
        try:
            for entry in node['entries']:
                client.pending_targets.append(docutils.nodes.make_id(entry[2]))
        except IndexError:
            if node['entries']:
                log.error("Can't process index entry: %s [%s]",
                    node['entries'], nodeid(node))
        return []

if sphinx.__version__ < '1.0':
    class HandleSphinxModule(SphinxHandler, sphinx.addnodes.module):
        def gather_elements(self, client, node, style):
            return [Reference('module-'+node['modname'])]

# custom SPHINX nodes.
# FIXME: make sure they are all here, and keep them all together

class HandleSphinxCentered(SphinxHandler, sphinx.addnodes.centered):
    def gather_elements(self, client, node, style):
        return [Paragraph(client.gather_pdftext(node),
                client.styles['centered'])]

class HandleSphinxDesc(SphinxHandler, sphinx.addnodes.desc):
    def gather_elements(self, client, node, style):
        st=client.styles[node['desctype']]
        if st==client.styles['normal']:
            st=copy(client.styles['desc'])
            st.spaceBefore=0
        pre=[MySpacer(0,client.styles['desc'].spaceBefore)]
        return pre + client.gather_elements(node, st)

class HandleSphinxDescSignature(SphinxHandler, sphinx.addnodes.desc_signature):
    def gather_elements(self, client, node, style):
        # Need to add ids as targets, found this when using one of the
        # django docs extensions
        targets=[i.replace(' ','') for i in node['ids']]
        pre=''
        for i in targets:
            if i not in client.targets:
                pre+='<a name="%s" />'% i
                client.targets.append(i)
        return [Paragraph(pre+client.gather_pdftext(node),style)]

class HandleSphinxDescContent(SphinxHandler, sphinx.addnodes.desc_content):
    def gather_elements(self, client, node, style):
        return [MyIndenter(left=10)] +\
                client.gather_elements(node, client.styles["definition"]) +\
                [MyIndenter(left=-10)]

class HandleHList(SphinxHandler, sphinx.addnodes.hlist):
    def gather_elements(self, client, node, style):
        # Each child is a hlistcol and represents a column.
        # Each grandchild is a bullet list that's the contents
        # of the column

        # Represent it as a N-column, 1-row table, each cell containing
        # a list.

        cells = [[ client.gather_elements(child, style) for child in node.children]]
        t_style=TableStyle(client.styles['hlist'].commands)
        cw=100./len(node.children)
        return [ DelayedTable( cells,
            colWidths=["%s%%"%cw,]*len(cells),
            style=t_style
            )]

from sphinx.ext import mathbase

class HandleHighlightLang(SphinxHandler, sphinx.addnodes.highlightlang):
    pass

class HandleSphinxMath(SphinxHandler, mathbase.math, mathbase.displaymath):
    def gather_elements(self, client, node, style):
        mflow=math_flowable.Math(node.get('latex',''),node.get('label',None))
        n=node['number']
        if n is not None:
            number='(%s)'%node['number']
            return [Table([[mflow,number]],)]
        return [mflow]

    def get_text(self, client, node, replaceEnt):
        mf = math_flowable.Math(node.get('latex',''))
        w, h = mf.wrap(0, 0)
        descent = mf.descent()
        img = mf.genImage()
        client.to_unlink.append(img)
        return '<img src="%s" width=%f height=%f valign=%f/>' % (
            img, w, h, -descent)

class HandleSphinxEq(SphinxHandler, mathbase.eqref):

    def get_text(self, client, node, replaceEnt):
        return '<a href="equation-%s" color="%s">%s</a>'%(node['target'],
            client.styles.linkColor, node.astext())

graphviz_warn = False

try:
    x=sphinx.ext.graphviz.graphviz
    class HandleSphinxGraphviz(SphinxHandler, sphinx.ext.graphviz.graphviz):
        def gather_elements(self, client, node, style):
            # Based on the graphviz extension
            global graphviz_warn
            try:
                # Is vectorpdf enabled?
                if hasattr(VectorPdf,'load_xobj'):
                    # Yes, we have vectorpdf
                    fname, outfn = sphinx.ext.graphviz.render_dot(node['builder'], node['code'], node['options'], 'pdf')
                else:
                    # Use bitmap
                    if not graphviz_warn:
                        log.warning('Using graphviz with PNG output. You get much better results if you enable the vectorpdf extension.')
                        graphviz_warn = True
                    fname, outfn = sphinx.ext.graphviz.render_dot(node['builder'], node['code'], node['options'], 'png')
                if outfn:
                    client.to_unlink.append(outfn)
                    client.to_unlink.append(outfn+'.map')
                else:
                    # Something went very wrong with graphviz, and
                    # sphinx should have given an error already
                    return []
            except sphinx.ext.graphviz.GraphvizError, exc:
                log.error('dot code %r: ' % node['code'] + str(exc))
                return [Paragraph(node['code'],client.styles['code'])]
            return [MyImage(filename=outfn, client=client)]
except AttributeError:
    # Probably the graphviz extension is not enabled
    pass



sphinxhandlers = SphinxHandler()
