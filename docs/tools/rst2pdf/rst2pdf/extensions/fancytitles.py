# -*- coding: utf-8 -*-
import rst2pdf.genelements as genelements
from rst2pdf.flowables import Heading, MyPageBreak
from rst2pdf.image import MyImage
import docutils
from rst2pdf.opt_imports import Paragraph
import reportlab
import tempfile
import re
from xml.sax.saxutils import unescape
import codecs

class FancyTitleHandler(genelements.HandleParagraph, docutils.nodes.title):
    '''
    This class will handle title nodes.

    It takes a "titletemplate.svg", replaces TITLEGOESHERE with
    the actual title text, and draws that using the FancyHeading flowable
    (see below).

    Since this class is defined in an extension, it
    effectively replaces rst2pdf.genelements.HandleTitle.
    '''
    
    def gather_elements(self, client, node, style):
        # This method is copied from the HandleTitle class
        # in rst2pdf.genelements.
        
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
            if client.depth > 1:
                node.elements = [ Heading(text,
                        client.styles['heading%d'%min(client.depth, maxdepth)],
                        level=client.depth-1,
                        parent_id=parent_id,
                        node=node,
                        )]
            else: # This is an important title, do our magic ;-)
                # Hack the title template SVG
                tfile = codecs.open('titletemplate.svg','r','utf-8')
                tdata = tfile.read()
                tfile.close()
                tfile = tempfile.NamedTemporaryFile(dir='.', delete=False, suffix='.svg')
                tfname = tfile.name
                tfile.write(tdata.replace('TITLEGOESHERE', text).encode('utf-8'))
                tfile.close()

                # Now tfname contains a SVG with the right title.
                # Make rst2pdf delete it later.
                client.to_unlink.append(tfname)
                
                e = FancyHeading(tfname,
                    width=700,
                    height=100,
                    client=client,
                    snum=snum,
                    parent_id=parent_id,
                    text=text,
                    hstyle=client.styles['heading%d'%min(client.depth, maxdepth)])
                
                node.elements = [e]
                
            if client.depth <= client.breaklevel:
                node.elements.insert(0, MyPageBreak(breakTo=client.breakside))
        return node.elements

class FancyHeading(MyImage, Heading):
    '''This is a cross between the Heading flowable, that adds outline
    entries so you have a PDF TOC, and MyImage, that draws images'''

    def __init__(self, *args, **kwargs):
        # The inicialization is taken from rst2pdf.flowables.Heading
        hstyle = kwargs.pop('hstyle')
        level = 0
        text = kwargs.pop('text')
        self.snum = kwargs.pop('snum')
        self.parent_id= kwargs.pop('parent_id')
        #self.stext = 
        Heading.__init__(self,text,hstyle,level=level,
            parent_id=self.parent_id)
        # Cleanup title text
        #self.stext = re.sub(r'<[^>]*?>', '', unescape(self.stext))
        #self.stext = self.stext.strip()

        # Stuff needed for the outline entry
        MyImage.__init__(self, *args, **kwargs)

    def drawOn(self,canv,x,y,_sW):

        ## These two lines are magic.
        #if isinstance(self.parent_id, tuple):
            #self.parent_id=self.parent_id[0]
        
        # Add outline entry. This is copied from rst2pdf.flowables.heading
        canv.bookmarkHorizontal(self.parent_id,0,y+self.image.height)

        if canv.firstSect:
            canv.sectName = self.stext
            canv.firstSect=False
            if self.snum is not None:
                canv.sectNum = self.snum
            else:
                canv.sectNum = ""

        canv.addOutlineEntry(self.stext.encode('utf-8','replace'),
                                  self.parent_id.encode('utf-8','replace'),
                                  int(self.level), False)

        # And let MyImage do all the drawing
        MyImage.drawOn(self,canv,x,y,_sW)
