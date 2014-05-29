#!/usr/bin/env python
# -*- coding: utf-8 -*-

from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.platypus.doctemplate import Indenter
from reportlab.platypus.flowables import *
from reportlab.platypus.tables import *
from reportlab.platypus.xpreformatted import *
from reportlab.lib.styles import getSampleStyleSheet
from copy import copy

def go():
        Story=[]
        styles = getSampleStyleSheet()
        doc = SimpleDocTemplate("issue277.pdf")
        ts=TableStyle()        
        knstyle=copy(styles['Normal'])
        heading=Paragraph('A heading at the beginning of the document',knstyle)
        heading.keepWithNext=True
        print [['This is the content'] for x in range(12)]
        content= Table([[Paragraph('This is the content',styles['Normal'])] for x in range(120)], style=ts)
        
        Story=[heading,content]
        doc.build(Story)

go()
