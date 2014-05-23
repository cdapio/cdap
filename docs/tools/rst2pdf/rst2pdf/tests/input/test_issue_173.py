#!/usr/bin/env python
# -*- coding: utf-8 -*-

from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.platypus.doctemplate import Indenter
from reportlab.platypus.tables import *
from reportlab.lib.styles import getSampleStyleSheet

def go():
        Story=[]
        ts=TableStyle([('GRID',(0,0),(-1,-1),0.25,"black"),
                      ('BOX',(0,0),(-1,-1),0.25,"black")]
                     )
        styles = getSampleStyleSheet()
        doc = SimpleDocTemplate("phello.pdf")
        
        cell=[Paragraph('This is not indented',styles['Normal']),
              Indenter(100,100),
              Paragraph('This should be indented',styles['Normal'])]
        
        Story=cell+[Table([[cell]],style=ts)]
        doc.build(Story)

go()
