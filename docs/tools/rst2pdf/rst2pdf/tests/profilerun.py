#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Run all tests under a profiling environment'''

import os
import cProfile
from rst2pdf.createpdf import RstToPdf

def run():
    inpdir=os.path.abspath('./input')
    outdir=os.path.abspath('./tmp')
    # Discard output, this is not about whether things 
    # work or not, that's testing ;-)

    for f in os.listdir(inpdir):
        if f.endswith('.txt'): # Test case
            print 'Running: ', f
            sheet=os.path.join(inpdir, f[:-4]+'.style')
            if os.path.exists(sheet):
                sheet=[sheet]
            else:
                sheet=[]
            
            r2p=RstToPdf(stylesheets=sheet)
            try:
                fname=os.path.join(inpdir, f)
                r2p.createPdf(
                    text=open(fname).read(),
                    output=os.path.join(outdir,f+'.pdf'),
                    source_path=fname,
                    )
            except:
                print 'FAIL'

cProfile.runctx( "run()", globals(), locals(), filename="rst2pdf.profile" )
