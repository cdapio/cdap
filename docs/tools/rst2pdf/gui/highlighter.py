# -*- coding: utf-8 -*-
import sys
import re
from PyQt4 import QtCore, QtGui
from pygments import highlight
from pygments.lexers import *
from pygments.formatter import Formatter
import time

# Copyright (C) 2008 Christophe Kibleur <kib2@free.fr>
#
# This file is part of WikiParser (http://thewikiblog.appspot.com/).
#

def hex2QColor(c):
    r=int(c[0:2],16)
    g=int(c[2:4],16)
    b=int(c[4:6],16)
    return QtGui.QColor(r,g,b)
    


class QFormatter(Formatter):
    
    def __init__(self):
        Formatter.__init__(self)
        self.data=[]
        
        # Create a dictionary of text styles, indexed
        # by pygments token names, containing QTextCharFormat
        # instances according to pygments' description
        # of each style
        
        self.styles={}
        for token, style in self.style:
            qtf=QtGui.QTextCharFormat()

            if style['color']:
                qtf.setForeground(hex2QColor(style['color'])) 
            if style['bgcolor']:
                qtf.setBackground(hex2QColor(style['bgcolor'])) 
            if style['bold']:
                qtf.setFontWeight(QtGui.QFont.Bold)
            if style['italic']:
                qtf.setFontItalic(True)
            if style['underline']:
                qtf.setFontUnderline(True)
            self.styles[str(token)]=qtf
    
    def format(self, tokensource, outfile):
        global styles
        # We ignore outfile, keep output in a buffer
        self.data=[]
        
        # Just store a list of styles, one for each character
        # in the input. Obviously a smarter thing with
        # offsets and lengths is a good idea!
        
        for ttype, value in tokensource:
            l=len(value)
            t=str(ttype)                
            self.data.extend([self.styles[t],]*l)


class Highlighter(QtGui.QSyntaxHighlighter):

    def __init__(self, parent, mode):
        QtGui.QSyntaxHighlighter.__init__(self, parent)
        self.tstamp=time.time()
        
        # Keep the formatter and lexer, initializing them 
        # may be costly.
        self.formatter=QFormatter()
        self.lexer=get_lexer_by_name(mode)
        self.enabled=True
        
        # Connect the document's contentChanged to our
        # reparse slot
        self.connect(parent,QtCore.SIGNAL("contentsChanged()"),
                     self.reparse)
        self.reparse()
        
    def reparse(self):
        # The \n is not really needed, but sometimes  
        # you are in an empty last block, so your position is
        # **after** the end of the document.
        text=unicode(self.document().toPlainText())+'\n'
        
        # Yes, re-highlight the whole document.
        # There **must** be some optimizacion possibilities
        # but it seems fast enough.
        highlight(text,self.lexer,self.formatter)
        
        
    def highlightBlock(self, text):
        """Takes a block, applies format to the document. 
        according to what's in it.
        """
        
        if not self.enabled:
            return
            
        # I need to know where in the document we are,
        # because our formatting info is global to
        # the document
        cb = self.currentBlock()
        p = cb.position()

        
        # Just apply the formatting to this block.
        # For titles, it may be necessary to backtrack
        # and format a couple of blocks **earlier**.
        for i in range(len(unicode(text))):
            try:
                self.setFormat(i,1,self.formatter.data[p+i])
            except IndexError:
                pass
        
        # I may need to do something about this being called
        # too quickly.
        self.tstamp=time.time() 


if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    
    rst = QtGui.QPlainTextEdit()
    rst.setWindowTitle('reSt')
    hl=Highlighter(rst.document(),"rest")
    rst.show()

    python = QtGui.QPlainTextEdit()
    python.setWindowTitle('python')
    hl=Highlighter(python.document(),"python")
    python.show()

    sys.exit(app.exec_())