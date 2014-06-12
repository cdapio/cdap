# -*- coding: utf-8 -*-

import sys
from PyQt4 import QtGui, QtCore

class LineNumberArea(QtGui.QWidget):

    def __init__(self,editor):
        self.codeEditor=editor
        QtGui.QWidget.__init__(self, editor)

    def sizeHint(self):
        return QtCore.QSize(self.codeEditor.lineNumberAreaWidth(),0)

    def paintEvent(self, event):
        self.codeEditor.lineNumberAreaPaintEvent(event)



class CodeEditor(QtGui.QPlainTextEdit):
    
    def __init__(self,parent=None):
        QtGui.QPlainTextEdit.__init__(self,parent)
        self.lineNumberArea = LineNumberArea (self)
        self.connect(self, QtCore.SIGNAL("blockCountChanged(int)"), 
                     self.updateLineNumberAreaWidth)
            
        self.connect(self, QtCore.SIGNAL("updateRequest(const QRect &, int)"), 
                     self.updateLineNumberArea)
                
        self.connect(self, QtCore.SIGNAL("cursorPositionChanged()"), 
                     self.highlightCurrentLine)

        self.updateLineNumberAreaWidth(0)
        self.errorPos=None
        self.highlightCurrentLine()
        
    def lineNumberAreaPaintEvent(self, event):
        painter=QtGui.QPainter(self.lineNumberArea)
        painter.fillRect(event.rect(), QtCore.Qt.lightGray)
        
        block = self.firstVisibleBlock()
        blockNumber = block.blockNumber();
        top = int(self.blockBoundingGeometry(block).translated(self.contentOffset()).top())
        bottom = top + int(self.blockBoundingRect(block).height())
        
        while block.isValid() and top <= event.rect().bottom():
            
            
            if block.isVisible() and bottom >= event.rect().top():
                number = str(blockNumber + 1)
                painter.setPen(QtCore.Qt.black)
                painter.drawText(0, top, self.lineNumberArea.width(), 
                    self.fontMetrics().height(),
                    QtCore.Qt.AlignRight, number)

            block = block.next()
            top = bottom
            bottom = top + int(self.blockBoundingRect(block).height())
            blockNumber+=1
        
    def lineNumberAreaWidth(self):
        digits = 1
        _max = max (1, self.blockCount())
        while (_max >= 10):
            _max = _max/10
            digits+=1
        space = 5 + self.fontMetrics().width('9') * digits
        return space
    
    def updateLineNumberAreaWidth(self, newBlockCount):
        self.setViewportMargins(self.lineNumberAreaWidth(), 0, 0, 0)


    def updateLineNumberArea(self, rect, dy):
        
        if dy:
            self.lineNumberArea.scroll(0, dy);
        else:
            self.lineNumberArea.update(0, rect.y(), 
                self.lineNumberArea.width(), rect.height())

        if rect.contains(self.viewport().rect()):
            self.updateLineNumberAreaWidth(0)

    def resizeEvent(self, e):
        QtGui.QPlainTextEdit.resizeEvent(self,e)
        self.cr = self.contentsRect()
        self.lineNumberArea.setGeometry(self.cr.left(), 
                                        self.cr.top(), 
                                        self.lineNumberAreaWidth(), 
                                        self.cr.height())
                                        
    def highlightError(self,pos):
        self.errorPos=pos
        self.highlightCurrentLine()

             
    def highlightCurrentLine(self):         
        extraSelections=[]
        if not self.isReadOnly():             
            selection = QtGui.QTextEdit.ExtraSelection()
            lineColor = QtGui.QColor(QtCore.Qt.yellow).lighter(160)
            selection.format.setBackground(lineColor)
            selection.format.setProperty(QtGui.QTextFormat.FullWidthSelection, True)
            selection.cursor = self.textCursor()
            selection.cursor.clearSelection()
            extraSelections.append(selection)
            
            if self.errorPos is not None:
                errorSel = QtGui.QTextEdit.ExtraSelection()
                lineColor = QtGui.QColor(QtCore.Qt.red).lighter(160)
                errorSel.format.setBackground(lineColor)
                errorSel.format.setProperty(QtGui.QTextFormat.FullWidthSelection, True)
                errorSel.cursor = QtGui.QTextCursor(self.document())
                errorSel.cursor.setPosition(self.errorPos)
                errorSel.cursor.clearSelection()
                extraSelections.append(errorSel)

        self.setExtraSelections(extraSelections)

if __name__ == "__main__":
    
    try:
        import json
    except ImportError:
        import simplejson as json
    from highlighter import Highlighter
    
    app = QtGui.QApplication(sys.argv)

    js = CodeEditor()
    js.setWindowTitle('javascript')
    hl=Highlighter(js.document(),"javascript")
    js.show()

    def validateJSON():
        style=unicode(js.toPlainText())
        if not style.strip(): #no point in validating an empty string
            return
        pos=None
        try:
            json.loads(style)
        except ValueError, e:
            s=str(e)
            print s
            if s == 'No JSON object could be decoded':
                pos=0
            elif s.startswith('Expecting '):
                pos=int(s.split(' ')[-1][:-1])
            elif s.startswith('Extra data'):
                pos=int(s.split(' ')[-3])
            else:
                print 'UNKNOWN ERROR'
                
        # This makes a red bar appear in the line
        # containing position pos
        js.highlightError(pos)

    # Run validateJSON on every keypress
    js.connect(js,QtCore.SIGNAL('textChanged()'),validateJSON)

    sys.exit(app.exec_())
