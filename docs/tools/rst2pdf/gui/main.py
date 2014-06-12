#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The user interface for our app"""

import os,sys,tempfile,re,functools,time,types,glob,codecs
from pprint import pprint
from copy import copy
from multiprocessing import Process, Queue
from Queue import Empty
from hashlib import md5
from StringIO import StringIO
from rst2pdf.createpdf import RstToPdf
from rst2pdf.styles import StyleSheet
from rst2pdf.log import log
import logging
import reportlab.lib.pagesizes as pagesizes

log.setLevel(logging.INFO)

import docutils

# Import Qt modules
from PyQt4 import QtCore,QtGui
from pypoppler import QtPoppler

# Syntax HL
from highlighter import Highlighter

# Import the compiled UI module
from Ui_main import Ui_MainWindow
from Ui_pdf import Ui_Form

try:
    import json
except ImportError:
    import simplejson as json

try:
    from lxml import etree
    print("running with lxml.etree")
except ImportError:
    try:
        # Python 2.5
        import xml.etree.cElementTree as etree
        print("running with cElementTree on Python 2.5+")
    except ImportError:
        try:
            # Python 2.5
            import xml.etree.ElementTree as etree
            print("running with ElementTree on Python 2.5+")
        except ImportError:
            try:
                # normal cElementTree install
                import cElementTree as etree
                print("running with cElementTree")
            except ImportError:
                try:
                    # normal ElementTree install
                    import elementtree.ElementTree as etree
                    print("running with ElementTree")
                except ImportError:
                    print("Failed to import ElementTree from any known place")


from pygments import highlight
from pygments.lexers import *
from pygments.formatters import HtmlFormatter

StringTypes=types.StringTypes+(QtCore.QString,)


def renderQueue(render_queue, pdf_queue, doctree_queue):
    _renderer = RstToPdf(splittables=True)

    def render(doctree, preview=True):
        '''Render text to PDF via rst2pdf'''
        # FIXME: get parameters for this from somewhere

        sio=StringIO()
        _renderer.createPdf(doctree=doctree, output=sio, debugLinesPdf=preview)
        return sio.getvalue()

    while True:
        try:
            style_file, text, preview = render_queue.get(10)
            style_file, text, preview = render_queue.get(False)
        except Empty: # no more things to render, so do it
            try:
                if style_file:
                    _renderer.loadStyles([style_file])
                flag = True
                #os.unlink(style_file)
                warnings=StringIO()
                doctree = docutils.core.publish_doctree(text,
                    settings_overrides={'warning_stream':warnings})
                doctree_queue.put([doctree,warnings.getvalue()])
                pdf_queue.put(render(doctree, preview))
            except Exception, e:
                # Don't crash ever ;-)
                print e
                pass
        if os.getppid()==1: # Parent died
            sys.exit(0)


class Main(QtGui.QMainWindow):
    def __init__(self):
        QtGui.QMainWindow.__init__(self)

        self.doctree=None
        self.lineMarks={}

        # We put things we want rendered here
        self.render_queue = Queue()
        # We get things rendered back
        self.pdf_queue = Queue()
        # We get doctrees for the outline viewer
        self.doctree_queue = Queue()

        print 'Starting background renderer...',
        self.renderProcess=Process(target = renderQueue,
            args=(self.render_queue, self.pdf_queue, self.doctree_queue))
        self.renderProcess.daemon=True
        self.renderProcess.start()
        print 'DONE'

        # This is always the same
        self.ui=Ui_MainWindow()
        self.ui.setupUi(self)

        # Adjust column widths in the structure tree
        self.ui.tree.header().setStretchLastSection(False)
        self.ui.tree.header().setResizeMode(0, QtGui.QHeaderView.Stretch)
        self.ui.tree.header().setResizeMode(1, QtGui.QHeaderView.ResizeToContents)

        self.pdf=PDFWidget()

        self.ui.pageNum = QtGui.QSpinBox()
        self.ui.pageNum.setMinimum(1)
        self.ui.pageNum.setValue(1)
        self.connect(self.pdf,QtCore.SIGNAL('pageCount'),
            self.ui.pageNum.setMaximum)
        self.connect(self.pdf,QtCore.SIGNAL('pageChanged'),
            self.ui.pageNum.setValue)
        self.connect(self.ui.pageNum,QtCore.SIGNAL('valueChanged(int)'),
            self.pdf.gotoPage)

        self.ui.actionShow_ToolBar=self.ui.toolBar.toggleViewAction()
        self.ui.actionShow_ToolBar.setText("Show Main Toolbar")
        self.ui.menuView.addAction(self.ui.actionShow_ToolBar)

        self.ui.pdfbar.addAction(self.pdf.ui.previous)
        self.ui.pdfbar.addWidget(self.ui.pageNum)
        self.ui.pdfbar.addAction(self.pdf.ui.next)
        self.ui.pdfbar.addSeparator()
        self.ui.pdfbar.addAction(self.pdf.ui.zoomin)
        self.ui.pdfbar.addAction(self.pdf.ui.zoomout)
        self.ui.actionShow_PDFBar=self.ui.pdfbar.toggleViewAction()
        self.ui.actionShow_PDFBar.setText("Show PDF Toolbar")
        self.ui.menuView.addAction(self.ui.actionShow_PDFBar)

        self.ui.dockLayout.addWidget(self.ui.pdfbar)
        self.ui.dockLayout.addWidget(self.pdf)
        self.ui.dock.hide()
        self.ui.actionShow_PDF=self.ui.dock.toggleViewAction()
        self.ui.actionShow_PDF.setText('Show Preview')
        self.ui.menuView.addAction(self.ui.actionShow_PDF)
        self.ui.actionShow_Structure=self.ui.structure.toggleViewAction()
        self.ui.actionShow_Structure.setText('Show Document Outline')
        self.ui.menuView.addAction(self.ui.actionShow_Structure)

        self.text_md5=''
        self.style_md5=''

        self.hl1 = Highlighter(self.ui.text.document(),'rest')
        self.hl2 = Highlighter(self.ui.style.document(),'javascript')

        self.editorPos=QtGui.QLabel()
        self.ui.statusBar.addWidget(self.editorPos)
        self.editorPos.show()

        self.statusMessage=QtGui.QLabel()
        self.ui.statusBar.addWidget(self.statusMessage)
        self.statusMessage.show()

        self.on_text_cursorPositionChanged()
        self.on_actionRender_triggered()

        # Connect editing actions to the editors
        self.ui.text.undoAvailable.connect(self.ui.actionUndo1.setEnabled)
        self.ui.actionUndo1.triggered.connect(self.ui.text.undo)
        self.ui.text.redoAvailable.connect(self.ui.actionRedo1.setEnabled)
        self.ui.actionRedo1.triggered.connect(self.ui.text.redo)

        self.ui.text.copyAvailable.connect(self.ui.actionCopy1.setEnabled)
        self.ui.actionCopy1.triggered.connect(self.ui.text.copy)
        self.ui.text.copyAvailable.connect(self.ui.actionCut1.setEnabled)
        self.ui.actionCut1.triggered.connect(self.ui.text.cut)
        self.ui.actionPaste1.triggered.connect(self.ui.text.paste)


        self.ui.style.undoAvailable.connect(self.ui.actionUndo2.setEnabled)
        self.ui.actionUndo2.triggered.connect(self.ui.style.undo)
        self.ui.style.redoAvailable.connect(self.ui.actionRedo2.setEnabled)
        self.ui.actionRedo2.triggered.connect(self.ui.style.redo)

        self.ui.style.copyAvailable.connect(self.ui.actionCopy2.setEnabled)
        self.ui.actionCopy2.triggered.connect(self.ui.style.copy)
        self.ui.style.copyAvailable.connect(self.ui.actionCut2.setEnabled)
        self.ui.actionCut2.triggered.connect(self.ui.style.cut)
        self.ui.actionPaste2.triggered.connect(self.ui.style.paste)

        self.clipBoard=QtGui.QApplication.clipboard()
        self.clipBoard.changed.connect(self.clipChanged)

        self.hookEditToolbar(self.ui.text)
        self.clipChanged(QtGui.QClipboard.Clipboard)

        self.text_fname=None
        self.style_fname=None
        self.pdf_fname=None

        self.ui.searchbar.setVisible(False)
        self.ui.searchWidget=SearchWidget()
        self.ui.searchbar.addWidget(self.ui.searchWidget)
        self.ui.actionFind.triggered.connect(self.ui.searchbar.show)
        self.ui.actionFind.triggered.connect(self.ui.searchWidget.ui.text.setFocus)
        self.ui.searchWidget.ui.close.clicked.connect(self.ui.searchbar.hide)
        self.ui.searchWidget.ui.close.clicked.connect(self.returnFocus)
        self.ui.searchWidget.ui.next.clicked.connect(self.doFind)
        self.ui.searchWidget.ui.previous.clicked.connect(self.doFindBackwards)

        self.updatePdf()

        self.renderTimer=QtCore.QTimer()
        self.renderTimer.timeout.connect(self.on_actionRender_triggered)
        self.renderTimer.start(5000)

    def returnFocus(self):
        """after the search bar closes, focus on the editing widget"""
        print 'RF:', self.ui.tabs.currentIndex()
        if self.ui.tabs.currentIndex()==0:
            self.ui.text.setFocus()
        else:
            self.ui.style.setFocus()

    def doFindBackwards (self):
        return self.doFind(backwards=True)

    def doFind(self, backwards=False):

        flags=QtGui.QTextDocument.FindFlags()
        print flags
        if backwards:
            flags=QtGui.QTextDocument.FindBackward
        if self.ui.searchWidget.ui.matchCase.isChecked():
            flags=flags|QtGui.QTextDocument.FindCaseSensitively

        text=unicode(self.ui.searchWidget.ui.text.text())

        print 'Serching for:',text

        if self.ui.tabs.currentIndex()==0:
            r=self.ui.text.find(text,flags)
        else:
            r=self.ui.style.find(text,flags)
        if r:
            self.statusMessage.setText('')
        else:
            self.statusMessage.setText('%s not found'%text)

    def clipChanged(self, mode=None):
        if mode is None: return
        if mode == QtGui.QClipboard.Clipboard:
            if unicode(self.clipBoard.text()):
                self.ui.actionPaste1.setEnabled(True)
                self.ui.actionPaste2.setEnabled(True)
            else:
                self.ui.actionPaste1.setEnabled(False)
                self.ui.actionPaste2.setEnabled(False)

    def hookEditToolbar(self, editor):
        if editor == self.ui.text:
            self.ui.actionUndo2.setVisible(False)
            self.ui.actionRedo2.setVisible(False)
            self.ui.actionCut2.setVisible(False)
            self.ui.actionPaste2.setVisible(False)
            self.ui.actionCopy2.setVisible(False)
            self.ui.actionUndo1.setVisible(True)
            self.ui.actionRedo1.setVisible(True)
            self.ui.actionCut1.setVisible(True)
            self.ui.actionPaste1.setVisible(True)
            self.ui.actionCopy1.setVisible(True)
        else:
            self.ui.actionUndo1.setVisible(False)
            self.ui.actionRedo1.setVisible(False)
            self.ui.actionCut1.setVisible(False)
            self.ui.actionPaste1.setVisible(False)
            self.ui.actionCopy1.setVisible(False)
            self.ui.actionUndo2.setVisible(True)
            self.ui.actionRedo2.setVisible(True)
            self.ui.actionCut2.setVisible(True)
            self.ui.actionPaste2.setVisible(True)
            self.ui.actionCopy2.setVisible(True)


    def createPopupMenu(self):
        self.popup=QtGui.QMenu()
        self.popup.addAction(self.ui.actionShow_ToolBar)
        self.popup.addAction(self.ui.actionShow_PDFBar)
        self.popup.addAction(self.ui.actionShow_PDF)
        return self.popup

    def enableHL(self):
        self.hl1.enabled=True
        self.hl2.enabled=True
        self.hl1.rehighlight()
        self.hl2.rehighlight()

    def disableHL(self):
        self.hl1.enabled=False
        self.hl2.enabled=False

    def on_actionSettings_triggered(self, b=None):
        if b is not None: return

        # I need to create a stylesheet object so I can parse and merge
        # the current stylesheet

        try:
            data=json.loads(unicode(self.ui.style.toPlainText()))
        except: # TODO: fail if sheet doesn't validate
            data={}
        config=ConfigDialog(data=copy(data))
        config.exec_()

        # merge the edited stylesheet with current one because the editor
        # is not complete yet. When it is, just replace it.
        data.update(config.data)
        self.ui.style.setPlainText(json.dumps(data, indent=2))

    def on_actionTest_Action_triggered(self, b=None):
        if b is not None: return



        self.testwidget=PageTemplates(self.styles)
        self.testwidget.show()


    def on_tree_itemClicked(self, item=None, column=None):
        if item is None: return

        destline=int(item.text(1))-1
        destblock=self.ui.text.document().findBlockByLineNumber(destline)
        cursor=self.ui.text.textCursor()
        cursor.setPosition(destblock.position())
        self.ui.text.setTextCursor(cursor)
        self.ui.text.ensureCursorVisible()

    def on_actionAbout_Bookrest_triggered(self, b=None):
        if b is None: return
        dlg=AboutDialog()
        dlg.exec_()

    def on_actionSave_Text_triggered(self, b=None):
        if b is not None: return

        if self.text_fname is not None:
            f=codecs.open(self.text_fname,'w+','utf-8')
            f.seek(0)
            f.write(unicode(self.ui.text.toPlainText()))
            f.close()
        else:
            self.on_actionSaveAs_Text_triggered()


    def on_actionSaveAs_Text_triggered(self, b=None):
        if b is not None: return

        fname=unicode(QtGui.QFileDialog.getSaveFileName(self,
                            'Save As',
                            os.getcwd(),
                            'reSt files (*.txt *.rst)'
                            ))
        if fname:
            self.text_fname=fname
            self.on_actionSave_Text_triggered()

    def on_actionLoad_Text_triggered(self, b=None):
        if b is None: return
        fname=QtGui.QFileDialog.getOpenFileName(self,
                                                'Open File',
                                                os.getcwd(),
                                                'reSt files (*.txt *.rst)'
                                                )
        self.text_fname=fname
        self.disableHL()
        self.ui.text.setPlainText(codecs.open(self.text_fname,'r','utf-8').read())
        self.enableHL()

    def on_actionSave_Style_triggered(self, b=None):
        if b is not None: return

        if self.style_fname is not None:
            f=codecs.open(self.style_fname,'w+','utf-8')
            f.seek(0)
            f.write(unicode(self.ui.style.toPlainText()))
            f.close()
        else:
            self.on_actionSaveAs_Style_triggered()


    def on_actionSaveAs_Style_triggered(self, b=None):
        if b is not None: return

        fname=unicode(QtGui.QFileDialog.getSaveFileName(self,
                            'Save As',
                            os.getcwd(),
                            'style files (*.json *.style)'
                            ))
        if fname:
            self.style_fname=fname
            self.on_actionSave_Style_triggered()


    def on_actionLoad_Style_triggered(self, b=None):
        if b is None: return

        fname=QtGui.QFileDialog.getOpenFileName(self,
                                                'Open File',
                                                os.getcwd(),
                                                'style files (*.json *.style)'
                                                )
        self.style_fname=fname
        self.disableHL()
        self.ui.style.setPlainText(codecs.open(self.style_fname,'rb', 'utf-8').read())
        self.enableHL()

    def on_actionSave_PDF_triggered(self, b=None):
        if b is not None: return

        # render it without line numbers in the toc
        self.on_actionRender_triggered(preview=False)

        if self.pdf_fname is not None:
            f=open(self.pdf_fname,'wb+')
            f.seek(0)
            f.write(self.goodPDF)
            f.close()
        else:
            self.on_actionSaveAs_PDF_triggered()


    def on_actionSaveAs_PDF_triggered(self, b=None):
        if b is not None: return

        fname=unicode(QtGui.QFileDialog.getSaveFileName(self,
                            'Save As',
                            os.getcwd(),
                            'PDF files (*.pdf)'
                            ))
        if fname:
            self.pdf_fname=fname
            self.on_actionSave_PDF_triggered()

    def on_tabs_currentChanged(self, i=None):
        print 'IDX:',self.ui.tabs.currentIndex()
        if self.ui.tabs.currentIndex() == 0:
            self.on_text_cursorPositionChanged()
            print 'hooking text editor'
            self.hookEditToolbar(self.ui.text)
        else:
            self.on_style_cursorPositionChanged()
            print 'hooking style editor'
            self.hookEditToolbar(self.ui.style)

    def on_style_cursorPositionChanged(self):
        cursor=self.ui.style.textCursor()
        self.editorPos.setText('Line: %d Col: %d'%(cursor.blockNumber(),cursor.columnNumber()))

    def on_text_cursorPositionChanged(self):
        cursor=self.ui.text.textCursor()
        row=cursor.blockNumber()
        column=cursor.columnNumber()
        self.editorPos.setText('Line: %d Col: %d'%(row,column))
        l='line-%s'%(row+1)
        m=self.lineMarks.get(l,None)
        if m:
            self.pdf.gotoPosition(*m)
    def validateStyle(self):
        style=unicode(self.ui.style.toPlainText())
        if not style.strip(): #no point in validating an empty string
            self.statusMessage.setText('')
            return
        pos=None
        try:
            json.loads(style)
            self.statusMessage.setText('')
        except ValueError, e:
            s=str(e)
            if s == 'No JSON object could be decoded':
                pos=0
            elif s.startswith('Expecting '):
                pos=int(s.split(' ')[-1][:-1])
            elif s.startswith('Extra data'):
                pos=int(s.split(' ')[-3])
            else:
                pass
            self.statusMessage.setText('Stylesheet error: %s'%s)

        # This makes a red bar appear in the line
        # containing position pos
        self.ui.style.highlightError(pos)

    on_style_textChanged = validateStyle

    def on_actionRender_triggered(self, b=None, preview=True):
        if b is not None: return
        text=unicode(self.ui.text.toPlainText())
        style=unicode(self.ui.style.toPlainText())
        self.hl1.rehighlight()
        m1=md5()
        m1.update(text.encode('utf-8'))
        m1=m1.digest()
        m2=md5()
        m2.update(style.encode('utf-8'))
        m2=m2.digest()

        flag = m1 != self.text_md5
        style_file=None
        if m2 != self.style_md5 and style:
            fd, style_file=tempfile.mkstemp()
            os.write(fd,style)
            os.close(fd)
            print 'Loading styles from style_file'
            flag = True
        if flag:
            if not preview:
                pass
                # Send text to the renderer in foreground
                # FIXME: render is no longer accessible from the parent
                # process
                #doctree = docutils.core.publish_doctree(text)
                #self.goodPDF=render(doctree, preview=False)
            else:
                # Que to render in background
                self.render_queue.put([style_file, text, preview])
                self.text_md5=m1
                self.style_md5=m2

    def updatePdf(self):

        # See if there is something in the doctree Queue
        try:
            self.doctree, self.warnings = self.doctree_queue.get(False)
            self.doctree.reporter=log
            class Visitor(docutils.nodes.SparseNodeVisitor):

                def __init__(self, document, treeWidget):
                    self.treeWidget=treeWidget
                    self.treeWidget.clear()
                    self.doctree=document
                    self.nodeDict={}
                    docutils.nodes.SparseNodeVisitor.__init__(self, document)

                def visit_section(self, node):
                    print 'SECTION:',node.line,
                    item=QtGui.QTreeWidgetItem(["",str(node.line)])
                    if node.parent==self.doctree:
                        # Top level section
                        self.treeWidget.addTopLevelItem(item)
                        self.nodeDict[id(node)]=item
                    else:
                        self.nodeDict[id(node.parent)].addChild(item)
                        self.nodeDict[id(node)]=item

                def visit_title(self, node):
                    if id(node.parent) in self.nodeDict:
                        self.nodeDict[id(node.parent)].setText(0,node.astext())

                def visit_document(self,node):
                    print 'DOC:',node.line

            print self.doctree.__class__
            self.visitor=Visitor(self.doctree, self.ui.tree)
            self.doctree.walkabout(self.visitor)
            print self.visitor.nodeDict

        except Empty:
            pass

        # See if there is something in the PDF Queue
        try:
            self.lastPDF=self.pdf_queue.get(False)
            self.pdf.loadDocument(self.lastPDF)
            toc=self.pdf.document.toc()
            if toc:
                tempMarks=[]
                def traverse(node):
                    children=node.childNodes()
                    for i in range(children.length()):
                        n=children.item(i)
                        e=n.toElement()
                        if e:
                            tag=str(e.tagName())
                            if tag.startswith('LINE'):
                                dest=str(e.attribute('Destination'))
                                dest=QtPoppler.Poppler.LinkDestination(dest)
                                tempMarks.append([int(tag.split('-')[1]),
                                    [dest.pageNumber(), dest.top(), dest.left(),1.]])
                        traverse(n)
                traverse(toc)
                tempMarks.sort()

                self.lineMarks={}
                lastMark=None
                lastKey=0
                for key,dest in tempMarks:
                    # Fix height of the previous mark, unless we changed pages
                    if lastMark and self.lineMarks[lastMark][0]==dest[0]:
                        self.lineMarks[lastMark][3]=dest[1]
                    # Fill missing lines

                    if lastMark:
                        ldest=self.lineMarks[lastMark]
                    else:
                        ldest=[1,0,0,0]
                    for n in range(lastKey,key):
                        self.lineMarks['line-%s'%n]=ldest
                    k='line-%s'%key
                    self.lineMarks[k]=dest
                    lastMark = k
                    lastKey = key

            self.on_text_cursorPositionChanged()
        except Empty: #Nothing there
            pass

        # Schedule to run again
        QtCore.QTimer.singleShot(500,self.updatePdf)


def main():
    # Again, this is boilerplate, it's going to be the same on
    # almost every app you write
    app = QtGui.QApplication(sys.argv)
    window=Main()
    window.show()
    # It's exec_ because exec is a reserved word in Python
    sys.exit(app.exec_())


class PDFWidget(QtGui.QWidget):
    def __init__(self,parent=None):
        QtGui.QWidget.__init__(self,parent)
        self.ui=Ui_Form()
        self.ui.setupUi(self)
        self.pdfd = None

    def loadDocument(self,data):
        self.document = QtPoppler.Poppler.Document.loadFromData(data)
        self.emit(QtCore.SIGNAL('pageCount'),self.document.numPages())
        self.document.setRenderHint(QtPoppler.Poppler.Document.Antialiasing and QtPoppler.Poppler.Document.TextAntialiasing)

        # When rerendering, keep state as much as possible in
        # the viewer

        if self.pdfd:
            res = self.pdfd.res
            xpos = self.ui.scroll.horizontalScrollBar().value()
            ypos = self.ui.scroll.verticalScrollBar().value()
            currentPage = self.pdfd.currentPage
        else:
            res=72.
            xpos=0
            ypos=0
            currentPage = 0

        self.pdfd=PDFDisplay(self.document)
        self.connect(self.pdfd,QtCore.SIGNAL('pageChanged'),
                     self,QtCore.SIGNAL('pageChanged'))
        self.pdfd.currentPage = currentPage
        self.checkActions()
        self.pdfd.res = res
        self.ui.scroll.setWidget(self.pdfd)
        self.ui.scroll.horizontalScrollBar().setValue(xpos)
        self.ui.scroll.verticalScrollBar().setValue(ypos)

    def checkActions(self):
        if not self.pdfd or \
               self.pdfd.currentPage == self.document.numPages():
            self.ui.next.setEnabled(False)
        else:
            self.ui.next.setEnabled(True)
        if not self.pdfd or \
                self.pdfd.currentPage == 1:
            self.ui.previous.setEnabled(False)
        else:
            self.ui.previous.setEnabled(True)

    def gotoPosition(self, page, top, left, bottom):
        """The position is defined in terms of poppler's linkdestinations,
        top is in the range 0-1, page is one-based."""

        if not self.pdfd:
            return

        self.gotoPage(page)

        # Draw a mark to see if we are calculating correctly
        pixmap=QtGui.QPixmap(self.pdfd.pdfImage)
        p=QtGui.QPainter(pixmap)
        c=QtGui.QColor(QtCore.Qt.yellow).lighter(160)
        c.setAlpha(150)
        p.setBrush(c)
        p.setPen(c)
        # FIXME, move the highlighting outside
        y1=self.pdfd.pdfImage.height()*top
        y2=self.pdfd.pdfImage.height()*(bottom-top)
        w=self.pdfd.pdfImage.width()
        p.drawRect(0,y1,w,y2)
        self.pdfd.setPixmap(pixmap)
        p.end()



    def gotoPage(self,n):
        if self.pdfd:
            self.pdfd.currentPage = n
        self.checkActions()

    def on_next_triggered(self, b=None):
        if b is None: return
        self.pdfd.nextPage()
        self.checkActions()

    def on_previous_triggered(self, b=None):
        if b is None: return
        self.pdfd.prevPage()
        self.checkActions()

    def on_zoomin_triggered(self, b=None):
        if b is None: return
        self.pdfd.zoomin()

    def on_zoomout_triggered(self, b=None):
        if b is None: return
        self.pdfd.zoomout()


class PDFDisplay(QtGui.QLabel):
    def __init__(self, doc):
        QtGui.QLabel.__init__(self, None)
        self.doc = doc
        self.pdfImage = None
        self._res = self.physicalDpiX()

        self._currentPage = 1
        self.display()

    @property
    def currentPage(self):
        '''The currently displayed page'''
        return self._currentPage

    @currentPage.setter
    def currentPage(self,value):
        value=int(value)
        if value != self._currentPage and \
                0 < value <= self.doc.numPages():
            self._currentPage = value
            self.display()
            self.emit(QtCore.SIGNAL('pageChanged'),self._currentPage)

    # Just so I can connect a signal to this
    def setCurrentPage(self,value):
        self.currentPage=value

    def nextPage(self):
        self.currentPage += 1

    def prevPage(self):
        self.currentPage -= 1

    @property
    def res(self):
        '''Display resolution in DPI'''
        return self._res

    @res.setter
    def res(self,value):
        self._res=value
        self.display()

    def zoomin(self):
        self.res=self.res*1.25

    def zoomout(self):
        self.res=self.res/1.25

    def display(self):
        if self.doc is not None:
            if self.doc.numPages() == 0:
                self.pdfImage = QtGui.QImage()
            else:
                page = self.doc.page(self.currentPage-1)
                if page:
                    self.pdfImage = None
                    self.pdfImage = page.renderToImage(self.res, self.res)
                    self.resize(self.pdfImage.width(),self.pdfImage.height())
            self.setPixmap(QtGui.QPixmap.fromImage(self.pdfImage))
                #self.update()
                #delete page;

# Firefox-style in-window search widget,
# copied from uRSSus: http://urssus.googlecode.com
from Ui_searchwidget import Ui_Form as UI_SearchWidget

class SearchWidget(QtGui.QWidget):
  def __init__(self):
    QtGui.QWidget.__init__(self)
    # Set up the UI from designer
    self.ui=UI_SearchWidget()
    self.ui.setupUi(self)

# Cute about dialog
from Ui_about import Ui_Dialog as Ui_AboutDialog


class AboutDialog(QtGui.QDialog):
  def __init__(self):
    QtGui.QDialog.__init__(self)
    # Set up the UI from designer
    self.ui=Ui_AboutDialog()
    self.ui.setupUi(self)

# Configuration dialog
from Ui_configdialog import Ui_Dialog as Ui_ConfigDialog


class ConfigDialog(QtGui.QDialog):
    def __init__(self, data={}):
        QtGui.QDialog.__init__(self)
        # Set up the UI from designer
        self.ui=Ui_ConfigDialog()
        self.ui.setupUi(self)
        self.curPageWidget=None
        self.scale=.3
        self.data=data

        # Load all config things
        self.pages={
            'Stylesheets':StyleSheets,
            'Page Setup':PageSetup,
            'Page Templates':PageTemplates,
        }
        keys=self.pages.keys()
        keys.sort()
        for page in keys:
            self.ui.pagelist.addItem(page)

    def on_pagelist_currentTextChanged(self, text=None):
        if text is None: return

        fd, style_file=tempfile.mkstemp()
        os.write(fd,json.dumps(self.data))
        os.close(fd)
        self.styles = StyleSheet([style_file])
        os.unlink(style_file)

        text=unicode(text)
        if self.curPageWidget:
            self.curPageWidget.hide()
            self.curPageWidget.deleteLater()
        widget=self.pages[text]
        self.curPageWidget=widget(self.styles,
                                  self.data,
                                  self.ui.preview,
                                  self.ui.snippet)
        self.ui.layout.addWidget(self.curPageWidget)
        self.curPageWidget.show()
        self.curPageWidget.updatePreview()

    def on_zoomin_clicked(self):
        self.scale=self.scale*1.25
        if self.curPageWidget:
            self.curPageWidget.scale=self.scale
            self.curPageWidget.updatePreview()

    def on_zoomout_clicked(self):
        self.scale=self.scale/1.25
        if self.curPageWidget:
            self.curPageWidget.scale=self.scale
            self.curPageWidget.updatePreview()

# Widget to edit page templates
from Ui_pagetemplates import Ui_Form as Ui_templates


class PageTemplates(QtGui.QWidget):
    def __init__(self, stylesheet, data, preview, snippet, parent=None):
        QtGui.QWidget.__init__(self,parent)
        self.scale = .3
        self.data = data
        self.ui=Ui_templates()
        self.ui.setupUi(self)
        self.ui.preview = preview
        self.ui.snippet = snippet
        self.stylesheet = stylesheet
        self.pw=self.stylesheet.ps[0]
        self.ph=self.stylesheet.ps[1]
        self.pageImage=QtGui.QImage(int(self.pw),
                                    int(self.ph),
                                    QtGui.QImage.Format_RGB32)

        self.templates = copy(self.stylesheet.pageTemplates)
        self.template = None
        for template in self.templates:
            self.ui.templates.addItem(template)

    def applyChanges(self):
        # TODO: validate everything
        self.frame=[unicode(w.text()) for w in [
            self.ui.left,
            self.ui.top,
            self.ui.width,
            self.ui.height
            ]]
        self.template["frames"][self.frameIndex]=self.frame
        self.template['showFooter']=self.ui.footer.isChecked()
        self.template['showHeader']=self.ui.header.isChecked()
        if unicode(self.ui.background.text()):
            self.template['background']=unicode(self.ui.background.text())
        self.updatePreview()

    def on_templates_currentIndexChanged(self, text):
        if not isinstance(text,StringTypes): return
        text=unicode(text)
        self.template=self.templates[text]
        self.ui.frames.clear()
        for i in range(0, len(self.template['frames'])):
            self.ui.frames.addItem('Frame %d'%(i+1))
        self.ui.footer.setChecked(self.template['showFooter'])
        self.ui.header.setChecked(self.template['showHeader'])
        self.ui.background.setText(self.template.get("background",""))
        self.updatePreview()

    def on_frames_currentIndexChanged(self, index):
        if type(index) != types.IntType: return
        if not self.template: return
        self.frameIndex=index
        self.frame=self.template['frames'][index]
        self.ui.left.setText(self.frame[0])
        self.ui.top.setText(self.frame[1])
        self.ui.width.setText(self.frame[2])
        self.ui.height.setText(self.frame[3])
        self.updatePreview()


    def on_selectFile_clicked(self,b=None):
        if b is not None: return

        fname=QtGui.QFileDialog.getOpenFileName(self,
                                                'Open Background Image',
                                                os.getcwd()
                                                )
        self.ui.background.setText(fname)
        self.applyChanges()

    def updatePreview(self):
        pm=QtGui.QPixmap(self.pageImage)
        p=QtGui.QPainter(pm)
        # Draw white page
        p.setBrush(QtGui.QBrush(QtGui.QColor("white")))
        p.drawRect(-1,-1,pm.width()+2,pm.height()+2)

        pen = QtGui.QPen()
        pen.setWidth(1/self.scale) # Make it be 1px wide when scaled
        p.setPen(pen)

        # Draw background
        bg=self.template.get("background",None)
        if bg:
            bg=QtGui.QImageReader(bg,)
            bg.setScaledSize(QtCore.QSize(pm.width(),pm.height()))
            p.drawImage(QtCore.QPoint(0,0),bg.read())

        x1=self.stylesheet.lm
        y1=self.stylesheet.tm
        tw=self.stylesheet.pw-self.stylesheet.lm-self.stylesheet.rm
        th=self.stylesheet.ph-self.stylesheet.bm-self.stylesheet.tm

        def drawFrame(frame):
            x=self.stylesheet.adjustUnits(frame[0],tw)
            y=self.stylesheet.adjustUnits(frame[1],th)
            w=self.stylesheet.adjustUnits(frame[2],tw)-1
            h=self.stylesheet.adjustUnits(frame[3],th)-1
            p.drawRect(x1+x,y1+y,w,h)

        p.setBrush(QtGui.QBrush(QtGui.QColor(150,150,150,128)))
        for frame in self.template['frames']:
            drawFrame(frame)
        p.setBrush(QtGui.QBrush(QtGui.QColor(255,255,0,128)))
        drawFrame(self.frame)
        self.ui.preview.setPixmap(pm.scaled(self.pw*self.scale,self.ph*self.scale))
        p.end()
        self.data['pageTemplates']=self.templates
        body=highlight(json.dumps(self.data, indent=2),
            JavascriptLexer(),HtmlFormatter())
        head=HtmlFormatter().get_style_defs('.highlight')
        self.ui.snippet.setHtml(
        '''<HEAD>
             <STYLE type="text/css">
             %s
             </STYLE>
           </HEAD>
           <BODY>
           %s
           </BODY>
        '''%(head,body))


# Widget to edit page templates
from Ui_pagesetup import Ui_Form as Ui_pagesetup


class PageSetup(QtGui.QWidget):
    def __init__(self, stylesheet, data, preview, snippet, parent=None):
        QtGui.QWidget.__init__(self,parent)
        self.scale = .3
        self.data = data
        self.ui=Ui_pagesetup()
        self.ui.setupUi(self)
        self.stylesheet=stylesheet
        self.ui.preview=preview
        self.ui.snippet=snippet
        ft=self.stylesheet.firstTemplate
        for template in self.stylesheet.pageTemplates:
            if ft == template:
                continue
            self.ui.firstTemplate.addItem(template)

        if 'size' in self.stylesheet.page:
            self.ui.size.insertItem(0,self.stylesheet.psname)
            self.ui.height.setEnabled(False)
            self.ui.width.setEnabled(False)
            self.ui.height.setText('')
            self.ui.width.setText('')
        else:
            self.ui.height.setEnabled(True)
            self.ui.width.setEnabled(True)
            self.ui.height.setText(self.stylesheet.page['height'])
            self.ui.width.setText(self.stylesheet.page['width'])
        self.ui.size.setCurrentIndex(0)

        self.ui.firstTemplate.insertItem(0,ft)
        self.ui.firstTemplate.setCurrentIndex(0)
        self.ui.margin_top.setText(unicode(self.stylesheet.page['margin-top']))
        self.ui.margin_bottom.setText(unicode(self.stylesheet.page['margin-bottom']))
        self.ui.margin_left.setText(unicode(self.stylesheet.page['margin-left']))
        self.ui.margin_right.setText(unicode(self.stylesheet.page['margin-right']))
        self.ui.margin_gutter.setText(unicode(self.stylesheet.page['margin-gutter']))
        self.ui.spacing_header.setText(unicode(self.stylesheet.page['spacing-header']))
        self.ui.spacing_footer.setText(unicode(self.stylesheet.page['spacing-footer']))
        self.pageImage=None
        self.applyChanges()

    def applyChanges(self):
        if unicode(self.ui.size.currentText())==u'Custom':
            # FIXME: % makes no sense for page size
            self.ui.width.setEnabled(True)
            self.ui.height.setEnabled(True)
            self.pw=self.stylesheet.adjustUnits(unicode(self.ui.width.text()),1000) or 0
            self.ph=self.stylesheet.adjustUnits(unicode(self.ui.height.text()),1000) or 0
        else:
            self.ui.width.setEnabled(False)
            self.ui.height.setEnabled(False)
            self.size=unicode(self.ui.size.currentText())
            self.pw=pagesizes.__dict__[self.size.upper()][0]
            self.ph=pagesizes.__dict__[self.size.upper()][1]

        self.lm=self.stylesheet.adjustUnits(unicode(self.ui.margin_left.text()),self.pw) or 0
        self.rm=self.stylesheet.adjustUnits(unicode(self.ui.margin_right.text()),self.pw) or 0
        self.tm=self.stylesheet.adjustUnits(unicode(self.ui.margin_top.text()),self.ph) or 0
        self.bm=self.stylesheet.adjustUnits(unicode(self.ui.margin_bottom.text()),self.ph) or 0
        self.ts=self.stylesheet.adjustUnits(unicode(self.ui.spacing_header.text()),self.ph) or 0
        self.bs=self.stylesheet.adjustUnits(unicode(self.ui.spacing_footer.text()),self.ph) or 0
        self.gm=self.stylesheet.adjustUnits(unicode(self.ui.margin_gutter.text()),self.pw) or 0
        self.pageImage=QtGui.QImage(int(self.pw),
                                    int(self.ph),
                                    QtGui.QImage.Format_RGB32)
        self.updatePreview()

    def updatePreview(self):
        pm=QtGui.QPixmap(self.pageImage)
        p=QtGui.QPainter(pm)
        pen = QtGui.QPen()
        pen.setWidth(1/self.scale) # Make it be 1px wide when scaled
        p.setPen(pen)

        # Draw white page
        p.setBrush(QtGui.QBrush(QtGui.QColor("white")))
        p.drawRect(-1,-1,pm.width()+2,pm.height()+2)


        for x in (self.gm,
                  self.gm+self.lm,
                  self.pw-self.rm):
            p.drawLine(x,0,x,pm.height())

        for y in (self.tm,
                  self.tm+self.ts,
                  self.ph-self.bm,
                  self.ph-self.bm-self.bs,
                  ):
            p.drawLine(0,y,pm.width(),y)

        p.end()
        self.ui.preview.setPixmap(pm.scaled(self.pw*self.scale,self.ph*self.scale))

        self.data["pageSetup"]= {
            "size": unicode(self.ui.size.currentText()).lower(),
            "width": unicode(self.ui.width.text()),
            "height": unicode(self.ui.height.text()),
            "margin-top": unicode(self.ui.margin_top.text()),
            "margin-bottom": unicode(self.ui.margin_bottom.text()),
            "margin-left": unicode(self.ui.margin_left.text()),
            "margin-right": unicode(self.ui.margin_right.text()),
            "margin-gutter": unicode(self.ui.margin_gutter.text()),
            "spacing-header": unicode(self.ui.spacing_header.text()),
            "spacing-footer": unicode(self.ui.spacing_footer.text()),
            "firstTemplate": unicode(self.ui.firstTemplate.currentText())
          }

        if self.data['pageSetup']['size']==u'custom':
            del(self.data['pageSetup']['size'])
        else:
            del(self.data['pageSetup']['width'])
            del(self.data['pageSetup']['height'])

        body=highlight(json.dumps(self.data, indent=2),
            JavascriptLexer(),HtmlFormatter())
        head=HtmlFormatter().get_style_defs('.highlight')
        self.ui.snippet.setHtml(
        '''<HEAD>
             <STYLE type="text/css">
             %s
             </STYLE>
           </HEAD>
           <BODY>
           %s
           </BODY>
        '''%(head,body))

# Widget to choose from system stylesheets
from Ui_stylesheets import Ui_Form as Ui_stylesheets

class StyleSheets(QtGui.QWidget):
    def __init__(self, stylesheet, data, preview, snippet, parent=None):
        QtGui.QWidget.__init__(self,parent)
        self.scale = .3
        self.data = data
        self.ui=Ui_stylesheets()
        self.ui.setupUi(self)
        self.stylesheet=stylesheet
        self.ui.preview=preview
        self.ui.snippet=snippet
        sheets=[]
        for folder in self.stylesheet.StyleSearchPath:
            sheets.extend(glob.glob(os.path.join(folder,'*.style')))
            sheets.extend(glob.glob(os.path.join(folder,'*.json')))
        sheets.sort()
        for s in sheets:
            self.ui.system.addItem(os.path.basename(s))
        self.applyChanges()

    def on_addFromFile_clicked(self, b = None):
        if b is None: return
        fname=QtGui.QFileDialog.getOpenFileName(self,
                                                'Open Stylesheet',
                                                os.getcwd(),
                                                'stylesheets (*.json *.style)'
                                                )
        if fname:
            self.ui.custom.addItem(fname)
        self.applyChanges()

    def on_addFromSystem_clicked(self, b = None):
        if b is None: return
        for i in self.ui.system.selectedItems():
            self.ui.custom.addItem(i.text())
            i.setSelected(False)
        self.applyChanges()

    def on_remove_clicked(self, b = None):
        if b is None: return
        i=self.ui.custom.currentItem()
        if not i:
            return
        self.ui.custom.takeItem(self.ui.custom.currentRow())
        self.applyChanges()

    def on_up_clicked(self, b = None):
        if b is None: return
        i=self.ui.custom.currentItem()
        if not i:
            return
        cr=self.ui.custom.currentRow()
        i=self.ui.custom.takeItem(cr)
        self.ui.custom.insertItem(cr-1,i)
        self.ui.custom.setCurrentItem(i)
        self.applyChanges()

    def on_down_clicked(self, b = None):
        if b is None: return
        i=self.ui.custom.currentItem()
        if not i:
            return
        cr=self.ui.custom.currentRow()
        i=self.ui.custom.takeItem(cr)
        self.ui.custom.insertItem(cr+1,i)
        self.ui.custom.setCurrentItem(i)
        self.applyChanges()

    def updatePreview(self):
        body=highlight(json.dumps(self.data, indent=2),
            JavascriptLexer(),HtmlFormatter())
        head=HtmlFormatter().get_style_defs('.highlight')
        self.ui.snippet.setHtml(
        '''<HEAD>
             <STYLE type="text/css">
             %s
             </STYLE>
           </HEAD>
           <BODY>
           %s
           </BODY>
        '''%(head,body))

    def applyChanges(self):
        self.data.update({'options':{'stylesheets':[unicode(self.ui.custom.item(x).text()) \
            for x in range(self.ui.custom.count())]}})
        self.updatePreview()

if __name__ == "__main__":
    main()

