import sys

from PyQt4 import QtCore, QtGui
import QtPoppler


class PDFDisplay(QtGui.QWidget):
    def __init__(self, doc):
        QtGui.QWidget.__init__(self, None)
        self.doc = doc
        self.pdfImage = None
        self.currentPage = 0
        self.display()

    def paintEvent(self, event):
        painter = QtGui.QPainter(self)
        if self.pdfImage is not None:
            painter.drawImage(0, 0, self.pdfImage)
        else:
            print "No Pixmap"

    def keyPressEvent(self, event):
        if event.key() == QtCore.Qt.Key_Down:
            if self.currentPage + 1 < self.doc.numPages():
                self.currentPage += 1
                self.display()
        elif event.key() == QtCore.Qt.Key_Up:
            if self.currentPage > 0:
                self.currentPage -= 1
                self.display()
        elif (event.key() == QtCore.Qt.Key_F):
            r = QtCore.QRectF()
            print self.doc.page(self.currentPage).search(QtCore.QString("Dinamis"), r, QtPoppler.Poppler.Page.FromTop, QtPoppler.Poppler.Page.CaseSensitive)
            print r
        elif (event.key() == QtCore.Qt.Key_Q):
            sys.exit(0)
    
    def display(self):
        if self.doc is not None:
            page = self.doc.page(self.currentPage)
            if page:
                self.pdfImage = None
                self.pdfImage = page.renderToImage()
                self.update()
                #delete page;
        else:
            print "doc not loaded"


if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    d = QtPoppler.Poppler.Document.load(sys.argv[1])
    d.setRenderHint(QtPoppler.Poppler.Document.Antialiasing and QtPoppler.Poppler.Document.TextAntialiasing)
    disp = PDFDisplay(d)
    disp.show()
    sys.exit(app.exec_())