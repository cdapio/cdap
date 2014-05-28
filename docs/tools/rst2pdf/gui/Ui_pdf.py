# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'pdf.ui'
#

#      by: PyQt4 UI code generator 4.5.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(400, 300)
        self.verticalLayout = QtGui.QVBoxLayout(Form)
        self.verticalLayout.setMargin(0)
        self.verticalLayout.setObjectName("verticalLayout")
        self.scroll = QtGui.QScrollArea(Form)
        self.scroll.setFrameShape(QtGui.QFrame.NoFrame)
        self.scroll.setWidgetResizable(True)
        self.scroll.setObjectName("scroll")
        self.scrollAreaWidgetContents = QtGui.QWidget(self.scroll)
        self.scrollAreaWidgetContents.setGeometry(QtCore.QRect(0, 0, 400, 300))
        self.scrollAreaWidgetContents.setObjectName("scrollAreaWidgetContents")
        self.scroll.setWidget(self.scrollAreaWidgetContents)
        self.verticalLayout.addWidget(self.scroll)
        self.zoomin = QtGui.QAction(Form)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(":/icons/viewmag+.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.zoomin.setIcon(icon)
        self.zoomin.setObjectName("zoomin")
        self.zoomout = QtGui.QAction(Form)
        icon1 = QtGui.QIcon()
        icon1.addPixmap(QtGui.QPixmap(":/icons/viewmag-.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.zoomout.setIcon(icon1)
        self.zoomout.setObjectName("zoomout")
        self.next = QtGui.QAction(Form)
        icon2 = QtGui.QIcon()
        icon2.addPixmap(QtGui.QPixmap(":/icons/next.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.next.setIcon(icon2)
        self.next.setObjectName("next")
        self.previous = QtGui.QAction(Form)
        icon3 = QtGui.QIcon()
        icon3.addPixmap(QtGui.QPixmap(":/icons/previous.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.previous.setIcon(icon3)
        self.previous.setObjectName("previous")

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(QtGui.QApplication.translate("Form", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.zoomin.setText(QtGui.QApplication.translate("Form", "Zoom In", None, QtGui.QApplication.UnicodeUTF8))
        self.zoomout.setText(QtGui.QApplication.translate("Form", "Zoom Out", None, QtGui.QApplication.UnicodeUTF8))
        self.next.setText(QtGui.QApplication.translate("Form", "Next Page", None, QtGui.QApplication.UnicodeUTF8))
        self.previous.setText(QtGui.QApplication.translate("Form", "Previous Page", None, QtGui.QApplication.UnicodeUTF8))

import icons_rc

if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())

