# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'searchwidget.ui'
#

#      by: PyQt4 UI code generator 4.5.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(369, 32)
        self.horizontalLayout = QtGui.QHBoxLayout(Form)
        self.horizontalLayout.setMargin(2)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.close = QtGui.QToolButton(Form)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(":/icons/close.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.close.setIcon(icon)
        self.close.setAutoRaise(True)
        self.close.setObjectName("close")
        self.horizontalLayout.addWidget(self.close)
        self.label = QtGui.QLabel(Form)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        self.text = QtGui.QLineEdit(Form)
        self.text.setObjectName("text")
        self.horizontalLayout.addWidget(self.text)
        self.previous = QtGui.QToolButton(Form)
        icon1 = QtGui.QIcon()
        icon1.addPixmap(QtGui.QPixmap(":/icons/previous.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.previous.setIcon(icon1)
        self.previous.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
        self.previous.setAutoRaise(True)
        self.previous.setObjectName("previous")
        self.horizontalLayout.addWidget(self.previous)
        self.next = QtGui.QToolButton(Form)
        icon2 = QtGui.QIcon()
        icon2.addPixmap(QtGui.QPixmap(":/icons/next.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.next.setIcon(icon2)
        self.next.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
        self.next.setAutoRaise(True)
        self.next.setObjectName("next")
        self.horizontalLayout.addWidget(self.next)
        self.matchCase = QtGui.QCheckBox(Form)
        self.matchCase.setObjectName("matchCase")
        self.horizontalLayout.addWidget(self.matchCase)
        self.label.setBuddy(self.text)

        self.retranslateUi(Form)
        QtCore.QObject.connect(self.text, QtCore.SIGNAL("returnPressed()"), self.next.animateClick)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(QtGui.QApplication.translate("Form", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.close.setText(QtGui.QApplication.translate("Form", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.close.setShortcut(QtGui.QApplication.translate("Form", "Esc", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("Form", "&Find:", None, QtGui.QApplication.UnicodeUTF8))
        self.previous.setText(QtGui.QApplication.translate("Form", "&Previous", None, QtGui.QApplication.UnicodeUTF8))
        self.next.setText(QtGui.QApplication.translate("Form", "&Next", None, QtGui.QApplication.UnicodeUTF8))
        self.matchCase.setText(QtGui.QApplication.translate("Form", "&Match Case", None, QtGui.QApplication.UnicodeUTF8))

import icons_rc

if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())

