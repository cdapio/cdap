# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'stylesheets.ui'
#

#      by: PyQt4 UI code generator 4.5.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(619, 451)
        self.horizontalLayout_2 = QtGui.QHBoxLayout(Form)
        self.horizontalLayout_2.setMargin(0)
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.verticalLayout_2 = QtGui.QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.label_2 = QtGui.QLabel(Form)
        self.label_2.setObjectName("label_2")
        self.verticalLayout_2.addWidget(self.label_2)
        self.system = QtGui.QListWidget(Form)
        self.system.setSelectionMode(QtGui.QAbstractItemView.MultiSelection)
        self.system.setTextElideMode(QtCore.Qt.ElideMiddle)
        self.system.setObjectName("system")
        self.verticalLayout_2.addWidget(self.system)
        self.horizontalLayout_2.addLayout(self.verticalLayout_2)
        self.addFromSystem = QtGui.QToolButton(Form)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(":/icons/next.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.addFromSystem.setIcon(icon)
        self.addFromSystem.setObjectName("addFromSystem")
        self.horizontalLayout_2.addWidget(self.addFromSystem)
        self.verticalLayout = QtGui.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.horizontalLayout = QtGui.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label = QtGui.QLabel(Form)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.addFromFile = QtGui.QToolButton(Form)
        icon1 = QtGui.QIcon()
        icon1.addPixmap(QtGui.QPixmap(":/icons/new.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.addFromFile.setIcon(icon1)
        self.addFromFile.setObjectName("addFromFile")
        self.horizontalLayout.addWidget(self.addFromFile)
        self.remove = QtGui.QToolButton(Form)
        icon2 = QtGui.QIcon()
        icon2.addPixmap(QtGui.QPixmap(":/icons/close.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.remove.setIcon(icon2)
        self.remove.setObjectName("remove")
        self.horizontalLayout.addWidget(self.remove)
        self.up = QtGui.QToolButton(Form)
        icon3 = QtGui.QIcon()
        icon3.addPixmap(QtGui.QPixmap(":/icons/up.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.up.setIcon(icon3)
        self.up.setObjectName("up")
        self.horizontalLayout.addWidget(self.up)
        self.down = QtGui.QToolButton(Form)
        icon4 = QtGui.QIcon()
        icon4.addPixmap(QtGui.QPixmap(":/icons/down.svg"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.down.setIcon(icon4)
        self.down.setObjectName("down")
        self.horizontalLayout.addWidget(self.down)
        self.verticalLayout.addLayout(self.horizontalLayout)
        self.custom = QtGui.QListWidget(Form)
        self.custom.setSelectionMode(QtGui.QAbstractItemView.SingleSelection)
        self.custom.setTextElideMode(QtCore.Qt.ElideMiddle)
        self.custom.setObjectName("custom")
        self.verticalLayout.addWidget(self.custom)
        self.horizontalLayout_2.addLayout(self.verticalLayout)

        self.retranslateUi(Form)
        QtCore.QObject.connect(self.system, QtCore.SIGNAL("itemSelectionChanged()"), Form.applyChanges)
        QtCore.QObject.connect(self.custom, QtCore.SIGNAL("itemSelectionChanged()"), Form.applyChanges)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(QtGui.QApplication.translate("Form", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.label_2.setText(QtGui.QApplication.translate("Form", "System StyleSheets:", None, QtGui.QApplication.UnicodeUTF8))
        self.addFromSystem.setText(QtGui.QApplication.translate("Form", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("Form", "Custom StyleSheets:", None, QtGui.QApplication.UnicodeUTF8))
        self.addFromFile.setToolTip(QtGui.QApplication.translate("Form", "Add another stylesheet", None, QtGui.QApplication.UnicodeUTF8))
        self.addFromFile.setText(QtGui.QApplication.translate("Form", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.remove.setToolTip(QtGui.QApplication.translate("Form", "Remove selected stylesheet", None, QtGui.QApplication.UnicodeUTF8))
        self.remove.setText(QtGui.QApplication.translate("Form", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.up.setText(QtGui.QApplication.translate("Form", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.down.setText(QtGui.QApplication.translate("Form", "...", None, QtGui.QApplication.UnicodeUTF8))

import icons_rc

if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())

