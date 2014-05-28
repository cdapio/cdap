# -*- coding: utf-8 -*-

'''A completely generic, data-driven, configuration dialog'''

class ConfigDialog(QtGui.QDialog):
  def __init__(self, parent):
    QtGui.QDialog.__init__(self, parent)
    # Set up the UI from designer
    self.ui=UI_ConfigDialog()
    self.ui.setupUi(self)
    pages=[]
    sections=[]
    self.values={}

    for sectionName, options in config.options:
      # Create a page widget/layout for this section:
      page=QtGui.QScrollArea()
      innerpage=QtGui.QFrame()
      layout=QtGui.QGridLayout()
      row=-2
      for optionName, definition in options:
        row+=2
        if definition[0]=='bool':
          cb=QtGui.QCheckBox(optionName)
          cb.setChecked(config.getValue(sectionName, optionName, definition[1]))
          layout.addWidget(cb, row, 0, 1, 2)
          self.values[sectionName+'/'+optionName]=[cb, lambda(cb): cb.isChecked()]
        elif definition[0]=='int':
          label=QtGui.QLabel(optionName+":")
          label.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignVCenter)
          spin=QtGui.QSpinBox()
          if definition[3] is not None:
            spin.setMinimum(definition[3])
          else:
            spin.setMinimum(-99999)
          if definition[4] is not None:
            spin.setMaximum(definition[4])
          else:
            spin.setMaximum(99999)
          spin.setValue(config.getValue(sectionName, optionName, definition[1]))
          layout.addWidget(label, row, 0, 1, 1)
          layout.addWidget(spin, row, 1, 1, 1)
          self.values[sectionName+'/'+optionName]=[spin, lambda(spin): spin.value()]
          
        elif definition[0]=='string':
          label=QtGui.QLabel(optionName+":")
          label.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignVCenter)
          text=QtGui.QLineEdit()
          text.setText(unicode(config.getValue(sectionName, optionName, definition[1])))          
          layout.addWidget(label, row, 0, 1, 1)
          layout.addWidget(text, row, 1, 1, 1)
          self.values[sectionName+'/'+optionName]=[text, lambda(text): unicode(text.text())]

        elif definition[0]=='password':
          label=QtGui.QLabel(optionName+":")
          label.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignVCenter)
          text=QtGui.QLineEdit()
          text.setEchoMode(QtGui.QLineEdit.Password)
          text.setText(unicode(config.getValue(sectionName, optionName, definition[1])))          
          layout.addWidget(label, row, 0, 1, 1)
          layout.addWidget(text, row, 1, 1, 1)
          self.values[sectionName+'/'+optionName]=[text, lambda(text): unicode(text.text())]

        help=QtGui.QLabel(definition[2])
        help.setWordWrap(True)
        layout.addWidget(help, row, 2, 1, 1)
        separator=QtGui.QFrame()
        separator.setFrameStyle(QtGui.QFrame.HLine|QtGui.QFrame.Plain)
        layout.addWidget(separator, row+1, 0, 1, 3)
      innerpage.setLayout(layout)
      innerpage.adjustSize()
      page.resize(QtCore.QSize(innerpage.width()+5, page.height()))
      page.setWidget(innerpage)
      pages.append(page)
      sections.append(sectionName)

    for page, name in zip(pages,sections) :
      # Make a tab out of it
      self.ui.tabs.addTab(page, name)
    self.ui.tabs.setCurrentIndex(1)
    self.ui.tabs.removeTab(0)

    

  def accept(self):
    for k in self.values:
      sec, opt=k.split('/')
      widget, l = self.values[k]
      config.setValue(sec, opt, l(widget))
    QtGui.QDialog.accept(self)
