
#!/bin/sh

for a in *.ui
do 
	pyuic4 $a -o Ui_`basename $a .ui`.py -x
 	sed -i 's/^# Created.*$//' Ui_`basename $a .ui`.py
done
pyrcc4 icons.qrc -o icons_rc.py
