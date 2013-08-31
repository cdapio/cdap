all: package

beamer.tgz:
	tar zcvf beamer.tgz beamer/
	
tgz: beamer.tgz

package: tgz
	cat beamer-install.template.sh > beamer-install.sh
	uuencode beamer.tgz beamer.tgz >> beamer-install.sh

clean:
	rm -f beamer.tgz beamer-install.sh
