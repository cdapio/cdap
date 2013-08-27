all: install

directories: /etc/beamer

/etc/beamer:
	mkdir -p /etc/beamer

install: directories
	cp bin/beamer bin/beamer-project-deployer bin/beamer-service /bin/
	cp -r conf/ /etc/beamer/
