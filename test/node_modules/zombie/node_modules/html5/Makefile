all: doc

PROJECT = html5

doc/index.html:
	ln -s api.html doc/index.html

doc/api.html: doc/api.markdown doc/api_header.html doc/api_footer.html
	node tools/ronnjs/bin/ronn.js --fragment doc/api.markdown \
	| sed "s/<h2>\(.*\)<\/h2>/<h2 id=\"\1\">\1<\/h2>/g" \
	| cat doc/api_header.html - doc/api_footer.html > $@

doc/node-${PROJECT}.3: doc/api.markdown
	node tools/ronnjs/bin/ronn.js --roff doc/api.markdown > $@

doc: doc/node-${PROJECT}.3 doc/api.html doc/index.html
