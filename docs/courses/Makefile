# Makefile for Reactor Training Course

# You can set these variables from the command line
BUILD_DIR      = build
HTML_DIR       = html
PDF_DIR        = pdf
EXAMPLE_DIR    = example
SOURCE_DIR     = source
STAGE_SERVER   = stg-web101.sw.joyent.continuuity.net
SPHINXBUILD    = sphinx-build
LANDSLIDE      = landslide
RST2PDF        = rst2pdf

# User-friendly checks for software installs
# sphinx-build
ifeq ($(shell which $(SPHINXBUILD) >/dev/null 2>&1; echo $$?), 1)
$(error The '$(SPHINXBUILD)' command was not found. Make sure you have Sphinx installed, then set the SPHINXBUILD environment variable to point to the full path of the '$(SPHINXBUILD)' executable. Alternatively you can add the directory with the executable to your PATH. If you don't have Sphinx installed, grab it from http://sphinx-doc.org/)
endif
# landslide
ifeq ($(shell which $(LANDSLIDE) >/dev/null 2>&1; echo $$?), 1)
$(error The '$(LANDSLIDE)' command was not found. Make sure you have $(LANDSLIDE) installed. If you don't have $(LANDSLIDE) installed, install it from reactor/tools/$(LANDSLIDE)/)
endif
# rst2pdf
ifeq ($(shell which $(RST2PDF) >/dev/null 2>&1; echo $$?), 1)
$(error The '$(RST2PDF)' command was not found. Make sure you have $(RST2PDF) installed. If you don't have $(RST2PDF) installed, install it from reactor/tools/$(RST2PDF)/)
endif

# Internal variables
BUILD_HTML_DIR    = $(BUILD_DIR)/$(HTML_DIR)
BUILD_PDF_DIR     = $(BUILD_DIR)/$(PDF_DIR)
BUILD_EXAMPLE_DIR = $(BUILD_DIR)/$(EXAMPLE_DIR)
USER             := $(shell whoami)


.PHONY: help clean dist html pdf zip

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean      delete all build results"
	@echo "  dist       to make HTML files, copy index file and package them into a zip"
	@echo "  html       to make HTML files (not completed, merely stages in build dir)"
	@echo "  pdf        to make PDF files (not completed, merely stages in build dir)"
	@echo "  upload     upload zips to $(USER)@$(STAGE_SERVER)"
	@echo "  zip        to zip files into an archive"

_dist:
	cp $(SOURCE_DIR)/index.html $(BUILD_HTML_DIR)

# Doesn't actual create HTML, copies files instead
_html: _makedirs
	cp -R $(HTML_DIR) $(BUILD_DIR)

_makedirs:
	mkdir -p $(BUILD_DIR)
	
# Doesn't actual create PDFs, copies files instead
_pdf: _makedirs
	cp -R $(PDF_DIR) $(BUILD_DIR)

_upload:
	rsync -vz $(BUILD_HTML_DIR).zip $(USER)@$(STAGE_SERVER):$(HTML_DIR).zip
	rsync -vz $(BUILD_PDF_DIR).zip $(USER)@$(STAGE_SERVER):$(PDF_DIR).zip

_example: _makedirs
	cp -R $(SOURCE_DIR)/$(EXAMPLE_DIR) $(BUILD_DIR)

_zip_example:
	cd $(BUILD_DIR) && \
	zip -r example example/*

_zip_html:
	cd $(BUILD_DIR) && \
	zip -j html html/*

_zip_pdf:
	cd $(BUILD_DIR) && \
	zip -j pdf pdf/*

clean:
	rm -rf $(BUILD_DIR)/*
	
dist: _html _pdf _example _dist zip
	@echo "Dist finished."
	@echo

html: _html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILD_HTML_DIR)."
	@echo

example: _example
	@echo
	@echo "Build finished. The example files are in $(BUILD_EXAMPLE_DIR)."
	@echo

pdf: _pdf
	@echo
	@echo "Build finished. The PDF files are in $(BUILD_PDF_DIR)."
	@echo

zip: _zip_html _zip_pdf _zip_example
	@echo
	@echo "The files are archived in $(BUILD_DIR)."
	@echo

upload: _upload
	@echo
	@echo "Upload complete."
	@echo
