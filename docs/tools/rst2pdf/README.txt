Intro
=====

The usual way of creating PDF from reStructuredText is by going through LaTeX. 
This tool provides an alternative by producing PDF directly using the ReportLab
library. 

Installing
==========

python setup.py install

should do the trick.

Features
========

* User-defined page layout. Multiple frames per page, multiple layouts per
  document. 

* Page transitions 

* Cascading stylesheet mechanism, define only what you want changed. 

* Supports TTF and Type1 font embedding. 

* Any number of paragraph styles using the class directive. 

* Any number of character styles using text roles. 

* Custom page sizes and margins. 

* Syntax highlighter for many languages, using Pygments. 

* Supports embedding almost any kind of raster or vector images. 

* Supports hyphenation and kerning (using wordaxe). 

* Full user's manual
