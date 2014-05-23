# -*- coding: utf-8 -*-

# An extension module for rst2pdf
# Copyright 2010, Patrick Maupin
# See LICENSE.txt for licensing terms

'''
preprocess is a rst2pdf extension module (invoked by -e preprocess
on the rst2pdf command line.

There is a testcase for this file at rst2pdf/tests/test_preprocess.txt

This preprocesses the source text file before handing it to docutils.

This module serves two purposes:

1) It demonstrates the technique and can be a starting point for similar
   user-written processing modules; and

2) It provides a simplified syntax for documents which are targeted only
   at rst2pdf, rather than docutils in general.

The design goal of "base rst2pdf" is to be completely compatible with
docutils, such that a file which works as a PDF can also work as HTML,
etc.

Unfortunately, base docutils is a slow-moving target, and does not
make this easy.  For example, SVG images do not work properly with
the HTML backend unless you install a patch, and docutils has no
concept of page breaks or additional vertical space (other than
the <hr>).

So, while it would be nice to have documents that render perfectly
with any backend, this goal is hard to achieve for some documents,
and once you are restricted to a particular transformation type,
then you might as well have a slightly nicer syntax for your source
document.

-----------------------------------------------------------------

Preprocessor extensions:

All current extensions except style occupy a single line in the
source file.

``.. include::``

    Processes the include file as well.  An include file may
    either be a restructured text file, OR may be an RSON or
    JSON stylesheet.  The determination is made by trying to
    parse it as RSON.  If it passes, it is a stylesheet; if not,
    well, we'll let the docutils parser have its way with it.

``.. page::``

    Is translated into a raw PageBreak.

``.. space::``

    Is translated into a raw Spacer.  If only one number given, is
    used for vertical space.  This is the canonical use case, since
    horizontal space is ignored anyway!

``.. style::``

    Allows you to create in-line stylesheets.  As with other
    restructured text components, the stylesheet data must
    be indented.  Stylesheets are in RSON or JSON.

``.. widths::``

    creates a new table style (based on table or the first
    non-numeric token) and creates a class using that style
    specifically for the next table in the document. (Creates
    a .. class::, so you must specify .. widths:: immediately
    before the table it applies to.   Allows you to set the
    widths for the table, using percentages.

``SingleWordAtLeftColumn``

    If a single word at the left column is surrounded by
    blank lines, the singleword style is automatically applied to
    the word.  This is a workaround for the broken interaction
    between docutils subtitles and bibliographic metadata.  (I
    found that docutils was referencing my subtitles from inside
    the TOC, and that seemed silly.  Perhaps there is a better
    workaround at a lower level in rst2pdf.)

-----------------------------------------------------------------

Preprocessor operation:

The preprocessor generates a file that has the same name as the source
file, with .build_temp. embedded in the name, and then passes that
file to the restructured text parser.

This file is left on the disk after operation, because any error
messages from docutils will refer to line numbers in it, rather than
in the original source, so debugging could be difficult if the
file were automatically removed.

'''

import os
import re

from rst2pdf.rson import loads as rson_loads

from rst2pdf.log import log

class DummyFile(str):
    ''' We could use stringio, but that's really overkill for what
        we need here.
    '''
    def read(self):
        return self

class Preprocess(object):
    def __init__(self, sourcef, incfile=False, widthcount=0):
        ''' Process a file and decorate the resultant Preprocess instance with
            self.result (the preprocessed file) and self.styles (extracted stylesheet
            information) for the caller.
        '''
        self.widthcount = widthcount

        name = sourcef.name
        source = sourcef.read().replace('\r\n', '\n').replace('\r', '\n')

        # Make the determination if an include file is a stylesheet or
        # another restructured text file, and handle stylesheets appropriately.

        if incfile:
            try:
                self.styles = styles = rson_loads(source)
                substyles = styles.get('styles')
                if substyles is not None:
                    styles['styles'] = dict(substyles)
            except:
                pass
            else:
                self.changed = True
                self.keep = False
                return

        # Read the whole file and wrap it in a DummyFile
        self.sourcef = DummyFile(source)
        self.sourcef.name = name

        # Use a regular expression on the source, to take it apart
        # and put it back together again.

        self.source = source = [x for x in self.splitter(source) if x]
        self.result = result = []
        self.styles = {}
        self.changed = False

        # More efficient to pop() a list than to keep taking tokens from [0]
        source.reverse()
        isblank = False
        keywords = self.keywords
        handle_single = keywords['single::']
        while source:
            wasblank = isblank
            isblank = False
            chunk = source.pop()
            result.append(chunk)

            # Only process single lines
            if not chunk.endswith('\n'):
                continue
            result[-1] = chunk[:-1]
            if chunk.index('\n') != len(chunk)-1:
                continue

            # Parse the line to look for one of our keywords.
            tokens = chunk.split()
            isblank = not tokens
            if len(tokens) >= 2 and tokens[0] == '..' and tokens[1].endswith('::'):
                func = keywords.get(tokens[1])
                if func is None:
                    continue
                chunk = chunk.split('::', 1)[1]
            elif wasblank and len(tokens) == 1 and chunk[0].isalpha() and tokens[0].isalpha():
                func = handle_single
                chunk = tokens[0]
            else:
                continue

            result.pop()
            func(self, chunk.strip())

        # Determine if we actually did anything or not.  Just use our source file
        # if not.  Otherwise, write the results to disk (so the user can use them
        # for debugging) and return them.
        if self.changed:
            result.append('')
            result = DummyFile('\n'.join(result))
            result.name = name + '.build_temp'
            self.keep = keep = len(result.strip())
            if keep:
                f = open(result.name, 'wb')
                f.write(result)
                f.close()
            self.result = result
        else:
            self.result = self.sourcef

    def handle_include(self, fname):
        # Ugly, violates DRY, etc., but I'm not about to go
        # figure out how to re-use docutils include file
        # path processing!

        for prefix in ('', os.path.dirname(self.sourcef.name)):
            try:
                f = open(os.path.join(prefix, fname), 'rb')
            except IOError:
                continue
            else:
                break
        else:
            log.error("Could not find include file %s", fname)
            self.changed = True
            return

        # Recursively call this class to process include files.
        # Extract all the information from the included file.

        inc = Preprocess(f, True, self.widthcount)
        self.widthcount = inc.widthcount
        if 'styles' in self.styles and 'styles' in inc.styles:
            self.styles['styles'].update(inc.styles.pop('styles'))
        self.styles.update(inc.styles)
        if inc.changed:
            self.changed = True
            if not inc.keep:
                return
            fname = inc.result.name
        self.result.extend(['', '', '.. include:: ' + fname, ''])

    def handle_single(self, word):
        ''' Prepend the singleword class in front of the word.
        '''
        self.changed = True
        self.result.extend(['', '', '.. class:: singleword', '', word, ''])

    def handle_page(self, chunk):
        ''' Insert a raw pagebreak
        '''
        self.changed = True
        self.result.extend(['', '', '.. raw:: pdf', '',
                    '    PageBreak ' + chunk, ''])

    def handle_space(self, chunk):
        ''' Insert a raw space
        '''
        self.changed = True
        if len(chunk.replace(',', ' ').split()) == 1:
            chunk = '0 ' + chunk
        self.result.extend(['', '', '.. raw:: pdf', '',
                    '    Spacer ' + chunk, ''])

    def handle_widths(self, chunk):
        ''' Insert a unique style in the stylesheet, and reference it
            from a .. class:: comment.
        '''
        self.changed = True
        chunk = chunk.replace(',', ' ').replace('%', ' ').split()
        if not chunk:
            log.error('no widths specified in .. widths ::')
            return
        parent = chunk[0][0].isalpha() and chunk.pop(0) or 'table'
        values = [float(x) for x in chunk]
        total = sum(values)
        values = [int(round(100 * x / total)) for x in values]
        while 1:
            total = sum(values)
            if total > 100:
                values[values.index(max(values))] -= 1
            elif total < 100:
                values[values.index(max(values))] += 1
            else:
                break

        values = ['%s%%' % x for x in values]
        self.widthcount += 1
        stylename = 'embeddedtablewidth%d' % self.widthcount
        self.styles.setdefault('styles', {})[stylename] = dict(parent=parent, colWidths=values)
        self.result.extend(['', '', '.. class:: ' + stylename, ''])

    def handle_style(self, chunk):
        ''' Parse through the source until we find lines that are no longer indented,
            then pass our indented lines to the RSON parser.
        '''
        self.changed = True
        if chunk:
            log.error(".. style:: does not recognize string %s" % repr(chunk))
            return

        mystyles = '\n'.join(self.read_indented())
        if not mystyles:
            log.error("Empty .. style:: block found")
        try:
            styles = rson_loads(mystyles)
        except ValueError, e: # Error parsing the JSON data
                log.critical('Error parsing stylesheet "%s": %s'%\
                    (mystyles, str(e)))
        else:
            self.styles.setdefault('styles', {}).update(styles)

    def read_indented(self):
        ''' Read data from source while it is indented (or blank).
            Stop on the first non-indented line, and leave the rest
            on the source.
        '''
        source = self.source
        data = None
        while source and not data:
            data = source and source.pop().splitlines() or []
            data.reverse()
            while data:
                line = data.pop().rstrip()
                if not line or line.lstrip() != line:
                    yield line
                    continue
                data.append(line)
                break
        data.reverse()
        data.append('')
        source.append('\n'.join(data))
        source.append('\n')

    # Automatically generate our keywords from methods prefixed with 'handle_'
    keywords = list(x[7:] for x in vars() if x.startswith('handle_'))

    # Generate the regular expression for parsing, and a split function using it.
    blankline  = r'^([ \t]*\n)'
    singleword = r'^([A-Za-z]+[ \t]*\n)(?=[ \t]*\n)'
    comment = r'^(\.\.[ \t]+(?:%s)\:\:.*\n)' % '|'.join(keywords)
    expression = '(?:%s)' % '|'.join([blankline, singleword, comment])
    splitter = re.compile(expression, re.MULTILINE).split

    # Once we have used the keywords in our regular expression,
    # fix them up for use by the parser.
    keywords = dict([(x + '::', vars()['handle_' + x]) for x in keywords])

class MyStyles(str):
    ''' This class conforms to the styles.py processing requirements
        for a stylesheet that is not really a file.  It must be callable(),
        and str(x) must return the name of the stylesheet.
    '''
    def __new__(cls, styles):
        self = str.__new__(cls, 'Embedded Preprocess Styles')
        self.data = styles
        return self
    def __call__(self):
        return self.data

def install(createpdf, options):
    ''' This is where we intercept the document conversion.
        Preprocess the restructured text, and insert our
        new styles (if any).
    '''
    data = Preprocess(options.infile)
    options.infile = data.result
    if data.styles:
        options.style.append(MyStyles(data.styles))
