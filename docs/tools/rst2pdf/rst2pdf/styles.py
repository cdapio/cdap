# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/styles.py $
#$Date: 2012-10-19 15:17:42 -0300 (Fri, 19 Oct 2012) $
#$Revision: 2531 $

import os
import sys
import re
from copy import copy
from types import *
from os.path import abspath, dirname, expanduser, join

import docutils.nodes

import reportlab
from reportlab.platypus import *
import reportlab.lib.colors as colors
import reportlab.lib.units as units
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.fonts import addMapping
from reportlab.lib.styles import *
from reportlab.lib.enums import *
from reportlab.pdfbase import pdfmetrics
import reportlab.lib.pagesizes as pagesizes
import reportlab.rl_config

from rst2pdf.rson import loads as rson_loads

import findfonts
from log import log

from opt_imports import ParagraphStyle, wordaxe, wordaxe_version

HAS_WORDAXE = wordaxe is not None

unit_separator = re.compile('(-?[0-9\.]*)')


class StyleSheet(object):
    '''Class to handle a collection of stylesheets'''

    @staticmethod
    def stylepairs(data):
        ''' Allows pairs of style information to be expressed
            in canonical reportlab list of two-item list/tuple,
            or in a more human-readable dictionary.
        '''
        styles = data.get('styles', {})
        try:
            stylenames = styles.keys()
        except AttributeError:
            for style in styles:
                yield style
            return

        # Traditional reportlab styles are in ordered (key, value)
        # tuples.  We also support dictionary lookup.  This is not
        # necessarily ordered.

        # The only problem with dictionary lookup is that
        # we need to insure that parents are processed before
        # their children.  This loop is a little ugly, but
        # gets the job done.

        while stylenames:
            name = stylenames.pop()
            parent = styles[name].get('parent')
            if parent not in stylenames:
                yield name, styles[name]
                continue
            names = [name]
            while parent in stylenames:
                stylenames.remove(parent)
                names.append(parent)
                parent = styles[names[-1]].get('parent')
            while names:
                name = names.pop()
                yield name, styles[name]

    def __init__(self, flist, font_path=None, style_path=None, def_dpi=300):
        log.info('Using stylesheets: %s' % ','.join(flist))
        # find base path
        if hasattr(sys, 'frozen'):
            self.PATH = abspath(dirname(sys.executable))
        else:
            self.PATH = abspath(dirname(__file__))

        # flist is a list of stylesheet filenames.
        # They will be loaded and merged in order.
        # but the two default stylesheets will always
        # be loaded first
        flist = [join(self.PATH, 'styles', 'styles.style'),
                join(self.PATH, 'styles', 'default.style')] + flist

        self.def_dpi=def_dpi
        if font_path is None:
            font_path=[]
        font_path+=['.', os.path.join(self.PATH, 'fonts')]
        self.FontSearchPath = map(os.path.expanduser, font_path)

        if style_path is None:
            style_path=[]
        style_path+=['.', os.path.join(self.PATH, 'styles'),
                      '~/.rst2pdf/styles']
        self.StyleSearchPath = map(os.path.expanduser, style_path)
        self.FontSearchPath=list(set(self.FontSearchPath))
        self.StyleSearchPath=list(set(self.StyleSearchPath))

        log.info('FontPath:%s'%self.FontSearchPath)
        log.info('StylePath:%s'%self.StyleSearchPath)

        findfonts.flist = self.FontSearchPath
        # Page width, height
        self.pw = 0
        self.ph = 0

        # Page size [w,h]
        self.ps = None

        # Margins (top,bottom,left,right,gutter)
        self.tm = 0
        self.bm = 0
        self.lm = 0
        self.rm = 0
        self.gm = 0

        #text width
        self.tw = 0

        # Default emsize, later it will be the fontSize of the base style
        self.emsize=10

        self.languages = []

        ssdata = self.readSheets(flist)

        # Get pageSetup data from all stylessheets in order:
        self.ps = pagesizes.A4
        self.page={}
        for data, ssname in ssdata:
            page = data.get('pageSetup', {})
            if page:
                self.page.update(page)
                pgs=page.get('size', None)
                if pgs: # A standard size
                    pgs=pgs.upper()
                    if pgs in pagesizes.__dict__:
                        self.ps = list(pagesizes.__dict__[pgs])
                        self.psname = pgs
                        if 'width' in self.page: del(self.page['width'])
                        if 'height' in self.page: del(self.page['height'])
                    elif pgs.endswith('-LANDSCAPE'):
                        self.psname = pgs.split('-')[0]
                        self.ps = list(pagesizes.landscape(pagesizes.__dict__[self.psname]))
                        if 'width' in self.page: del(self.page['width'])
                        if 'height' in self.page: del(self.page['height'])
                    else:
                        log.critical('Unknown page size %s in stylesheet %s'%\
                            (page['size'], ssname))
                        continue
                else: #A custom size
                    if 'size'in self.page:
                        del(self.page['size'])
                    # The sizes are expressed in some unit.
                    # For example, 2cm is 2 centimeters, and we need
                    # to do 2*cm (cm comes from reportlab.lib.units)
                    if 'width' in page:
                        self.ps[0] = self.adjustUnits(page['width'])
                    if 'height' in page:
                        self.ps[1] = self.adjustUnits(page['height'])
                self.pw, self.ph = self.ps
                if 'margin-left' in page:
                    self.lm = self.adjustUnits(page['margin-left'])
                if 'margin-right' in page:
                    self.rm = self.adjustUnits(page['margin-right'])
                if 'margin-top' in page:
                    self.tm = self.adjustUnits(page['margin-top'])
                if 'margin-bottom' in page:
                    self.bm = self.adjustUnits(page['margin-bottom'])
                if 'margin-gutter' in page:
                    self.gm = self.adjustUnits(page['margin-gutter'])
                if 'spacing-header' in page:
                    self.ts = self.adjustUnits(page['spacing-header'])
                if 'spacing-footer' in page:
                    self.bs = self.adjustUnits(page['spacing-footer'])
                if 'firstTemplate' in page:
                    self.firstTemplate = page['firstTemplate']

                # tw is the text width.
                # We need it to calculate header-footer height
                # and compress literal blocks.
                self.tw = self.pw - self.lm - self.rm - self.gm

        # Get page templates from all stylesheets
        self.pageTemplates = {}
        for data, ssname in ssdata:
            templates = data.get('pageTemplates', {})
            # templates is a dictionary of pageTemplates
            for key in templates:
                template = templates[key]
                # template is a dict.
                # template[´frames'] is a list of frames
                if key in self.pageTemplates:
                    self.pageTemplates[key].update(template)
                else:
                    self.pageTemplates[key] = template

        # Get font aliases from all stylesheets in order
        self.fontsAlias = {}
        for data, ssname in ssdata:
            self.fontsAlias.update(data.get('fontsAlias', {}))

        embedded_fontnames = []
        self.embedded = []
        # Embed all fonts indicated in all stylesheets
        for data, ssname in ssdata:
            embedded = data.get('embeddedFonts', [])

            for font in embedded:
                try:
                    # Just a font name, try to embed it
                    if isinstance(font, unicode):
                        # See if we can find the font
                        fname, pos = findfonts.guessFont(font)
                        if font in embedded_fontnames:
                            pass
                        else:
                            fontList = findfonts.autoEmbed(font)
                            if fontList:
                                embedded_fontnames.append(font)
                        if not fontList:
                            if (fname, pos) in embedded_fontnames:
                                fontList = None
                            else:
                                fontList = findfonts.autoEmbed(fname)
                        if fontList is not None:
                            self.embedded += fontList
                            # Maybe the font we got is not called
                            # the same as the one we gave
                            # so check that out
                            suff = ["", "-Oblique", "-Bold", "-BoldOblique"]
                            if not fontList[0].startswith(font):
                                # We need to create font aliases, and use them
                                for fname, aliasname in zip(
                                        fontList,
                                        [font + suffix for suffix in suff]):
                                    self.fontsAlias[aliasname] = fname
                        continue

                    # Each "font" is a list of four files, which will be
                    # used for regular / bold / italic / bold+italic
                    # versions of the font.
                    # If your font doesn't have one of them, just repeat
                    # the regular font.

                    # Example, using the Tuffy font from
                    # http://tulrich.com/fonts/
                    # "embeddedFonts" : [
                    #                    ["Tuffy.ttf",
                    #                     "Tuffy_Bold.ttf",
                    #                     "Tuffy_Italic.ttf",
                    #                     "Tuffy_Bold_Italic.ttf"]
                    #                   ],

                    # The fonts will be registered with the file name,
                    # minus the extension.

                    if font[0].lower().endswith('.ttf'): # A True Type font
                        for variant in font:
                            location=self.findFont(variant)
                            pdfmetrics.registerFont(
                                TTFont(str(variant.split('.')[0]),
                                location))
                            log.info('Registering font: %s from %s'%\
                                (str(variant.split('.')[0]),location))
                            self.embedded.append(str(variant.split('.')[0]))

                        # And map them all together
                        regular, bold, italic, bolditalic = [
                            variant.split('.')[0] for variant in font]
                        addMapping(regular, 0, 0, regular)
                        addMapping(regular, 0, 1, italic)
                        addMapping(regular, 1, 0, bold)
                        addMapping(regular, 1, 1, bolditalic)
                    else: # A Type 1 font
                        # For type 1 fonts we require
                        # [FontName,regular,italic,bold,bolditalic]
                        # where each variant is a (pfbfile,afmfile) pair.
                        # For example, for the URW palladio from TeX:
                        # ["Palatino",("uplr8a.pfb","uplr8a.afm"),
                        #             ("uplri8a.pfb","uplri8a.afm"),
                        #             ("uplb8a.pfb","uplb8a.afm"),
                        #             ("uplbi8a.pfb","uplbi8a.afm")]
                        faceName = font[0]
                        regular = pdfmetrics.EmbeddedType1Face(*font[1])
                        italic = pdfmetrics.EmbeddedType1Face(*font[2])
                        bold = pdfmetrics.EmbeddedType1Face(*font[3])
                        bolditalic = pdfmetrics.EmbeddedType1Face(*font[4])

                except Exception, e:
                    try:
                        if isinstance(font, list):
                            fname = font[0]
                        else:
                            fname = font
                        log.error("Error processing font %s: %s",
                            os.path.splitext(fname)[0], str(e))
                        log.error("Registering %s as Helvetica alias", fname)
                        self.fontsAlias[fname] = 'Helvetica'
                    except Exception, e:
                        log.critical("Error processing font %s: %s",
                            fname, str(e))
                        continue

        # Go though all styles in all stylesheets and find all fontNames.
        # Then decide what to do with them
        for data, ssname in ssdata:
            for [skey, style] in self.stylepairs(data):
                for key in style:
                    if key == 'fontName' or key.endswith('FontName'):
                        # It's an alias, replace it
                        if style[key] in self.fontsAlias:
                            style[key] = self.fontsAlias[style[key]]
                        # Embedded already, nothing to do
                        if style[key] in self.embedded:
                            continue
                        # Standard font, nothing to do
                        if style[key] in (
                                    "Courier",
                                    "Courier-Bold",
                                    "Courier-BoldOblique",
                                    "Courier-Oblique",
                                    "Helvetica",
                                    "Helvetica-Bold",
                                    "Helvetica-BoldOblique",
                                    "Helvetica-Oblique",
                                    "Symbol",
                                    "Times-Bold",
                                    "Times-BoldItalic",
                                    "Times-Italic",
                                    "Times-Roman",
                                    "ZapfDingbats"):
                            continue
                        # Now we need to do something
                        # See if we can find the font
                        fname, pos = findfonts.guessFont(style[key])

                        if style[key] in embedded_fontnames:
                            pass
                        else:
                            fontList = findfonts.autoEmbed(style[key])
                            if fontList:
                                embedded_fontnames.append(style[key])
                        if not fontList:
                            if (fname, pos) in embedded_fontnames:
                                fontList = None
                            else:
                                fontList = findfonts.autoEmbed(fname)
                            if fontList:
                                embedded_fontnames.append((fname, pos))
                        if fontList:
                            self.embedded += fontList
                            # Maybe the font we got is not called
                            # the same as the one we gave so check that out
                            suff = ["", "-Bold", "-Oblique", "-BoldOblique"]
                            if not fontList[0].startswith(style[key]):
                                # We need to create font aliases, and use them
                                basefname=style[key].split('-')[0]
                                for fname, aliasname in zip(
                                        fontList,
                                        [basefname + suffix for
                                        suffix in suff]):
                                    self.fontsAlias[aliasname] = fname
                                style[key] = self.fontsAlias[basefname +\
                                             suff[pos]]
                        else:
                            log.error("Unknown font: \"%s\","
                                      "replacing with Helvetica", style[key])
                            style[key] = "Helvetica"

        #log.info('FontList: %s'%self.embedded)
        #log.info('FontAlias: %s'%self.fontsAlias)
        # Get styles from all stylesheets in order
        self.stylesheet = {}
        self.styles = []
        self.linkColor = 'navy'
        # FIXME: linkColor should probably not be a global
        #        style, and tocColor should probably not
        #        be a special case, but for now I'm going
        #        with the flow...
        self.tocColor = None
        for data, ssname in ssdata:
            self.linkColor = data.get('linkColor') or self.linkColor
            self.tocColor = data.get('tocColor') or self.tocColor
            for [skey, style] in self.stylepairs(data):
                sdict = {}
                # FIXME: this is done completely backwards
                for key in style:
                    # Handle color references by name
                    if key == 'color' or key.endswith('Color') and style[key]:
                        style[key] = formatColor(style[key])

                    # Yet another workaround for the unicode bug in
                    # reportlab's toColor
                    elif key == 'commands':
                        style[key]=validateCommands(style[key])
                        #for command in style[key]:
                            #c=command[0].upper()
                            #if c=='ROWBACKGROUNDS':
                                #command[3]=[str(c) for c in command[3]]
                            #elif c in ['BOX','INNERGRID'] or c.startswith('LINE'):
                                #command[4]=str(command[4])

                    # Handle alignment constants
                    elif key == 'alignment':
                        style[key] = dict(TA_LEFT=0,
                                          LEFT=0,
                                          TA_CENTER=1,
                                          CENTER=1,
                                          TA_CENTRE=1,
                                          CENTRE=1,
                                          TA_RIGHT=2,
                                          RIGHT=2,
                                          TA_JUSTIFY=4,
                                          JUSTIFY=4,
                                          DECIMAL=8, )[style[key].upper()]

                    elif key == 'language':
                        if not style[key] in self.languages:
                            self.languages.append(style[key])

                    # Make keys str instead of unicode (required by reportlab)
                    sdict[str(key)] = style[key]
                    sdict['name'] = skey
                # If the style already exists, update it
                if skey in self.stylesheet:
                    self.stylesheet[skey].update(sdict)
                else: # New style
                    self.stylesheet[skey] = sdict
                    self.styles.append(sdict)

        # If the stylesheet has a style name docutils won't reach
        # make a copy with a sanitized name.
        # This may make name collisions possible but that should be
        # rare (who would have custom_name and custom-name in the
        # same stylesheet? ;-)
        # Issue 339

        styles2=[]
        for s in self.styles:
            if not re.match("^[a-z](-?[a-z0-9]+)*$", s['name']):
                s2 = copy(s)
                s2['name'] = docutils.nodes.make_id(s['name'])
                log.warning('%s is an invalid docutils class name, adding alias %s'%(s['name'], s2['name']))
                styles2.append(s2)
        self.styles.extend(styles2)

        # And create  reportlabs stylesheet
        self.StyleSheet = StyleSheet1()
        # Patch to make the code compatible with reportlab from SVN 2.4+ and
        # 2.4
        if not hasattr(self.StyleSheet, 'has_key'):
            self.StyleSheet.__class__.has_key = lambda s, k : k in s
        for s in self.styles:
            if 'parent' in s:
                if s['parent'] is None:
                    if s['name'] != 'base':
                        s['parent'] = self.StyleSheet['base']
                    else:
                        del(s['parent'])
                else:
                    s['parent'] = self.StyleSheet[s['parent']]
            else:
                if s['name'] != 'base':
                    s['parent'] = self.StyleSheet['base']

            # If the style has no bulletFontName but it has a fontName, set it
            if ('bulletFontName' not in s) and ('fontName' in s):
                s['bulletFontName'] = s['fontName']

            hasFS = True
            # Adjust fontsize units
            if 'fontSize' not in s:
                s['fontSize'] = s['parent'].fontSize
                s['trueFontSize']=None
                hasFS = False
            elif 'parent' in s:
                # This means you can set the fontSize to
                # "2cm" or to "150%" which will be calculated
                # relative to the parent style
                s['fontSize'] = self.adjustUnits(s['fontSize'],
                                    s['parent'].fontSize)
                s['trueFontSize']=s['fontSize']
            else:
                # If s has no parent, it's base, which has
                # an explicit point size by default and %
                # makes no sense, but guess it as % of 10pt
                s['fontSize'] = self.adjustUnits(s['fontSize'], 10)

            # If the leading is not set, but the size is, set it
            if 'leading' not in s and hasFS:
                s['leading'] = 1.2*s['fontSize']

            # If the bullet font size is not set, set it as fontSize
            if ('bulletFontSize' not in s) and ('fontSize' in s):
                s['bulletFontSize'] = s['fontSize']

            # If the borderPadding is a list and wordaxe <=0.3.2,
            # convert it to an integer. Workaround for Issue
            if 'borderPadding' in s and ((HAS_WORDAXE and \
                    wordaxe_version <='wordaxe 0.3.2') or
                    reportlab.Version < "2.3" )\
                    and isinstance(s['borderPadding'], list):
                log.warning('Using a borderPadding list in '\
                    'style %s with wordaxe <= 0.3.2 or Reportlab < 2.3. That is not '\
                    'supported, so it will probably look wrong'%s['name'])
                s['borderPadding']=s['borderPadding'][0]

            self.StyleSheet.add(ParagraphStyle(**s))


        self.emsize=self['base'].fontSize
        # Make stdFont the basefont, for Issue 65
        reportlab.rl_config.canvas_basefontname = self['base'].fontName
        # Make stdFont the default font for table cell styles (Issue 65)
        reportlab.platypus.tables.CellStyle.fontname=self['base'].fontName


    def __getitem__(self, key):

        # This 'normalizes' the key.
        # For example, if the key is todo_node (like sphinx uses), it will be
        # converted to 'todo-node' which is a valid docutils class name.

        if not re.match("^[a-z](-?[a-z0-9]+)*$", key):
            key = docutils.nodes.make_id(key)

        if self.StyleSheet.has_key(key):
            return self.StyleSheet[key]
        else:
            if key.startswith('pygments'):
                log.info("Using undefined style '%s'"
                            ", aliased to style 'code'."%key)
                newst=copy(self.StyleSheet['code'])
            else:
                log.warning("Using undefined style '%s'"
                            ", aliased to style 'normal'."%key)
                newst=copy(self.StyleSheet['normal'])
            newst.name=key
            self.StyleSheet.add(newst)
            return newst

    def readSheets(self, flist):
        ''' Read in the stylesheets.  Return a list of
            (sheetdata, sheetname) tuples.

            Orders included sheets in front
            of including sheets.
        '''
        # Process from end of flist
        flist.reverse()
        # Keep previously seen sheets in sheetdict
        sheetdict = {}
        result = []

        while flist:
            ssname = flist.pop()
            data = sheetdict.get(ssname)
            if data is None:
                data = self.readStyle(ssname)
                if data is None:
                    continue
                sheetdict[ssname] = data
                if 'options' in data and 'stylesheets' in data['options']:
                    flist.append(ssname)
                    newsheets = list(data['options']['stylesheets'])
                    newsheets.reverse()
                    flist.extend(newsheets)
                    continue
            result.append((data, ssname))
        return result

    def readStyle(self, ssname):
            # If callables are used, they should probably be subclassed
            # strings, or something else that will print nicely for errors
            if callable(ssname):
                return ssname()

            fname = self.findStyle(ssname)
            if fname:
                try:
                    return rson_loads(open(fname).read())
                except ValueError, e: # Error parsing the JSON data
                    log.critical('Error parsing stylesheet "%s": %s'%\
                        (fname, str(e)))
                except IOError, e: #Error opening the ssheet
                    log.critical('Error opening stylesheet "%s": %s'%\
                        (fname, str(e)))

    def findStyle(self, fn):
        """Find the absolute file name for a given style filename.

        Given a style filename, searches for it in StyleSearchPath
        and returns the real file name.

        """

        def innerFind(path, fn):
            if os.path.isabs(fn):
                if os.path.isfile(fn):
                    return fn
            else:
                for D in path:
                    tfn = os.path.join(D, fn)
                    if os.path.isfile(tfn):
                        return tfn
            return None
        for ext in ['', '.style', '.json']:
            result = innerFind(self.StyleSearchPath, fn+ext)
            if result:
                break
        if result is None:
            log.warning("Can't find stylesheet %s"%fn)
        return result

    def findFont(self, fn):
        """Find the absolute font name for a given font filename.

        Given a font filename, searches for it in FontSearchPath
        and returns the real file name.

        """
        if not os.path.isabs(fn):
            for D in self.FontSearchPath:
                tfn = os.path.join(D, fn)
                if os.path.isfile(tfn):
                    return str(tfn)
        return str(fn)

    def styleForNode(self, node):
        """Return the right default style for any kind of node.

        That usually means "bodytext", but for sidebars, for
        example, it's sidebar.

        """
        n= docutils.nodes
        styles={n.sidebar: 'sidebar',
                n.figure: 'figure',
                n.tgroup: 'table',
                n.table: 'table',
                n.Admonition: 'admonition'
        }

        return self[styles.get(node.__class__, 'bodytext')]

    def tstyleHead(self, rows=1):
        """Return a table style spec for a table header of `rows`.

        The style will be based on the table-heading style from the stylesheet.

        """
        # This alignment thing is exactly backwards from
        # the alignment for paragraphstyles
        alignment = {0: 'LEFT', 1: 'CENTER', 1: 'CENTRE', 2: 'RIGHT',
            4: 'JUSTIFY', 8: 'DECIMAL'}[self['table-heading'].alignment]
        return [
            ('BACKGROUND',
                (0, 0),
                (-1, rows - 1),
                self['table-heading'].backColor),
            ('ALIGN',
                (0, 0),
                (-1, rows - 1),
                alignment),
            ('TEXTCOLOR',
                (0, 0),
                (-1, rows - 1),
                self['table-heading'].textColor),
            ('FONT',
                (0, 0),
                (-1, rows - 1),
                self['table-heading'].fontName,
                self['table-heading'].fontSize,
                self['table-heading'].leading),
            ('VALIGN',
                (0, 0),
                (-1, rows - 1),
                self['table-heading'].valign)]

    def adjustFieldStyle(self):
        """Merges fieldname and fieldvalue styles into the field table style"""
        tstyle=self.tstyles['field']
        extras=self.pStyleToTStyle(self['fieldname'], 0, 0)+\
                self.pStyleToTStyle(self['fieldvalue'], 1, 0)
        for e in extras:
            tstyle.add(*e)
        return tstyle

    def pStyleToTStyle(self, style, x, y):
        """Return a table style similar to a given paragraph style.

        Given a reportlab paragraph style, returns a spec for a table style
        that adopts some of its features (for example, the background color).

        """
        results = []
        if style.backColor:
            results.append(('BACKGROUND', (x, y), (x, y), style.backColor))
        if style.borderWidth:
            bw = style.borderWidth
            del style.__dict__['borderWidth']
            if style.borderColor:
                bc = style.borderColor
                del style.__dict__['borderColor']
            else:
                bc = colors.black
            bc=str(bc)
            results.append(('BOX', (x, y), (x, y), bw, bc))
        if style.borderPadding:
            if isinstance(style.borderPadding, list):
                results.append(('TOPPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding[0]))
                results.append(('RIGHTPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding[1]))
                results.append(('BOTTOMPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding[2]))
                results.append(('LEFTPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding[3]))
            else:
                results.append(('TOPPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding))
                results.append(('RIGHTPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding))
                results.append(('BOTTOMPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding))
                results.append(('LEFTPADDING',
                    (x, y),
                    (x, y),
                    style.borderPadding))
        return results

    def adjustUnits(self, v, total=None, default_unit='pt'):
        if total is None:
            total = self.tw
        return adjustUnits(v, total,
                           self.def_dpi,
                           default_unit,
                           emsize=self.emsize)

    def combinedStyle(self, styles):
        '''Given a list of style names, it merges them (the existing ones)
        and returns a new style.

        The styles that don't exist are silently ignored.

        For example, if called with styles=['style1','style2'] the returned
        style will be called 'merged_style1_style2'.

        The styles that are *later* in the list will have priority.
        '''

        validst = [x for x in styles if self.StyleSheet.has_key(x)]
        newname = '_'.join(['merged']+validst)
        validst = [self[x] for x in validst]
        newst=copy(validst[0])

        for st in validst[1:]:
            newst.__dict__.update(st.__dict__)

        newst.name=newname
        return newst


def adjustUnits(v, total=None, dpi=300, default_unit='pt', emsize=10):
    """Takes something like 2cm and returns 2*cm.

    If you use % as a unit, it returns the percentage of "total".

    If total is not given, returns a percentage of the page width.
    However, if you get to that stage, you are doing it wrong.

    Example::

            >>> adjustUnits('50%',200)
            100

    """

    if v is None or v=="":
        return None

    v = str(v)
    l = re.split('(-?[0-9\.]*)', v)
    n=l[1]
    u=default_unit
    if len(l) == 3 and l[2]:
        u=l[2]
    if u in units.__dict__:
        return float(n) * units.__dict__[u]
    else:
        if u == '%':
            return float(n) * total/100
        elif u=='px':
            return float(n) * units.inch / dpi
        elif u=='pt':
            return float(n)
        elif u=='in':
            return float(n) * units.inch
        elif u=='em':
            return float(n) * emsize
        elif u=='ex':
            return float(n) * emsize /2
        elif u=='pc': # picas!
            return float(n) * 12
        log.error('Unknown unit "%s"' % u)
    return float(n)


def formatColor(value, numeric=True):
    """Convert a color like "gray" or "0xf" or "ffff"
    to something ReportLab will like."""
    if value in colors.__dict__:
        return colors.__dict__[value]
    else: # Hopefully, a hex color:
        c = value.strip()
        if c[0] == '#':
            c = c[1:]
        while len(c) < 6:
            c = '0' + c
        if numeric:
            r = int(c[:2], 16)/255.
            g = int(c[2:4], 16)/255.
            b = int(c[4:6], 16)/255.
            if len(c) >= 8:
                alpha = int(c[6:8], 16)/255.
                return colors.Color(r, g, b, alpha=alpha)
            return colors.Color(r, g, b)
        else:
            return str("#"+c)

# The values are:
# * Minimum number of arguments
# * Maximum number of arguments
# * Valid types of arguments.
#
# For example, if option FOO takes a list a string and a number,
# but the number is optional:
#
# "FOO":(2,3,"list","string","number")
#
# The reportlab command could look like
#
# ["FOO",(0,0),(-1,-1),[1,2],"whatever",4]
#
# THe (0,0) (-1,-1) are start and stop and are mandatory.
#
# Possible types of arguments are string, number, color, colorlist


validCommands={
        # Cell format commands
        "FONT":(1,3,"string","number","number"),
        "FONTNAME":(1,1,"string"),
        "FACE":(1,1,"string"),
        "FONTSIZE":(1,1,"number"),
        "SIZE":(1,1,"number"),
        "LEADING":(1,1,"number"),
        "TEXTCOLOR":(1,1,"color"),
        "ALIGNMENT":(1,1,"string"),
        "ALIGN":(1,1,"string"),
        "LEFTPADDING":(1,1,"number"),
        "RIGHTPADDING":(1,1,"number"),
        "TOPPADDING":(1,1,"number"),
        "BOTTOMPADDING":(1,1,"number"),
        "BACKGROUND":(1,1,"color"),
        "ROWBACKGROUNDS":(1,1,"colorlist"),
        "COLBACKGROUNDS":(1,1,"colorlist"),
        "VALIGN":(1,1,"string"),
        # Line commands
        "GRID":(2,2,"number","color"),
        "BOX":(2,2,"number","color"),
        "OUTLINE":(2,2,"number","color"),
        "INNERGRID":(2,2,"number","color"),
        "LINEBELOW":(2,2,"number","color"),
        "LINEABOVE":(2,2,"number","color"),
        "LINEBEFORE":(2,2,"number","color"),
        "LINEAFTER":(2,2,"number","color"),
        # You should NOT have span commands, man!
        #"SPAN":(,,),
    }

def validateCommands(commands):
    '''Given a list of reportlab's table commands, it fixes some common errors
    and/or removes commands that can't be fixed'''

    fixed=[]

    for command in commands:
        command[0]=command[0].upper()
        flag=False
        # See if the command is valid
        if command[0] not in validCommands:
            log.error('Unknown table command %s in stylesheet',command[0])
            continue

        # See if start and stop are the right types
        if type(command[1]) not in (ListType,TupleType):
            log.error('Start cell in table command should be list or tuple, got %s [%s]',type(command[1]),command[1])
            flag=True

        if type(command[2]) not in (ListType,TupleType):
            log.error('Stop cell in table command should be list or tuple, got %s [%s]',type(command[1]),command[1])
            flag=True

        # See if the number of arguments is right
        l=len(command)-3
        if l>validCommands[command[0]][1]:
            log.error('Too many arguments in table command: %s',command)
            flag=True

        if l<validCommands[command[0]][0]:
            log.error('Too few arguments in table command: %s',command)
            flag=True

        # Validate argument types
        for pos,arg in enumerate(command[3:]):
            typ = validCommands[command[0]][pos+2]
            if typ == "color":
                # Convert all 'string' colors to numeric
                command[3+pos]=formatColor(arg)
            elif typ == "colorlist":
                command[3+pos]=[ formatColor(c) for c in arg]
            elif typ == "number":
                pass
            elif typ == "string":
                # Force string, not unicode
                command[3+pos]=str(arg)
            else:
                log.error("This should never happen: wrong type %s",typ)

        if not flag:
            fixed.append(command)

    return fixed

class CallableStyleSheet(str):
    ''' Useful for programmatically generated stylesheets.
        A generated stylesheet is a callable string (name),
        which returns the pre-digested stylesheet data
        when called.
    '''
    def __new__(cls, name, value=''):
        self = str.__new__(cls, name)
        self.value = value
        return self
    def __call__(self):
        return rson_loads(self.value)

