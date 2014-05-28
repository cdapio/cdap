#!/usr/bin/env python
# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms

"""
Scan a list of folders and find all .afm files,
then create rst2pdf-ready font-aliases.
"""

import os
import sys

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont, TTFontFile, TTFError, FF_FORCEBOLD, FF_ITALIC
from reportlab.lib.fonts import addMapping

from log import log

flist = []
afmList = []
pfbList = {}
ttfList = []

# Aliases defined by GhostScript, so if you use Palatino or whatever you
# may get **something**. They are family name aliases.
Alias = {
    'itc bookman': 'urw bookman l',
    'itc avant garde gothic': 'urw gothic l',
    'palatino': 'urw palladio l',
    'new century schoolbook': 'century schoolbook l',
    'itc zapf chancery': 'urw chancery l'}

# Standard PDF fonts, so no need to embed them
Ignored = ['times', 'itc zapf dingbats', 'symbol', 'helvetica', 'courier']


fonts = {}
families = {}
fontMappings = {}


def loadFonts():
    """
    Search the system and build lists of available fonts.
    """
    
    if not afmList and not pfbList and not ttfList:
        # Find all ".afm" and ".pfb" files files
        def findFontFiles(_, folder, names):
            for f in os.listdir(folder):
                ext=os.path.splitext(f)[-1]
                if ext in ['.ttf','.ttc']:
                    ttfList.append(os.path.join(folder, f))
                if ext=='.afm':
                    afmList.append(os.path.join(folder, f))
                if ext=='.pfb':
                    pfbList[f[:-4]] = os.path.join(folder, f)

        for folder in flist:
            os.path.walk(folder, findFontFiles, None)

        for ttf in ttfList:
            '''Find out how to process these'''
            try:
                font = TTFontFile(ttf)
            except TTFError:
                continue
            
            #print ttf, font.name, font.fullName, font.styleName, font.familyName
            family=font.familyName.lower()
            fontName=font.name
            baseName = os.path.basename(ttf)[:-4]
            fullName=font.fullName
            
            fonts[fontName.lower()] = (ttf, ttf, family)
            fonts[fullName.lower()] = (ttf, ttf, family)
            fonts[fullName.lower().replace('italic','oblique')] = (ttf, ttf, family)
            bold = (FF_FORCEBOLD == FF_FORCEBOLD & font.flags)
            italic = (FF_ITALIC == FF_ITALIC & font.flags)

            # And we can try to build/fill the family mapping
            if family not in families:
                families[family] = [fontName, fontName, fontName, fontName]
            if bold and italic:
                families[family][3] = fontName
            elif bold:
                families[family][1] = fontName
            elif italic:
                families[family][2] = fontName
            # FIXME: what happens if there are Demi and Medium
            # weights? We get a random one.
            else:
                families[family][0] = fontName
                
        # Now we have full afm and pbf lists, process the
        # afm list to figure out family name, weight and if
        # it's italic or not, as well as where the
        # matching pfb file is

        for afm in afmList:
            family = None
            fontName = None
            italic = False
            bold = False
            for line in open(afm, 'r'):
                line = line.strip()
                if line.startswith('StartCharMetrics'):
                    break
                elif line.startswith('FamilyName'):
                    family = ' '.join(line.split(' ')[1:]).lower()
                elif line.startswith('FontName'):
                    fontName = line.split(' ')[1]
                # TODO: find a way to alias the fullname to this font
                # so you can use names like "Bitstream Charter Italic"
                elif line.startswith('FullName'):
                    fullName = ' '.join(line.split(' ')[1:])
                elif line.startswith('Weight'):
                    w = line.split(' ')[1]
                    if w == 'Bold':
                        bold = True
                elif line.startswith('ItalicAngle'):
                    if line.split(' ')[1] != '0.0':
                        italic = True

            baseName = os.path.basename(afm)[:-4]
            if family in Ignored:
                continue
            if family in Alias:
                continue
            if baseName not in pfbList:
                log.info("afm file without matching pfb file: %s"% baseName)
                continue

            # So now we have a font we know we can embed.
            fonts[fontName.lower()] = (afm, pfbList[baseName], family)
            fonts[fullName.lower()] = (afm, pfbList[baseName], family)
            fonts[fullName.lower().replace('italic','oblique')] = (afm, pfbList[baseName], family)

            # And we can try to build/fill the family mapping
            if family not in families:
                families[family] = [fontName, fontName, fontName, fontName]
            if bold and italic:
                families[family][3] = fontName
            elif bold:
                families[family][1] = fontName
            elif italic:
                families[family][2] = fontName
            # FIXME: what happens if there are Demi and Medium
            # weights? We get a random one.
            else:
                families[family][0] = fontName

def findFont(fname):
    loadFonts()
    # So now we are sure we know the families and font
    # names. Well, return some data!
    fname=fname.lower()
    if fname in fonts:
        font = fonts[fname.lower()]
    else:
        if fname in Alias:
            fname = Alias[fname]
        if fname in families:
            font = fonts[families[fname][0].lower()]
        else:
            return None
    return font

def findTTFont(fname):

    def get_family(query):
        data = os.popen("fc-match \"%s\""%query, "r").read()
        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            fname, family, _, variant = line.split('"')[:4]
            family = family.replace('"', '')
            if family:
                return family
        return None

    def get_fname(query):
        data = os.popen("fc-match -v \"%s\""%query, "r").read()
        for line in data.splitlines():
            line = line.strip()
            if line.startswith("file: "):
                return line.split('"')[1]
        return None

    def get_variants(family):
        variants = [
            get_fname(family + ":style=Roman"),
            get_fname(family + ":style=Bold"),
            get_fname(family + ":style=Oblique"),
            get_fname(family + ":style=Bold Oblique")]
        if variants[2] == variants[0]:
            variants[2] = get_fname(family + ":style=Italic")
        if variants[3] == variants[0]:
            variants[3] = get_fname(family + ":style=Bold Italic")
        if variants[0].endswith('.pfb') or variants[0].endswith('.gz'):
            return None
        return variants

    if os.name != 'nt':
        family = get_family(fname)
        if not family:
            log.error("Unknown font: %s", fname)
            return None
        return get_variants(family)
    else:
        # lookup required font in registry lookup, alternative approach
        # is to let loadFont() traverse windows font directory or use
        # ctypes with EnumFontFamiliesEx

        def get_nt_fname(ftname):
            import _winreg as _w
            fontkey = _w.OpenKey(_w.HKEY_LOCAL_MACHINE,
                "SOFTWARE\Microsoft\Windows NT\CurrentVersion\Fonts")
            fontname = ftname + " (TrueType)"
            try:
                fname = _w.QueryValueEx(fontkey, fontname)[0]
                if os.path.isabs(fname):
                    fontkey.close()
                    return fname
                fontdir = os.environ.get("SystemRoot", u"C:\\Windows")
                fontdir += u"\\Fonts"
                fontkey.Close()
                return fontdir + "\\" + fname
            except WindowsError, err:
                fontkey.Close()
                return None

        family, pos = guessFont(fname)
        fontfile = get_nt_fname(fname)
        if not fontfile:
            if pos == 0:
                fontfile = get_nt_fname(family)
            elif pos == 1:
                fontfile = get_nt_fname(family + " Bold")
            elif pos == 2:
                fontfile = get_nt_fname(family + " Italic") or \
                    get_nt_fname(family + " Oblique")
            else:
                fontfile = get_nt_fname(family + " Bold Italic") or \
                    get_nt_fname(family + " Bold Oblique")

            if not fontfile:
                log.error("Unknown font: %s", fname)
                return None

        family, pos = guessFont(fname)
        variants = [
            get_nt_fname(family) or fontfile,
            get_nt_fname(family+" Bold") or fontfile,
            get_nt_fname(family+" Italic") or \
                get_nt_fname(family+" Oblique") or fontfile,
            get_nt_fname(family+" Bold Italic") or \
                get_nt_fname(family+" Bold Oblique") or fontfile,
        ]
        return variants

def autoEmbed(fname):
    """Given a font name, does a best-effort of embedding
    said font and its variants.

    Returns a list of the font names it registered with ReportLab.

    """
    log.info('Trying to embed %s'%fname)
    fontList = []
    variants=[]
    f = findFont(fname)
    if f : # We have this font located
        if f[0].lower()[-4:]=='.afm': #Type 1 font
            family = families[f[2]]

            # Register the whole family of faces
            faces = [pdfmetrics.EmbeddedType1Face(*fonts[fn.lower()][:2]) for fn in family]
            for face in faces:
                pdfmetrics.registerTypeFace(face)

            for face, name in zip(faces, family):
                fontList.append(name)
                font = pdfmetrics.Font(face, name, "WinAnsiEncoding")
                log.info('Registering font: %s from %s'%\
                            (face,name))
                pdfmetrics.registerFont(font)

            # Map the variants
            regular, italic, bold, bolditalic = family
            addMapping(fname, 0, 0, regular)
            addMapping(fname, 0, 1, italic)
            addMapping(fname, 1, 0, bold)
            addMapping(fname, 1, 1, bolditalic)
            addMapping(regular, 0, 0, regular)
            addMapping(regular, 0, 1, italic)
            addMapping(regular, 1, 0, bold)
            addMapping(regular, 1, 1, bolditalic)
            log.info('Embedding as %s'%fontList)
            return fontList
        else: # A TTF font
            variants = [fonts[f.lower()][0] for f in families[f[2]]]
    if not variants: # Try fc-match
        variants = findTTFont(fname)
    # It is a TT Font and we found it using fc-match (or found *something*)
    if variants:
        for variant in variants:
            vname = os.path.basename(variant)[:-4]
            try:
                if vname not in pdfmetrics._fonts:
                    _font=TTFont(vname, variant)
                    log.info('Registering font: %s from %s'%\
                            (vname,variant))
                    pdfmetrics.registerFont(_font)
            except TTFError:
                log.error('Error registering font: %s from %s'%(vname,variant))
            else:
                fontList.append(vname)
        regular, bold, italic, bolditalic = [
            os.path.basename(variant)[:-4] for variant in variants]
        addMapping(regular, 0, 0, regular)
        addMapping(regular, 0, 1, italic)
        addMapping(regular, 1, 0, bold)
        addMapping(regular, 1, 1, bolditalic)
        log.info('Embedding via findTTFont as %s'%fontList)
    return fontList


def guessFont(fname):
    """Given a font name like "Tahoma-BoldOblique", "Bitstream Charter Italic"
    or "Perpetua Bold Italic" guess what it means.

    Returns (family, x) where x is
        0: regular
        1: bold
        2: italic
        3: bolditalic

    """
    italic = 0
    bold = 0
    if '-' not in fname:
        sfx = {"Bold":1, "Bold Italic":3, "Bold Oblique":3, "Italic":2,
            "Oblique":2}
        for key in sfx:
            if fname.endswith(" "+key):
                return fname.rpartition(key)[0], sfx[key]
        return fname, 0

    else:
        family, mod = fname.rsplit('-', 1)
        
    mod = mod.lower()
    if "oblique" in mod or "italic" in mod:
        italic = 1
    if "bold" in mod:
        bold = 1
     
    if bold+italic == 0: #Not really a modifier
        return fname, 0
    return family, bold + 2*italic


def main():
    global flist
    if len(sys.argv) != 2:
        print "Usage: findfont fontName"
        sys.exit(1)
    if os.name == 'nt':
        flist = [".", os.environ.get("SystemRoot", "C:\\Windows")+"\\Fonts"]
    else:
        flist = [".", "/usr/share/fonts", "/usr/share/texmf-dist/fonts"]
    fn, pos = guessFont(sys.argv[1])
    f = findFont(fn)
    if not f:
        f = findTTFont(fn)
    if f:
        print f
    else:
        print "Unknown font %s" % sys.argv[1]


if __name__ == "__main__":
    main()
