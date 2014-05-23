# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/languages.py $
#$Date: 2012-02-28 21:07:21 -0300 (Tue, 28 Feb 2012) $
#$Revision: 2443 $

from docutils.languages import get_language as get_language

from rst2pdf.log import log


def get_language_silent(lang):
    """Docutils get_language() with patches for older versions."""
    try:
        return get_language(lang)
    except TypeError, err: # Docutils 0.8.1
        if 'get_language() takes exactly 2 arguments' in str(err):
            class SilentReporter(object):
                def warning(self, msg):
                    pass
            return get_language(lang, SilentReporter())
        raise # re-raise any other TypeError
    except ImportError: # Docutils < 0.8
        return get_language('en')


def get_language_available(lang):
    """Docutils get_language() also returning the available language."""
    module = get_language_silent(lang)
    docutils_lang = module.__name__.rsplit('.', 1)[-1]
    if (docutils_lang == 'en' and docutils_lang != lang
            and '_' in lang):
        module = get_language_silent(lang.split('_', 1)[0])
        docutils_lang = module.__name__.rsplit('.', 1)[-1]
    if docutils_lang != lang:
        warn = (docutils_lang.split('_', 1)[0] == lang.split('_', 1)[0]
                    and log.info or log.warning)
        warn("Language '%s' not supported by Docutils,"
            " using '%s' instead." % (lang, docutils_lang))
        if docutils_lang == 'en' and lang.split('_', 1)[0] != 'en':
            lang = 'en_US'
    return lang, docutils_lang, module
