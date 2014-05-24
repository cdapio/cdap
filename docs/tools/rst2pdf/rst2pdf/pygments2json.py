# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
'''
Creates a rst2pdf stylesheet for each pygments style.
'''

import sys
import os
import simplejson
from pygments.token import STANDARD_TYPES
from pygments import styles as pstyles


def css2rl(css):
    dstyles = {}
    # First create a dumb stylesheet
    for key in STANDARD_TYPES:
        dstyles["pygments-" + STANDARD_TYPES[key]] = {'parent': 'code'}

    styles = []
    for line in css.splitlines():
        line = line.strip()
        sname = "pygments-" + line.split(' ')[0][1:]
        style = dstyles.get(sname, {'parent': 'code'})
        options = line.split('{')[1].split('}')[0].split(';')
        for option in options:
            option = option.strip()
            option, argument = option.split(':')
            option=option.strip()
            argument=argument.strip()
            if option == 'color':
                style['textColor'] = argument.strip()
            if option == 'background-color':
                style['backColor'] = argument.strip()

            # These two can come in any order
            if option == 'font-weight' and argument == 'bold':
                if 'fontName' in style and \
                    style['fontName'] == 'stdMonoItalic':
                    style['fontName'] = 'stdMonoBoldItalic'
                else:
                    style['fontName'] = 'stdMonoBold'
            if option == 'font-style' and argument == 'italic':
                if 'fontName' in style and style['fontName'] == 'stdBold':
                    style['fontName'] = 'stdMonoBoldItalic'
                else:
                    style['fontName'] = 'stdMonoItalic'
        if style.get('textColor', None) is None:
		style['textColor']='black'
        styles.append([sname, style])

    return simplejson.dumps({'styles': styles}, indent=2)

for name in list(pstyles.get_all_styles()):
    css=os.popen('pygmentize -S %s -f html'%name, 'r').read()
    open(name+'.json', 'w').write(css2rl(css))
