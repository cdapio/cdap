#!/usr/bin/env python
'''
    Call dumps() to dump a stylesheet to a string.

    Or run the script to dump all .json in the styles directory
    to .style in the styles directory.
'''

import sys
import os

from rson import loads as rloads
from json import loads as jloads

def dumps(obj, forcestyledict=True):
    ''' If forcestyledict is True, will attempt to
        turn styles into a dictionary.
    '''

    def dofloat(result, obj, indent):
        s = '%.3f' % obj
        while '.' in s and s.endswith('0'):
            s = s[:-1]
        result.append(s)

    def doint(result, obj, indent):
        if isinstance(obj, bool):
            obj = repr(obj).lower()
        result.append(str(obj))

    badch = set('[]{}:=,"\n')

    def dostr(result, obj, indent):
        try:
            float(obj)
        except:
            ok = True
        else:
            ok = obj == obj.strip()
        ok = ok and not set(obj) & badch
        if ok:
            result.append(obj)
            return

        obj = obj.replace('\\', '\\\\').replace('\n', '\\n')
        obj = obj.replace('"', '\\"')
        result.append('"%s"' % obj)

    def dolist(result, obj, indent):
        if indent:
            if not obj:
                result.append('[]')
                return
            elif isinstance(obj[0], list):
                result.append('[]')
                indent += '    '
                for item in obj:
                    dumprecurse(result, item, indent)
                return
        result.append('[')
        obj = [[x, ', '] for x in obj]
        obj[-1][-1] = ']'
        for item, separator in obj:
            dumprecurse(result, item, '')
            result.append(separator)

    def dodict(result, obj, indent):
        if not obj:
            result.append('{}')
            return
        obj = sorted(obj.iteritems())
        multiline = indent and ( len(obj) > 2 or
                    len(obj) == 2 and (
                         isinstance(obj[0][-1], (list, dict)) or
                         isinstance(obj[-1][-1], (list, dict))))
        if not multiline and (not indent or len(obj) != 1):
            result.append('{')
            obj = [[x, ', '] for x in obj]
            obj[-1][-1] = '}'
            for (key, value), separator in obj:
                dumprecurse(result, key, '')
                result.append(': ')
                dumprecurse(result, value, '')
                result.append(separator)
            return
        doindent = len(obj) > 1
        for key, value in obj:
            dumprecurse(result, key, indent, doindent)
            result.append(': ')
            dumprecurse(result, value, indent + '    ', False)

    def donone(result, obj, indent):
        result.append('null')

    dumpfuncs = {float: dofloat, int: doint, basestring: dostr,
                     list: dolist, dict: dodict, type(None): donone}

    dumpfuncs = dumpfuncs.items()

    def dumprecurse(result, obj, indent='\n', indentnow=True):
        if indentnow:
            result.append(indent)
        for otype, ofunc in dumpfuncs:
            if isinstance(obj, otype):
                return ofunc(result, obj, indent)
        raise ValueError(repr(obj))

    result = []
    if forcestyledict:
        obj = fixstyle(obj)
    dumprecurse(result, obj, indentnow=False)
    return fixspacing(''.join(result))

def fixspacing(s):
    ''' Try to make the output prettier by inserting blank lines
        in random places.
    '''
    result = []
    indent = -1
    for line in s.splitlines():
        line = line.rstrip()  # Some lines had ' '
        if not line:
            continue
        indent, previndent = len(line) - len(line.lstrip()), indent
        if indent <= previndent and indent < 8:
            if indent < previndent or not indent:
                result.append('')
        result.append(line)
    result.append('')
    return '\n'.join(result)

def fixstyle(obj):
    ''' Try to convert styles into a dictionary
    '''
    if obj:
        if isinstance(obj, list):
            lengths = [len(x) for x in obj]
            if min(lengths) == max(lengths) == 2:
                obj = dict(obj)
        elif isinstance(obj, dict) and 'styles' in obj:
            obj['styles'] = dict(obj['styles'])
    return obj

def convert(srcname):
    ''' Convert a single file from .json to .style
    '''
    print srcname
    sstr = open(srcname, 'rb').read()
    sdata = fixstyle(jloads(sstr))
    dstr = dumps(sdata)
    assert sdata == rloads(dstr), "Bad round-trip"

    dstname = srcname.replace('.json', '.style')
    dstf = open(dstname, 'wb')
    dstf.write(dstr)
    dstf.close()


if __name__ == '__main__':
    for fname in [os.path.join('styles', x) for x in os.listdir('styles') if x.endswith('.json')]:
        convert(fname)
