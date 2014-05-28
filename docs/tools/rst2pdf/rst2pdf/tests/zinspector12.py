#! /usr/bin/env python
# -*- coding: utf-8 -*-

#$HeadURL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/tests/zinspector12.py $
#$LastChangedDate: 2009-11-05 13:58:39 -0300 (Thu, 05 Nov 2009) $
#$LastChangedRevision: 1379 $

# See LICENSE.txt for licensing terms

'''
zinspector12 is part of the rst2pdf utility package.

Copyright (C) Patrick Maupin, Austin, Texas

It is designed for Luddites like the author to be able
to navigate the code base a little more easily.

Executing zinspector12 with no arguments will report
on import statements.

Executing zinspector12 with arguments will report on
where each name in the arguments list is imported from.

'''

import pythonpaths
import sys

loader = '../../bin/rst2pdf'
checkdirs = '../',
importf = '../r2p_imports.py'

ignore = set(''.split())

pythonpaths.setpythonpaths(loader)
sys.path[0:0] = checkdirs

def readf(fname):
    f = open(fname, 'rb')
    data = f.read()
    f.close()
    for splitcomment in ('"""', "'''"):
        data = data.split(splitcomment)
        assert len(data) in (1, 3)
        data = data[-1]
    data = [x.split('#')[0].rstrip() for x in data.splitlines()]
    for line in data:
        if line:
            yield line

def splitf(fname):
    result = []
    for line in readf(importf):
        if line.startswith((' ', 'else', 'elif', 'except')):
            result.append(line)
        else:
            if result:
                result.append('')
                yield '\n'.join(result)
            result = [line]


def indent(what, indent):
    newlf = '\n' + indent
    what = indent + what.replace('\n', newlf)
    if what.endswith(newlf):
        what = what[:-len(indent)]
    return what

def getimports(importf, ignore):
    importinfo = {}
    badimports = []
    basedir = {}
    exec '0' in basedir
    globaldir = basedir.copy()

    for code in splitf(importf):
        newdir = basedir.copy()
        exec code in newdir
        for key in basedir:
            del newdir[key]
        for key, value in newdir.iteritems():
            oldcode = importinfo.setdefault(key, code)
            if oldcode is not code:
                badimports.append((key, oldcode, code, globaldir[key], value))
        globaldir.update(newdir)

    return globaldir, importinfo, badimports

def checkimports(importf, ignore):
    _, _, badimports = getimports(importf, ignore)

    same = {}
    diff = {}

    for key, oldcode, code, oldvalue, value in badimports:
        code = indent(code, '    ')
        oldcode = indent(oldcode, '    ')
        mydict = (same, diff)[oldvalue != value]
        mydict.setdefault((oldcode, code), []).append(key)

    printed = False

    fmt = "\nSame '%s' imported twice:\n\n%s\n%s"
    for (oldcode, code), deflist in sorted(same.iteritems()):
        nevermind = set()
        x, y = oldcode.split('.', 1)[0], code.split('.', 1)[0]
        if ('.' in oldcode and '.' in code and
            oldcode != code and x == y):
            nevermind.add(x.split()[-1])
        deflist = ', '.join(sorted(set(deflist) - nevermind))
        if deflist:
            print fmt % (deflist, oldcode, code)
            printed = True

    fmt = "\nConflicting definitions for %s:\n\n%s\n%s"
    for (oldcode, code), deflist in sorted(diff.iteritems()):
        deflist = ', '.join(sorted(deflist))
        print fmt % (deflist, oldcode, code)
        printed = True

    if not printed:
        print "No conflicting imports"

def dumpinfo(varnames, importf, ignore):
    import inspect
    print
    g, importinfo, _ = getimports(importf, ignore)
    for name in varnames:
        if name not in g:
            print '%s not defined globally' % repr(name)
            continue
        value = g[name]
        info = '%s (value %s)' % (repr(name), repr(value))
        line = importinfo.get(name)
        if line is not None:
            print '%s imported with %s' % (
                    info, repr(line.split('#')[0].rstrip()))
        else:
            print '%s imported or defined explicitly (see source code)'
        try:
            sourcef = inspect.getfile(value)
        except TypeError:
            pass
        else:
            if sourcef.endswith('.pyc'):
                sourcef = sourcef[:-1]
            print '      defined in %s' % sourcef
    print

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        checkimports(importf, ignore)
    else:
        dumpinfo(sys.argv[1:], importf, ignore)
