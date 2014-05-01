#!/usr/bin/env python
# -*- coding: utf-8 -*-

#$HeadURL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/tests/setmd5.py $
#$LastChangedDate: 2012-03-04 00:01:12 -0300 (Sun, 04 Mar 2012) $
#$LastChangedRevision: 2459 $


'''
Copyright (c) 2009, Patrick Maupin, Austin, Texas

See LICENSE.txt for licensing terms

setmd5 takes a result code and a list of tests, and changes
the test results to that code.

'''

import sys
import os
import autotest

def showhelp():
    raise SystemExit('''
usage: setmd5 code test [test ...]

Sets the MD5 code for all given tests to 'code' if the code
has not been set.

Valid codes are good, bad, incomplete, unknown, deprecated.

'test' can be specified as either the base name of the test,
or the filename of the test, the pdf, the md5, etc.  setmd5
will strip everything except the base name.

''')

def setcode(testname, code):
    testname = os.path.split(testname)
    if not testname[1]:
        testname = os.path.split(testname[0])
    testname = os.path.splitext(testname[1])[0]
    pdfpath = os.path.join(autotest.PathInfo.outdir, testname + '.pdf')
    md5path = os.path.join(autotest.PathInfo.md5dir, testname + '.json')
    return autotest.checkmd5(pdfpath, md5path, [], code)

def main(args=None):
    if args is None:
        args = sys.argv[1:]
    if len(args) < 2:
        return showhelp()

    code = args.pop(0)
    if code not in 'good bad incomplete unknown deprecated'.split():
        return showhelp()

    for testname in args:
        if code != setcode(testname, code):
            raise SystemExit('\nCould not update %s; checksum already marked %s\n' % 
                    (testname, repr(code)))

if __name__ == '__main__':
    main()
