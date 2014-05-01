#!/usr/bin/env python
# -*- coding: utf-8 -*-

#$HeadURL: http://rst2pdf.googlecode.com/svn/trunk/rst2pdf/tests/parselogs.py $
#$LastChangedDate: 2009-10-31 00:53:18 -0500 (Sat, 31 Oct 2009) $
#$LastChangedRevision: 1271 $

# See LICENSE.txt for licensing terms

'''
This program is designed to migrate checksums to new versions of software,
when it is known that all the checksum changes are irrelevant to the visual
aspects of the PDF.  An example of this is when the PDF version number is
incremented from 1.3 to 1.4 for no good reason :-)

Usage:

1) Clean the output directory -- rm -Rf output/*
2) Run autotest with the old version of the software, to populate
   the output directory with known good, known bad, etc. versions.
3) Run parselogs and save the results:
      ./parselogs.py > oldlog.txt
4) Make the change to the software which is known not to affect the
   output visually.
5) Run this script.  It should rerun autotest, updating checksums to
   the same as they were previously.
6) Re-clean the output directory -- rm -Rf output/*
7) Re-run autotest for all the files --same as in step 2, but with new
   software version.
8) Run parselogs and save the results:
      ./parselogs.py > newlog.txt
9) Check the logs to make sure no files moved to a different category:
      tkdiff oldlog.txt newlog.txt
10) Check in the fixed checksums
'''

import os
import glob
import subprocess

def getchecksuminfo():
    for fn in sorted(glob.glob(os.path.join('old', '*.log'))):
        f = open(fn, 'rb')
        data = f.read()
        f.close()
        fn = os.path.splitext(os.path.basename(fn))[0]
        data = data.rsplit('\n', 2)[1]
        if data.startswith('File'):
            yield fn, 'fail', None
        else:
            yield fn, data.rsplit(' ', 1)[-1][:-1], data.split("'")[1]

def getcategories():
    mydict = {}
    for fn, category, checksum in getchecksuminfo():
        myset = mydict.get(category)
        if myset is None:
            mydict[category] = myset = set()
        myset.add((fn, checksum))
    return mydict

def dumpinfo():
    mydict = getcategories()
    if not mydict:
        print '\nNo log files found'
    migrate = set('good bad incomplete'.split())
    for checksum_result, values in sorted(mydict.iteritems()):
        if checksum_result not in migrate:
            continue
        names = ' '.join('input/%s' % x[0] for x in values)
        cmd = './autotest.py -f -u %s %s' %  (checksum_result, names)
        subprocess.call(cmd.split())

if __name__ == '__main__':
    dumpinfo()
