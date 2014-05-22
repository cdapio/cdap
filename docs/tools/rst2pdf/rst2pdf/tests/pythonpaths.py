# -*- coding: utf-8 -*-

import os
import sys

def setpythonpaths(execfn, rootdir=None):
    ''' There is probably a cleaner way to do this.
        maybe have buildout give us a json or something.
        This imports everything and takes awhile, but
        it is a useful side-effect for the -f option
        (and would have to be done anyway for that).
        We only need the paths themselves when we are
        setting up for sphinx execution.
    '''
    pathlen = len(sys.path)
    f = open(execfn, 'rb')
    exec f in {'__name__':'testing'}
    f.close()
    newpaths = sys.path[:len(sys.path)-pathlen]
    ppath = os.environ.get('PYTHONPATH')
    if ppath is not None:
        newpaths.append(ppath)
    if rootdir is not None:
        newpaths.append(rootdir)
    os.environ['PYTHONPATH'] = ':'.join(newpaths)
