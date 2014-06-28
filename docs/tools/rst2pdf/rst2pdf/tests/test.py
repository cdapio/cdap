# -*- coding: utf-8 -*-

from autotest import MD5Info, PathInfo, globjoin
from autotest import run_single, dirname, checkmd5

import sys, os
import nose.plugins.skip

class RunTest:
    def __init__(self,f):
        basename = os.path.basename(f)
        self.description = basename 
        mprefix = os.path.join(PathInfo.md5dir, basename)[:-4]
        md5file = mprefix + '.json'
        ignfile = os.path.join(PathInfo.inpdir , basename[:-4])+'.ignore'
        info=MD5Info()
        self.skip=False
        self.openIssue=False
        if os.path.exists(ignfile):
            self.skip=True
        if os.path.exists(md5file):
            f = open(md5file, 'rb')
            exec f in info
            f.close()
        if info.good_md5 in [[],['sentinel']]:
            # This is an open issue or something that can't be checked automatically
            self.openIssue=True
            
    def __call__(self,f):
        if self.skip:
            raise nose.plugins.skip.SkipTest
        elif self.openIssue:
            assert False, 'Test has no known good output (Open Issue)'
        else:
            key, errcode = run_single(f)
            if key in ['incomplete']:
                raise nose.plugins.skip.SkipTest
            assert key == 'good', '%s is not good: %s'%(f,key)

from execmgr import textexec, default_logger as log
import shlex


def run_installed_single(inpfname):
    """Like run_single, but runs the test using the installed version
    of rst2pdf"""

    iprefix = os.path.splitext(inpfname)[0]
    basename = os.path.basename(iprefix)
    if os.path.exists(iprefix + '.ignore'):
        return 'ignored', 0
        
    oprefix = os.path.join(PathInfo.outdir, basename)
    mprefix = os.path.join(PathInfo.md5dir, basename)
    outpdf = oprefix + '.pdf'
    outtext = oprefix + '.log'
    md5file = mprefix + '.json'
    
    inpfname = iprefix + '.txt'
    style = iprefix + '.style'
    cli = iprefix + '.cli'
    if os.path.isfile(cli):
        f = open(cli)
        extraargs=shlex.split(f.read())
        f.close()
    else:
        extraargs=[]
    args = ['rst2pdf'] + ['--date-invariant', '-v', os.path.basename(inpfname)]+extraargs
    if os.path.exists(style):
        args.extend(('-s', os.path.basename(style)))
    args.extend(('-o', outpdf))
    errcode, result = textexec(args, cwd=dirname(inpfname), python_proc=None)

    checkinfo = checkmd5(outpdf, md5file, result, None, errcode, iprefix)
    log(result, '')
    outf = open(outtext, 'wb')
    outf.write('\n'.join(result))
    outf.close()
    return checkinfo, errcode


class RunInstalledTest:
    def __init__(self,f):
        basename = os.path.basename(f)
        self.description = basename 
        mprefix = os.path.join(PathInfo.md5dir, basename)[:-4]
        md5file = mprefix + '.json'
        ignfile = os.path.join(PathInfo.inpdir , basename[:-4])+'.ignore'
        info=MD5Info()
        self.skip=False
        self.openIssue=False
        if os.path.exists(ignfile):
            self.skip=True
        if os.path.exists(md5file):
            f = open(md5file, 'rb')
            exec f in info
            f.close()
        if info.good_md5 in [[],['sentinel']]:
            # This is an open issue or something that can't be checked automatically
            self.openIssue=True
            
    def __call__(self,f):
        if self.skip:
            raise nose.plugins.skip.SkipTest
        elif self.openIssue:
            assert False, 'Test has no known good output (Open Issue)'
        else:
            key, errcode = run_installed_single(f)
            if key in ['incomplete']:
                raise nose.plugins.skip.SkipTest
            assert key == 'good', '%s is not good: %s'%(f,key)

class RunSphinxTest:
    def __init__(self,f):
        basename = os.path.basename(f[:-1])
        self.description = basename 
        mprefix = os.path.join(PathInfo.md5dir, basename)
        md5file = mprefix + '.json'
        ignfile = os.path.join(PathInfo.inpdir , basename)+'.ignore'
        info=MD5Info()
        self.skip=False
        self.openIssue=False
        
        if os.path.exists(ignfile):
            self.skip=True
        if os.path.exists(md5file):
            f = open(md5file, 'rb')
            exec f in info
            f.close()
        if info.good_md5 in [[],['sentinel']]:
            # This is an open issue or something that can't be checked automatically
            self.openIssue=True
            
    def __call__(self,f):
        if self.skip:
            raise nose.plugins.skip.SkipTest
        elif self.openIssue:
            assert False, 'Test has no known good output (Open Issue)'
        else:
            key, errcode = run_single(f)
            if key in ['incomplete']:
                raise nose.plugins.skip.SkipTest
            assert key == 'good', '%s is not good: %s'%(f,key)


def regulartest():
    '''To run these tests (similar to autotest), run
    nosetests -i regulartest'''
    testfiles = globjoin(PathInfo.inpdir, '*.txt')
    results = {}
    for fname in testfiles:
        yield RunTest(fname), fname

def releasetest():
    '''To run these tests (after you run setup.py install), run
    nosetests -i releasetest'''
    testfiles = globjoin(PathInfo.inpdir, '*.txt')
    results = {}
    for fname in testfiles:
        yield RunInstalledTest(fname), fname

def sphinxtest():
    '''To run these tests , run nosetests -i sphinxtest'''
    testfiles = globjoin(PathInfo.inpdir, 'sphinx*/')
    results = {}
    for fname in testfiles:
        yield RunSphinxTest(fname), fname


def setup():
    PathInfo.add_coverage()
