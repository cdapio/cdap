#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright © 2015 Cask Data, Inc.
#
# Used to generate JSONP from a CDAP documentation directory on a webserver.
# 
# sudo echo "versionscallback({\"development\": \"2.6.0-SNAPSHOT\", \"current\": \"2.5.2\", \"versions\": [\"2.5.1\", \"2.5.0\"]});" > json-versions.js; ls -l

import sys
from os import getcwd, listdir, readlink
from os.path import isdir, islink, join

def add_value(call, name, value):
    if value:
        if call:
            call += ', '
        call += '\\\"%s\\\": \\\"%s\\\"' % (name, value)
    return call


def add_object(call, name, value):
    if value:
        if call:
            call += ', '
        call += ('\\\"%s\\\": %s' % (name, value)).replace("\'", '\\\"')
    return call


def walk_directory(path=''):
    global current, development, older

    if not path:
        path = getcwd()
    
    onlydirs = [ d for d in listdir(path) if isdir(join(path,d)) ]
    onlydirs.reverse()
    
    for d in onlydirs:
        if d == 'current':
            d_path = join(path,d)
            if islink(d_path):
                current = readlink(d_path)
        elif d.endswith('SNAPSHOT'):
            development.append(d)
        elif d and d != current:
            older.append(d)

def build(path=''):
    global current, development, older
    call = ''

    walk_directory(path)

    call = add_object(call, 'development', development)
    call = add_value(call, 'current', current)
    call = add_object(call, 'older', older)

    target = join(path, 'json-versions.js')
        
    print 'sudo echo "versionscallback({%s});" > %s; ls -l' % (call, target)

def usage():
    print 'Generates a command that creates the "versionscallback" JSONP from a CDAP documentation directory on a webserver.'
    print 'Run this with the path to the directory containing the documentation directories.'
    print 'python %s <path>' % sys.argv[0]

#  Main
if __name__ == '__main__':
    current = ''
    development = []
    older = []
    path = ''
    if len(sys.argv) > 1:
        path = sys.argv[1]
        build(path)
    else:
        usage()
    