# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
"""Singleton config object"""


import ConfigParser
import os
from rst2pdf.rson import loads

cfdir = os.path.join(os.path.expanduser('~'), '.rst2pdf')
cfname = os.path.join(cfdir, 'config')


def getValue(section, key, default=None):
    section = section.lower()
    key = key.lower()
    try:
        return loads(conf.get(section, key))
    except Exception:
        return default


class ConfigError(Exception):

    def __init__(self, modulename, msg):
        self.modulename = modulename
        self.msg = msg

conf = ConfigParser.SafeConfigParser()

def parseConfig(extracf=None):
    global conf
    cflist = ["/etc/rst2pdf.conf", cfname]
    if extracf:
        cflist.append(extracf)
    conf = ConfigParser.SafeConfigParser()
    conf.read(cflist)
    
parseConfig()
