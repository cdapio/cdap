# -*- coding: utf-8 -*-

import sys
import os

# Set this global variable so that it is set when importing the common conf.py
import __builtin__
__builtin__.CASK_PROJECT_TYPE = 'cdap-tutorial'

# Import the common config file
# Note that paths in the common config are interpreted as if they were 
# in the location of this file

sys.path.insert(0, os.path.abspath('../_common'))
from common_conf import * 

# Override the common config

html_short_title_toc = 'CDAP Tutorials'
html_short_title = u'%s' % html_short_title_toc

html_context = {'html_short_title_toc': html_short_title_toc}
