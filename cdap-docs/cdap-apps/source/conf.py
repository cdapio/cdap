# -*- coding: utf-8 -*-

import sys
import os

# Import the common config file
# Note that paths in the common config are interpreted as if they were 
# in the location of this file

# Setup the config
sys.path.insert(0, os.path.abspath('../../_common'))

from common_conf import setup as _setup
from common_conf import *

html_short_title_toc, html_short_title, html_context = set_conf_for_manual()

rst_epilog += """
.. |cdap-hydrator-version| replace:: %(cdap-hydrator-version)s

.. |literal-cdap-hydrator-version| replace:: ``%(cdap-hydrator-version)s``

""" % {'cdap-hydrator-version': os.environ.get('HYDRATOR_VERSION')}


def setup(app):
    # Call imported setup
    _setup(app)
    
    # Add a custom config value that can be used for conditional content
    app.add_config_value('release_version', '', 'env')


# Set the condition
if version == ('3.2.0'):
    release_version = 'equal_to_3.2.0'
else:
    release_version = 'greater_than_3.2.0'
