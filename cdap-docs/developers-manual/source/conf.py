# -*- coding: utf-8 -*-

import os
import sys

# Import the common config file
# Note that paths in the common config are interpreted as if they were 
# in the location of this file

# Setup the config
sys.path.insert(0, os.path.abspath('../../_common'))
from common_conf import setup as _setup
from common_conf import *

html_short_title_toc, html_short_title, html_context = set_conf_for_manual()

def setup(app):
    # Call imported setup
    _setup(app)
    # Add a custom config value that can be used for conditional content
    app.add_config_value('snapshot_version', '', 'env')

# Set the condition
archetype_version = "-DarchetypeVersion=%s" % release
if release.endswith('SNAPSHOT'):
    snapshot_version = True
    archetype_repository = '-DarchetypeRepository=https://oss.sonatype.org/content/repositories/snapshots/'
    archetype_repository_version = "%s %s" % (archetype_repository, archetype_version)
else:
    snapshot_version = False
    archetype_repository = '-DarchetypeRepository=https://oss.sonatype.org/service/local/staging/deploy/maven2'
    archetype_repository_version = archetype_version

rst_epilog += """

.. |archetype-repository| replace:: %(archetype_repository)s
.. |archetype-version| replace:: %(archetype_version)s
.. |archetype-repository-version| replace:: %(archetype_repository_version)s

""" % {'archetype_repository': archetype_repository,
       'archetype_version': archetype_version,
       'archetype_repository_version': archetype_repository_version,
       }
