# -*- coding: utf-8 -*-

import sys
import os

# Import the common config file
# Note that paths in the common config are interpreted as if they were 
# in the location of this file

# Setup the config
sys.path.insert(0, os.path.abspath('../../_common'))
from common_conf import * 

html_short_title_toc, html_short_title, html_context = set_conf_for_manual()

navigator_version = '0.1.0'

extlinks['cdap-kafka-flow'] = ('https://github.com/caskdata/cdap-packs/tree/release/cdap-%s-compatible/cdap-kafka-pack/cdap-kafka-flow%%s' % short_version, None)
extlinks['navigator-jar'] = ('http://search.maven.org/remotecontent?filepath=co/cask/cdap/metadata/navigator/%(navigator_version)s/navigator-%(navigator_version)s.jar%%s' % {'navigator_version': navigator_version}, None)

rst_epilog +=  """
.. |navigator-version| replace:: %(navigator_version)s
""" % {'navigator_version': navigator_version}
