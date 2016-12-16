# -*- coding: utf-8 -*-

import os
import sys

# Import the common config file
# Note that paths in the common config are interpreted as if they were 
# in the location of this file

# Setup the config
sys.path.insert(0, os.path.abspath('../../_common'))
from common_conf import * 

html_short_title_toc, html_short_title, html_context = set_conf_for_manual()

cdap_kafka_flow_pattern = ("https://github.com/caskdata/cdap-packs/tree/" 
    + "%s/cdap-kafka-pack/cdap-kafka-flow%%s" % git_build_vars['GIT_BRANCH_CDAP_PACKS'])
navigator_jar_github_pattern = ("http://search.maven.org/remotecontent?filepath=co/cask/cdap/metadata/navigator/"
    + "%(nav_ver)s/navigator-%(nav_ver)s.jar%%s" % {'nav_ver': git_build_vars['GIT_VERSION_NAVIGATOR']})

extlinks['cdap-kafka-flow'] = (cdap_kafka_flow_pattern, None)
extlinks['navigator-jar'] = (navigator_jar_github_pattern, None)

rst_epilog +=  """
.. |navigator-version| replace:: %(navigator_version)s
""" % {'navigator_version': git_build_vars['GIT_VERSION_NAVIGATOR']}
