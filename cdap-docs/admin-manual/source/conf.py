# -*- coding: utf-8 -*-

import json
import os
import sys

# Import the common config file
# Note that paths in the common config are interpreted as if they were
# in the location of this file

# Setup the config
sys.path.insert(0, os.path.abspath('../../_common'))
from common_conf import *

html_short_title_toc, html_short_title, html_context = set_conf_for_manual()

if release:
    rst_epilog += """
.. |literal-Wordcount-release-jar| replace:: ``WordCount-%(release)s.jar``

""" % {'release': release}

# Build property tables files imported by logging.rst

writing_logs_to_kafka = [
    "log.kafka.topic",
    "log.publish.num.partitions",
    "log.publish.partition.key",
    ]
build_property_tables(defaults_dict, exclusions, dcd.RST_TABLE_HEADER, "logging-writing-logs-to-kafka.rst", writing_logs_to_kafka)

logging_log_saver_service = [
    "log.saver.max.instances",
    "log.saver.num.instances",
    "log.saver.container.memory.mb",
    "log.saver.container.num.cores",
    ]
build_property_tables(defaults_dict, exclusions, dcd.RST_TABLE_HEADER, "logging-log-saver-service.rst", logging_log_saver_service)

logging_pipeline_configuration_1 = [
    "log.pipeline.cdap.dir.permissions",
    "log.pipeline.cdap.file.cleanup.interval.mins",
    "log.pipeline.cdap.file.cleanup.transaction.timeout",
    "log.pipeline.cdap.file.max.lifetime.ms",
    "log.pipeline.cdap.file.max.size.bytes",
    "log.pipeline.cdap.file.permissions",
    "log.pipeline.cdap.file.retention.duration.days",
    ]
build_property_tables(defaults_dict, exclusions, dcd.RST_TABLE_HEADER, "logging-pipeline-configuration-1.rst", logging_pipeline_configuration_1)

logging_pipeline_configuration_2 = [
    "log.process.pipeline.checkpoint.interval.ms",
    "log.process.pipeline.config.dir",
    "log.process.pipeline.event.delay.ms",
    "log.process.pipeline.kafka.fetch.size",
    "log.process.pipeline.lib.dir",
    ]
build_property_tables(defaults_dict, exclusions, dcd.RST_TABLE_HEADER, "logging-pipeline-configuration-2.rst", logging_pipeline_configuration_2)


# Add Microsoft Azure HDInsight versions for this branch

hdinsight_versions = ''
hdinsight_path = os.path.join(os.getcwd(), '../../..', 'cdap-distributions/src/hdinsight/pkg/createUiDefinition.json')
try:
    with open(hdinsight_path) as json_data:
        d = json.load(json_data)
        v_list = d['clusterFilters']['versions']
        if not v_list:
            raise
        if len(v_list) == 1:
            hdinsight_versions = v_list[0]
        elif len(v_list) == 2:
            hdinsight_versions = "%s and %s" % (v_list[0], v_list[1])
        else:
            for v in v_list[:-1]:
                hdinsight_versions += "%s, " % v
            hdinsight_versions += "and %s" % v_list[-1]
except:
    raise Exception(html_short_title_toc, "hdinsight_versions can't be found at %s" % hdinsight_path)

if hdinsight_versions:
    rst_epilog += """
.. |hdinsight-versions| replace:: %s

""" % hdinsight_versions

