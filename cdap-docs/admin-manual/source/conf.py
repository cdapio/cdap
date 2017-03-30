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

if release:
    rst_epilog += """
.. |literal-Wordcount-release-jar| replace:: ``WordCount-%(release)s.jar``

""" % {'release': release}

# Build property tables files imported by logging.rst

def build_property_tables(defaults, exclusions, rst_table_header, name, properties):
    target = os.path.join(os.getcwd(), '../target/_includes', name)
    table = rst_table_header + '\n'
    for prop in properties:
        if prop in exclusions:
            print "Ignoring prop '%s' as it is in the exclusions list" % prop
        else:
            if defaults.has_key(prop):
                table += defaults[prop].rst()
            else:
                print "Unable to find prop '%s' in defaults" % prop
                raise Exception('build_property_tables', "Unable to find prop '%s' in defaults" % prop)
    f = open(target, 'w')
    f.write(table)
    f.close()
    print "Wrote property table: %s" % name

defaults_dict = {}
sys.path.insert(0, os.path.abspath('../../tools/cdap-default'))
dcd = __import__("doc-cdap-default")
defaults, tree = dcd.load_xml()
exclusions = dcd.load_exclusions()
if defaults:
    for item in defaults:
        defaults_dict[item.name] = item
else:
    print "Unable to build property tables from the cdap-defaults.xml file"

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
