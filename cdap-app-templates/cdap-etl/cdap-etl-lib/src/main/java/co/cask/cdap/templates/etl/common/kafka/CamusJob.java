/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.templates.etl.common.kafka;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 *
 */
public class CamusJob {

  private static final Logger LOG = LoggerFactory.getLogger(CamusJob.class);

  public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
  public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";
  public static final String ETL_COUNTS_PATH = "etl.counts.path";
  public static final String ETL_COUNTS_CLASS = "etl.counts.class";
  public static final String ETL_COUNTS_CLASS_DEFAULT = "com.linkedin.camus.etl.kafka.common.EtlCounts";
  public static final String ETL_KEEP_COUNT_FILES = "etl.keep.count.files";
  public static final String ETL_BASEDIR_QUOTA_OVERIDE = "etl.basedir.quota.overide";
  public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
  public static final String ETL_FAIL_ON_ERRORS = "etl.fail.on.errors";
  public static final String ETL_FAIL_ON_OFFSET_OUTOFRANGE = "etl.fail.on.offset.outofrange";
  public static final String ETL_FAIL_ON_OFFSET_OUTOFRANGE_DEFAULT = Boolean.TRUE.toString();
  public static final String ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND = "etl.max.percent.skipped.schemanotfound";
  public static final String ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND_DEFAULT = "0.1";
  public static final String ETL_MAX_PERCENT_SKIPPED_OTHER = "etl.max.percent.skipped.other";
  public static final String ETL_MAX_PERCENT_SKIPPED_OTHER_DEFAULT = "0.1";
  public static final String ETL_MAX_ERRORS_TO_PRINT_FROM_FILE = "etl.max.errors.to.print.from.file";
  public static final String ETL_MAX_ERRORS_TO_PRINT_FROM_FILE_DEFAULT = "10";
  public static final String ZK_AUDIT_HOSTS = "zookeeper.audit.hosts";
  public static final String KAFKA_MONITOR_TIER = "kafka.monitor.tier";
  public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
  public static final String BROKER_URI_FILE = "brokers.uri";
  public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
  public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
  public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
  public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";
  public static final String KAFKA_CLIENT_NAME = "kafka.client.name";
  public static final String KAFKA_FETCH_BUFFER_SIZE = "kafka.fetch.buffer.size";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_HOST_URL = "kafka.host.url";
  public static final String KAFKA_HOST_PORT = "kafka.host.port";
  public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
  public static final String CAMUS_REPORTER_CLASS = "etl.reporter.class";
  public static final String LOG4J_CONFIGURATION = "log4j.configuration";
  public static String kafkaBrokersList = "";

  private static HashMap<String, Long> timingMap = new HashMap<String, Long>();

  public static void startTiming(String name) {
    timingMap.put(name, (timingMap.get(name) == null ? 0 : timingMap.get(name)) - System.currentTimeMillis());
  }

  public static void stopTiming(String name) {
    timingMap.put(name, (timingMap.get(name) == null ? 0 : timingMap.get(name)) + System.currentTimeMillis());
  }

  public static void setTime(String name) {
    timingMap.put(name, (timingMap.get(name) == null ? 0 : timingMap.get(name)) + System.currentTimeMillis());
  }

  public static int getKafkaFetchRequestMinBytes(JobContext context) {
    return context.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MIN_BYTES, 1024);
  }

  public static int getKafkaFetchRequestMaxWait(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MAX_WAIT, 1000);
  }

  public static String getKafkaBrokers(JobContext job) {
    return "10.150.26.255:9092";
//    String brokers = job.getConfiguration().get(KAFKA_BROKERS);
//    if (brokers == null) {
//      brokers = job.getConfiguration().get(KAFKA_HOST_URL);
//      if (brokers != null) {
//        LOG.warn("The configuration properties " + KAFKA_HOST_URL + " and " + KAFKA_HOST_PORT +
//                   "are deprecated. Please switch to using " + KAFKA_BROKERS);
//        return brokers + ":" + job.getConfiguration().getInt(KAFKA_HOST_PORT, 10251);
//      }
//    }
//    return brokers;
  }

  public static int getKafkaFetchRequestCorrelationId(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_CORRELATION_ID, -1);
  }

  public static String getKafkaClientName(JobContext job) {
    return job.getConfiguration().get(KAFKA_CLIENT_NAME, "client");
  }

  public static String getKafkaFetchRequestBufferSize(JobContext job) {
    return job.getConfiguration().get(KAFKA_FETCH_BUFFER_SIZE, "100");
  }

  public static int getKafkaTimeoutValue(JobContext job) {
    int timeOut = job.getConfiguration().getInt(KAFKA_TIMEOUT_VALUE, 30000);
    return timeOut;
  }

  public static int getKafkaBufferSize(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_FETCH_BUFFER_SIZE, 1024 * 1024);
  }
}
