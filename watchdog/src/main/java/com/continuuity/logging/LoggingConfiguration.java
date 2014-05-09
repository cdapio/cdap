/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Logging configuration helper.
 */
public final class LoggingConfiguration {
  // Common between Distributed and Single Node
  public static final String LOG_PATTERN = "log.pattern";
  public static final String LOG_BASE_DIR = "log.base.dir";
  public static final String LOG_FILE_SYNC_INTERVAL_BYTES = "log.file.sync.interval.bytes";

  // Used only in Distributed mode
  public static final String NUM_PARTITIONS = "log.publish.num.partitions";
  public static final String KAFKA_SEED_BROKERS = "kafka.seed.brokers";
  public static final String LOG_RUN_ACCOUNT = "log.run.account";
  public static final String LOG_SAVER_EVENT_BUCKET_INTERVAL_MS = "log.saver.event.bucket.interval.ms";
  public static final String LOG_SAVER_EVENT_PROCESSING_DELAY_MS = "log.saver.event.processing.delay.ms";
  public static final String LOG_SAVER_INACTIVE_FILE_INTERVAL_MS = "log.saver.inactive.file.interval.ms";
  public static final String LOG_SAVER_CHECKPOINT_INTERVAL_MS = "log.saver.checkpoint.interval.ms";
  public static final String LOG_SAVER_TOPIC_WAIT_SLEEP_MS = "log.saver.topic.wait.sleep.ms";
  public static final String LOG_RETENTION_DURATION_DAYS = "log.retention.duration.days";
  public static final String LOG_MAX_FILE_SIZE_BYTES = "log.max.file.size.bytes";
  public static final String KAFKA_PRODUCER_TYPE = "kafka.producer.type";
  public static final String KAFKA_PROCUDER_BUFFER_MS = "kafka.producer.buffer.ms";
  public static final String LOG_CLEANUP_RUN_INTERVAL_MINS = "log.cleanup.run.interval.mins";

  // Constants
  // Table used to store log metadata
  public static final String LOG_META_DATA_TABLE = "log.meta";
  // Defaults
  public static final String DEFAULT_LOG_PATTERN = "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n";
  public static final String DEFAULT_KAFKA_PRODUCER_TYPE = "async";
  public static final long DEFAULT_KAFKA_PROCUDER_BUFFER_MS = 1000;
  public static final String DEFAULT_NUM_PARTITIONS = "10";
  public static final int DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS = 24 * 60;

  public static final long DEFAULT_LOG_SAVER_EVENT_BUCKET_INTERVAL_MS = 4 * 1000;
  public static final long DEFAULT_LOG_SAVER_EVENT_PROCESSING_DELAY_MS = 8 * 1000;
  public static final long DEFAULT_LOG_SAVER_INACTIVE_FILE_INTERVAL_MS = 60 * 60 * 1000;
  public static final long DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS = 60 * 1000;
  public static final long DEFAULT_LOG_RETENTION_DURATION_DAYS = 30;
  public static final long DEFAULT_LOG_SAVER_TOPIC_WAIT_SLEEP_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

  private LoggingConfiguration() {}

  /**
   * Given a string of format "host1:port1,host2:port2", the function returns a list of Kafka hosts.
   * @param seedBrokers String to parse the host/port list from.
   * @return list of Kafka hosts.
   */
  public static List<KafkaHost> getKafkaSeedBrokers(String seedBrokers) {
    List<KafkaHost> kafkaHosts = Lists.newArrayList();
    for (String hostPort : Splitter.on(",").trimResults().split(seedBrokers)) {
      Iterable<String> hostPortList = Splitter.on(":").trimResults().split(hostPort);

      Iterator<String> it = hostPortList.iterator();
      kafkaHosts.add(new KafkaHost(it.next(), Integer.parseInt(it.next())));
    }
    return kafkaHosts;
  }

  /**
   * Represents a Kafka host with hostname and port.
   */
  public static class KafkaHost {
    private final String hostname;
    private final int port;

    public KafkaHost(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("hostname", hostname)
        .add("port", port)
        .toString();
    }
  }
}
