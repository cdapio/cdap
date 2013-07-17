package com.continuuity.metrics;

/**
 * Define constants for byte[] that are needed in multiple classes.
 */
public final class MetricsConstants {

  public static final class ConfigKeys {

    public static final String ENTITY_TABLE_NAME = "metrics.data.entity.tableName";
    public static final String METRICS_TABLE_PREFIX = "metrics.data.table.prefix";
    public static final String TIME_SERIES_TABLE_ROLL_TIME = "metrics.data.table.ts.rollTime";

    public static final String SERVER_ADDRESS = "metrics.query.server.address";
    public static final String SERVER_PORT = "metrics.query.server.port";
    public static final String THREAD_POOL_SIZE = "metrics.query.thread.pool.size";
    public static final String KEEP_ALIVE_SECONDS = "metrics.query.keepAlive.seconds";

    public static final String KAFKA_TOPIC = "metrics.kafka.topic";
    public static final String KAFKA_PARTITION_SIZE = "metrics.kafka.partition.size";
    public static final String KAFKA_CONSUMER_PERSIST_THRESHOLD = "metrics.kafka.consumer.persist.threshold";

    public static final String PROCESSING_THREADS = "metrics.process.threads";
  }

  public static final String EMPTY_TAG = "-";
  public static final int DEFAULT_CONTEXT_DEPTH = 6;
  public static final int DEFAULT_METRIC_DEPTH = 4;
  public static final int DEFAULT_TAG_DEPTH = 3;

  public static final String DEFAULT_ENTITY_TABLE_NAME = "MetricsEntity";
  public static final String DEFAULT_METRIC_TABLE_PREFIX = "MetricsTable";
  public static final int DEFAULT_TIME_SERIES_TABLE_ROLL_TIME = 3600;

  public static final String DEFAULT_KAFKA_TOPIC = "metrics";
  public static final int DEFAULT_KAFKA_PARTITION_SIZE = 10;

  public static final int DEFAULT_PROCESSING_THREADS = 1;

  // Number of seconds to subtract from current timestamp when query without "end" time.
  public static final long QUERY_SECOND_DELAY = 2;

  private MetricsConstants() {
  }
}
