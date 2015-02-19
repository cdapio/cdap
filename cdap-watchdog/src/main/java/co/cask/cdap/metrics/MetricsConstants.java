/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics;

/**
 * Define constants for byte[] that are needed in multiple classes.
 */
public final class MetricsConstants {

  /**
   * Define constants for metrics configuration keys.
   */
  public static final class ConfigKeys {

    public static final String ENTITY_TABLE_NAME = "metrics.data.entity.tableName";
    public static final String METRICS_TABLE_PREFIX = "metrics.data.table.prefix";
    public static final String TIME_SERIES_TABLE_ROLL_TIME = "metrics.data.table.ts.rollTime";

    // Key prefix for retention seconds. The actual key is suffixed by the table resolution.
    public static final String RETENTION_SECONDS = "metrics.data.table.retention.resolution";

    public static final String SERVER_ADDRESS = "metrics.query.bind.address";
    public static final String SERVER_PORT = "metrics.query.bind.port";

    public static final String KAFKA_TOPIC_PREFIX = "metrics.kafka.topic.prefix";
    public static final String KAFKA_PARTITION_SIZE = "metrics.kafka.partition.size";
    public static final String KAFKA_CONSUMER_PERSIST_THRESHOLD = "metrics.kafka.consumer.persist.threshold";
    public static final String KAFKA_META_TABLE = "metrics.kafka.meta.table";
  }

  // v2 to avoid conflict with data of older metrics system
  public static final String DEFAULT_ENTITY_TABLE_NAME = "metrics.v2.entity";
  public static final String DEFAULT_METRIC_TABLE_PREFIX = "metrics.v2.table";
  public static final int DEFAULT_TIME_SERIES_TABLE_ROLL_TIME = 3600;
  public static final long DEFAULT_RETENTION_HOURS = 2;

  public static final long METRICS_HOUR_RESOLUTION_CUTOFF = 3610;
  public static final long METRICS_MINUTE_RESOLUTION_CUTOFF = 610;

  public static final String DEFAULT_KAFKA_META_TABLE = "metrics.kafka.meta";
  public static final String DEFAULT_KAFKA_TOPIC_PREFIX = "metrics";

  public static final int DEFAULT_KAFKA_CONSUMER_PERSIST_THRESHOLD = 100;
  public static final int DEFAULT_KAFKA_PARTITION_SIZE = 1;

  // Number of seconds to subtract from current timestamp when query without "end" time.
  public static final long QUERY_SECOND_DELAY = 2;

  // for migration purpose
  public static final String EMPTY_TAG = "-";
  public static final int DEFAULT_CONTEXT_DEPTH = 6;
  public static final int DEFAULT_METRIC_DEPTH = 4;
  public static final int DEFAULT_TAG_DEPTH = 3;

  private MetricsConstants() {
  }
}
