/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging;

/**
 * Logging configuration helper.
 */
public final class LoggingConfiguration {
  // Common between Distributed and Single Node
  public static final String LOG_PATTERN = "log.pattern";
  public static final String LOG_BASE_DIR = "log.base.dir";
  @Deprecated
  public static final String LOG_FILE_SYNC_INTERVAL_BYTES = "log.file.sync.interval.bytes";

  public static final String KAFKA_SEED_BROKERS = "kafka.seed.brokers";
  public static final String LOG_RETENTION_DURATION_DAYS = "log.retention.duration.days";
  public static final String LOG_MAX_FILE_SIZE_BYTES = "log.max.file.size.bytes";
  public static final String KAFKA_PRODUCER_TYPE = "kafka.producer.type";
  public static final String KAFKA_PRODUCER_BUFFER_MS = "kafka.producer.buffer.ms";
  public static final String LOG_CLEANUP_MAX_NUM_FILES = "log.cleanup.max.num.files";

  // Defaults
  public static final String DEFAULT_LOG_PATTERN = "%d{ISO8601} - %-5p [%t:%c{1}@%L] - %m%n";
  public static final String DEFAULT_KAFKA_PRODUCER_TYPE = "async";

  public static final long DEFAULT_KAFKA_PRODUCER_BUFFER_MS = 1000;

  private LoggingConfiguration() {}
}
