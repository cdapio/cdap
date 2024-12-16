/*
 *Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.stackdriver.logs;

import java.util.Collections;
import java.util.Map;

/**
 * Metrics writer configuration.
 */
public class LoggingConfig {

  private static final long DEFAULT_POLL_INTERVAL = 300;
  public static final LoggingConfig EMPTY = new LoggingConfig(Collections.emptyMap(),
      DEFAULT_POLL_INTERVAL,
      "logging.googleapis.com:443");
  private final Map<String, LogsMapping> stackdriverMapping;
  private final Long pollInterval;
  private final String loggingEndpoint;

  public LoggingConfig(Map<String, LogsMapping> stackdriverMapping, Long pollInterval,
      String loggingEndpoint) {
    this.stackdriverMapping = stackdriverMapping;
    this.pollInterval = pollInterval;
    this.loggingEndpoint = loggingEndpoint;
  }

  public Map<String, LogsMapping> getLogsMapping() {
    // can be null when deserialized through gson
    return stackdriverMapping == null ? Collections.emptyMap()
        : Collections.unmodifiableMap(stackdriverMapping);
  }

  public long getPollInterval() {
    // return default value if pollInterval is not defined in the JSON
    return pollInterval == null ? DEFAULT_POLL_INTERVAL : pollInterval;
  }

  public String getloggingEndpoint() {
    return loggingEndpoint;
  }
}