/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.log;

/**
 * Builds {@link LogQuery}.
 */
public class LogQueryBuilder {
  // Common hierarchy

  public LogQueryBuilder setAccountId(String accountId) {
    // TODO
    return this;
  }

  public LogQueryBuilder setApplicationId(String applicationId) {
    // TODO
    return this;
  }

  // Different emitter types

  public LogQueryBuilder setFlowId(String flowId) {
    // TODO
    return this;
  }

  public LogQueryBuilder setFlowletId(String flowletId) {
    // TODO
    return this;
  }

  public LogQueryBuilder setQueryId(String queryId) {
    // TODO
    return this;
  }

  public LogQueryBuilder setDatasetId(String datasetId) {
    // TODO
    return this;
  }

  public LogQueryBuilder setStreamId(String streamId) {
    // TODO
    return this;
  }

  // Filtering by user markers

  public LogQueryBuilder setMarkerFilter(LogMarkerFilter markerFilter) {
    // TODO
    return this;
  }

  // Filtering by log level

  public LogQueryBuilder setMinLogLevel(LogMessage.LogLevel markerFilter) {
    // TODO
    return this;
  }

  /**
   * @return constructed {@link LogQuery}.
   */
  public LogQuery build() {
    // TODO
    return null;
  }
}
