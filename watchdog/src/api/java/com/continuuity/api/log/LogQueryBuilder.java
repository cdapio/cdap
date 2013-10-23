/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
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
