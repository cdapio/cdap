/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import java.net.URI;

/**
 * An internal builder for creating MetricsRequest.
 */
final class MetricsRequestBuilder {
  private final URI requestURI;
  private String contextPrefix;
  private String runId;
  private String metricPrefix;
  private String tagPrefix;
  private long startTime;
  private long endTime;
  private MetricsRequest.Type type;
  private int count;

  MetricsRequestBuilder(URI requestURI) {
    this.requestURI = requestURI;
  }

  MetricsRequestBuilder setContextPrefix(String contextPrefix) {
    this.contextPrefix = contextPrefix;
    return this;
  }

  MetricsRequestBuilder setRunId(String runId) {
    this.runId = runId;
    return this;
  }

  MetricsRequestBuilder setMetricPrefix(String metricPrefix) {
    this.metricPrefix = metricPrefix;
    return this;
  }

  MetricsRequestBuilder setTagPrefix(String tagPrefix) {
    this.tagPrefix = tagPrefix;
    return this;
  }

  MetricsRequestBuilder setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  MetricsRequestBuilder setEndTime(long endTime) {
    this.endTime = endTime;
    return this;
  }

  MetricsRequestBuilder setType(MetricsRequest.Type type) {
    this.type = type;
    return this;
  }

  MetricsRequestBuilder setCount(int count) {
    this.count = count;
    return this;
  }

  MetricsRequest build() {
    return new MetricsRequestImpl(requestURI, contextPrefix, runId, metricPrefix,
                                  tagPrefix, startTime, endTime, type, count);
  }

  private static class MetricsRequestImpl implements MetricsRequest {
    private final URI requestURI;
    private final String contextPrefix;
    private final String runId;
    private final String metricPrefix;
    private final String tagPrefix;
    private final long startTime;
    private final long endTime;
    private final Type type;
    private final int count;

    public MetricsRequestImpl(URI requestURI, String contextPrefix, String runId, String metricPrefix,
                              String tagPrefix, long startTime, long endTime, Type type, int count) {
      this.contextPrefix = contextPrefix;
      this.requestURI = requestURI;
      this.runId = runId;
      this.metricPrefix = metricPrefix;
      this.tagPrefix = tagPrefix;
      this.startTime = startTime;
      this.endTime = endTime;
      this.type = type;
      this.count = count;
    }

    @Override
    public URI getRequestURI() {
      return requestURI;
    }

    @Override
    public String getContextPrefix() {
      return contextPrefix;
    }

    @Override
    public String getRunId() {
      return runId;
    }

    @Override
    public String getMetricPrefix() {
      return metricPrefix;
    }

    @Override
    public String getTagPrefix() {
      return tagPrefix;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public long getEndTime() {
      return endTime;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public int getCount() {
      return count;
    }
  }
}
