/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.data.Interpolator;
import com.google.common.base.Preconditions;

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
  private MetricsScope scope;
  private Interpolator interpolator;

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

  MetricsRequestBuilder setScope(MetricsScope scope) {
    this.scope = scope;
    return this;
  }

  MetricsRequestBuilder setInterpolator(Interpolator interpolator) {
    this.interpolator = interpolator;
    return this;
  }

  MetricsRequest build() {
    return new MetricsRequestImpl(requestURI, contextPrefix, runId, metricPrefix,
                                  tagPrefix, startTime, endTime, type, count, scope, interpolator);
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
    private MetricsScope scope;
    private Interpolator interpolator;

    public MetricsRequestImpl(URI requestURI, String contextPrefix, String runId, String metricPrefix, String tagPrefix,
                              long startTime, long endTime, Type type, int count, MetricsScope scope,
                              Interpolator interpolator) {
      Preconditions.checkNotNull(scope);
      this.contextPrefix = contextPrefix;
      this.requestURI = requestURI;
      this.runId = runId;
      this.metricPrefix = metricPrefix;
      this.tagPrefix = tagPrefix;
      this.startTime = startTime;
      this.endTime = endTime;
      this.type = type;
      this.count = count;
      this.scope = scope;
      this.interpolator = interpolator;
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

    @Override
    public Interpolator getInterpolator() {
      return interpolator;
    }

    @Override
    public MetricsScope getScope() {
      return scope;
    }
  }
}
