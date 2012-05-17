/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.RecordBuilder;
import com.continuuity.overlord.metrics.client.metric.AbstractMetric;
import com.continuuity.overlord.metrics.client.metric.IntegerMetric;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 *
 *
 */
public class RecordBuilderImpl implements RecordBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(RecordBuilderImpl.class);
  private final long timestamp;
  private final List<AbstractMetric> metrics;
  private final Info info;

  public RecordBuilderImpl(Info info) {
    timestamp = System.currentTimeMillis();
    metrics = Lists.newArrayList();
    this.info = info;
  }

  public RecordImpl getRecord() {
    return new RecordImpl(
      timestamp,
      info,
      Collections.unmodifiableList(metrics)
    ) ;
  }

  public RecordBuilderImpl addMetric(AbstractMetric metric) {
    metrics.add(metric);
    return this;
  }

  public RecordBuilderImpl addCounter(Info info, int value) {
    metrics.add(new IntegerMetric(info, value));
    return this;
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("client", metrics)
      .add("info", info)
      .toString();
  }

}
