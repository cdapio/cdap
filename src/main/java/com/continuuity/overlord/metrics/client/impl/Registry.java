/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.RecordBuilder;
import com.continuuity.overlord.metrics.client.annotation.Metric;
import com.continuuity.overlord.metrics.client.counters.Counter;
import com.continuuity.overlord.metrics.client.counters.MutableCounter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Registry holds all the instances of classes that have been instrumented
 * with client.
 */
public class Registry {
  private static final Logger LOG = LoggerFactory.getLogger(Registry.class);
  private final Map<String, MutableCounter> metricsMap = Maps.newLinkedHashMap();
  private final Info info;

  /**
   * Constructor
   * @param name  Name of the registry for debugging purpose.
   */
  public  Registry(String name) {
    this.info = new InfoImpl(name, name);
  }

  /**
   * Creates a new counter
   * @param name   name of the counter
   * @param desc    description of the counter
   * @param value   value associated with the counter.
   * @return {@link Counter} object
   */
  public Counter newCounter(String name, String desc, Metric.Type type, int value) {
    return newCounter(new InfoImpl(name, desc), type, value);
  }

  /**
   * Creates a new counter
   * @param info    name/description of counter
   * @param value   value associated with the counter
   * @return {@link Counter} object
   */
  public synchronized Counter newCounter(Info info, Metric.Type type, int value) {
    Counter counter = new Counter(info, type, value);
    metricsMap.put(info.getName(), counter);
    return counter;
  }


  /**
   * @return Collection of mutable client.
   */
  Collection<MutableCounter> getMutableMetrics() {
    return metricsMap.values();
  }

  /**
   * Takes a snapshot of all the mutable counters that are held in the
   * registry.
   * @param builder
   */
  public synchronized void snapshot(RecordBuilder builder) {
    for (MutableCounter metric : getMutableMetrics()) {
      metric.snapshot(builder);
    }
  }

}
