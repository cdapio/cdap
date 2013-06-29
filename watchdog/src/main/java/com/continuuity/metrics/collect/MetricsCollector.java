/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

/**
 * A MetricCollector allows client publish counter metrics.
 */
public interface MetricsCollector {

  String getName();

  void gauge(int value, String...tags);
}
