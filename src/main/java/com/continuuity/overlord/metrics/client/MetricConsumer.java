/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client;

/**
 *  Consumer Interface for consuming Metrics from the {@link Queue}
 */
public interface MetricConsumer<T> {
  /**
   * Consumes records from the queue.
   * @param object
   * @throws InterruptedException
   */
  void consume(T object) throws InterruptedException;
}
