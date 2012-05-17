/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client;

/**
 *  Specifies an abstract concept source that are actually the source of
 *  client. The sources are registered with
 *  {@link MetricsController#register(Object)}
 */
public interface MetricSource {

  /**
   * Samples the client from the source.
   * @param builder
   */
  public void getMetrics(RecordBuilder builder);

}
