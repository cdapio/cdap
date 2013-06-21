/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.pusher;

import com.continuuity.metrics.kafka.KafkaPublisher;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class KafkaMetricsPusherService extends AbstractScheduledService implements MetricsPusherService {

  private final KafkaPublisher publisher;

  public KafkaMetricsPusherService(KafkaPublisher publisher) {
    this.publisher = publisher;
  }

  @Override
  protected void runOneIteration() throws Exception {
    
  }

  @Override
  protected ScheduledExecutorService executor() {
    return Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                        .setDaemon(true)
                                                        .setNameFormat("kafka-metrics-pusher")
                                                        .build());
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(1, 1, TimeUnit.SECONDS);
  }
}
