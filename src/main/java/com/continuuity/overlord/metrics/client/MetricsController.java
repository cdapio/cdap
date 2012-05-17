/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client;

import com.continuuity.overlord.metrics.client.impl.*;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 */
public class MetricsController {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsController.class);
  private final MetricCollectorAdapter metricCollectorAdapter;
  private final List<MetricSource> metricSources = Lists.newArrayList();
  private RecordCollector recordCollector;
  private Timer timer;

  /**
   * Constructor
   * @param metricCollectorAdapter   Adaptor for sink
   */
  @Inject
  public MetricsController(MetricCollectorAdapter metricCollectorAdapter) {
    this.metricCollectorAdapter = metricCollectorAdapter;
    this.recordCollector = new RecordCollectorImpl();
  }

  /**
   * Registers the metricSources that have client that need to be monitored.
   * @param source  A source class with counters that need to be monitored
   * @param <T>
   * @return Returns the objects itself.
   */
  public synchronized<T> T register(T source) {
    MetricSource s = new MetricSourceImpl(source);
    metricSources.add(s);
    return source;
  }

  /**
   *   Registers a source to tracked for client - associates the source
   *   with a user defined name.
   *
   * @param name  Name of the source - to make it readable
   * @param source  Source object
   * @param <T>
   * @return  Source object
   */
  public synchronized<T> T register (String name, T source) {
    MetricSource s = new MetricSourceImpl(name, source);
    metricSources.add(s);
    return source;
  }

  /**
   * Starts {@link MetricsController} passing in the period at which
   * the controller needs to take a snapshot of the client. This period
   * is specified in seconds. Currently, all the metricSources will be snapshotted
   * at the same interval.
   *
   * @param period in seconds at which the client needs to be snapshotted
   */
  public synchronized void start(int period) {
    startTimer(period);
    metricCollectorAdapter.start();
  }

  /**
   * Stops the {@link MetricsController}
   */
  public synchronized void stop() {
    metricCollectorAdapter.stop();
    stopTimer();
  }

  private synchronized void startTimer(int period) {
    period = 1000*period;
    timer = new Timer("Timer", true);
    timer.scheduleAtFixedRate(new TimerTask() {
      public void run() {
        try {
          onTimerEvent();
        } catch (Exception e) {
        }
      }
    }, period, period);
  }

  private synchronized void stopTimer() {
    if (timer == null) {
      return;
    }
    timer.cancel();
    timer = null;
  }

  synchronized void onTimerEvent() {
    Buffer buffer = sampleMetrics();
    metricCollectorAdapter.putMetrics(buffer);
  }

  synchronized Buffer sampleMetrics() {
    recordCollector.clear();

    BufferBuilder bufferBuilder = new BufferBuilder();
    RecordBuilder builder = recordCollector.addRecord("System");

    // Iterate through all metricSources and for each source take a snapshot
    // of metric and add it to builder.
    for(MetricSource metricSource : metricSources) {
      metricSource.getMetrics(builder);
    }

    bufferBuilder.add("S", recordCollector.getRecords());
    return bufferBuilder.getBuffer();
  }

}
