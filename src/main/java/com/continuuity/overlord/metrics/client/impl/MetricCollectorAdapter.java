/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.MetricCollector;
import com.continuuity.overlord.metrics.client.Record;
import com.continuuity.overlord.metrics.client.MetricConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MetricCollectorAdapter
 */
public class MetricCollectorAdapter implements MetricConsumer<Buffer> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollectorAdapter.class);
  private final MetricCollector metricCollector;
  private final SmartQueue<Buffer> queue;
  private volatile boolean stopping = false;
  private final Thread sinkThread;

  public MetricCollectorAdapter(MetricCollector metricCollector) throws Exception {
    this.metricCollector = checkNotNull(metricCollector, "metricCollector object");
    this.queue = new SmartQueue<Buffer>(1000);
    sinkThread = new Thread() {
      @Override public void run() {
        publishMetricsFromQueue();
      }
    };
    metricCollector.init();
    sinkThread.setName("MetricCollectorAdapter");
    sinkThread.setDaemon(true);
  }

  public boolean putMetrics(Buffer buffer ) {
    if (queue.enqueue(buffer)) return true;
    return false;
  }

  void publishMetricsFromQueue() {
    while(! stopping) {
      try {
        queue.consume(this);
      }  catch (InterruptedException e) {
         // Thread has been interrupted.
      }
    }
  }
  
  @Override
  public void consume(Buffer buffer) {
    for(BufferEntry entry : buffer) {
      for(Record record :  entry.getRecords()) {
        try {
          metricCollector.push(record);
        } catch(IOException e)  {
          LOG.warn("Failed to push record to metricCollector ", e);
        }
      }
    }
    
    try {
      metricCollector.flush();
    } catch (IOException e) {
      LOG.warn("Failed while attempting to flush metricCollector ", e);
    }
  }

  public void start() {
    sinkThread.start();
  }

  public void stop() {
    stopping = true;
    sinkThread.interrupt();
    try {
      sinkThread.join();
    }
    catch (InterruptedException e) {
    }
  }

  public MetricCollector getMetricCollector() {
    return metricCollector;
  }
}
