/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.runner.Metric;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

/**
 *
 */
public final class MensaMetricsReporter  {
  static final Logger LOG = LoggerFactory.getLogger(MensaMetricsReporter.class);

  private final String tsdbHostName;
  private final int tsdbPort;
  private final RuntimeMetricsCollector collector;
  private final LinkedBlockingDeque<String> dispatchQueue;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private Future futureCollector;
  private Future futureDispatcher;

  public MensaMetricsReporter(CConfiguration config, List<String> metricNames, String tags, int interval) {
    if (StringUtils.isNotEmpty(config.get("opentsdb.server.address"))) {
      tsdbHostName = config.get("opentsdb.server.address");
    } else {
      tsdbHostName = "localhost";
    }
    if (StringUtils.isNotEmpty(config.get("opentsdb.server.port"))) {
      tsdbPort = Integer.valueOf(config.get("opentsdb.server.port"));
    } else {
      tsdbPort = 4242;
    }
    String extraTags = config.get("perf.tags");
    if (StringUtils.isNotEmpty(extraTags)) {
      tags = tags + extraTags.replace(",", " ");
      tags = tags.trim();
    }
    dispatchQueue = new LinkedBlockingDeque<String>(50000);
    collector = new RuntimeMetricsCollector(dispatchQueue, interval, getMetricsInstances(metricNames), tags);
    init();
  }

  private static List<Metric> getMetricsInstances(List<String> metricPaths) {
    List<Metric> metricInstances = new ArrayList<Metric>(metricPaths.size());
    for (String metricPath : metricPaths) {
      metricInstances.add(new Metric(metricPath));
    }
    return metricInstances;
  }

  public MensaMetricsReporter(String tsdbHostName, int tsdbPort, List<String> metricNames, String tags, int interval) {
    this.tsdbHostName = tsdbHostName;
    this.tsdbPort = tsdbPort;
    dispatchQueue = new LinkedBlockingDeque<String>(50000);
    collector = new RuntimeMetricsCollector(dispatchQueue, interval, getMetricsInstances(metricNames), tags);
    init();
  }

  private void init() {
    // Start the metrics collector thread.
    futureCollector = executorService.submit(collector);
  }

  public void reportNow(String metricName, double value) {
    collector.enqueueMetric(new Metric(metricName), value);
  }

  public void reportNow(String metricName) {
    collector.enqueueMetric(new Metric(metricName));
  }

  public final void shutdown() {
    // Shutdown the metricsDispatcher thread.
    LOG.debug("Stopping metrics collector thread.");
    collector.stop();
    futureCollector.cancel(true);

    LOG.debug("Shutting down executor service of metrics collector and dispatcher.");
    executorService.shutdown();
  }
}
