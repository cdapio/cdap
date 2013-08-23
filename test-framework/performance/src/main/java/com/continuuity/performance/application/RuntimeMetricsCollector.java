/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.runner.BenchmarkRuntimeStats;
import com.continuuity.performance.runner.BenchmarkRuntimeStats.Counter;
import com.continuuity.performance.runner.Metric;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Class for collecting runtime metrics while performance test is being executed.
 */
public final class RuntimeMetricsCollector implements MetricsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMetricsCollector.class);

  private final int interval;
  private final List<Metric> metrics;
  private final String tags;
  private final LinkedBlockingDeque<String> queue;

  private final Stopwatch stopwatch = new Stopwatch();
  private volatile boolean interrupt = false;

  public RuntimeMetricsCollector(LinkedBlockingDeque<String> queue, int interval, List<Metric> metrics, String tags) {
    this.metrics = metrics;
    this.tags = tags;
    this.queue = queue;
    this.interval = interval;
  }

  public void configure(CConfiguration config) {
  }

  @Override
  public void run() {
    try {
      LOG.debug("Initializing metrics collector.");
      stopwatch.start();

      // wake up every interval (i.e. every minute) to report the metrics
      LOG.debug("Starting to collect metrics every {} seconds.", interval);

      for (int seconds = interval; !interrupt; seconds += interval) {
        long nextWakeupMillis = seconds * 1000L;
        long elapsedMillis = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        try {
          if (nextWakeupMillis > elapsedMillis) {
            Thread.sleep(nextWakeupMillis - elapsedMillis);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          interrupt = true;
        }
        LOG.debug("Metrics collector woke up.");

        if (!interrupt) {
          final long unixTime = getCurrentUnixTime();
          for (Metric metric : metrics) {
            enqueueMetric(metric, unixTime);
          }
        }
      } // each interval
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      LOG.debug("Shutting down runtime metrics collector.");
      stopwatch.stop();
    }
  }

  @Override
  public void stop() {
    interrupt = true;
  }

  public void enqueueMetric(Metric metric) {
    enqueueMetric(metric, getCurrentUnixTime());
  }

  public void enqueueMetric(Metric metric, double value) {
    enqueueMetric(metric, getCurrentUnixTime(),  value);
  }

  public void enqueueMetric(Metric metric, long unixTime, double value) {
    enqueueMetricCommand(metric.getPath(), unixTime, value, tags);
  }

  public void enqueueMetric(Metric metric, long unixTime) {
    final Counter counter = BenchmarkRuntimeStats.getCounter(metric);
    if (counter != null) {
      StringBuilder metricTags = new StringBuilder(tags);
      if (StringUtils.isNotEmpty(metric.getParameter(Metric.Parameter.APPLICATION_ID))) {
        metricTags.append("application=" + metric.getParameter(Metric.Parameter.APPLICATION_ID));
        metricTags.append(" ");
      }
      if (StringUtils.isNotEmpty(metric.getParameter(Metric.Parameter.FLOW_ID))) {
        metricTags.append("flow=" + metric.getParameter(Metric.Parameter.FLOW_ID));
        metricTags.append(" ");
      }
      if (StringUtils.isNotEmpty(metric.getParameter(Metric.Parameter.FLOWLET_ID))) {
        metricTags.append("flowlet=" + metric.getParameter(Metric.Parameter.FLOWLET_ID));
      }
      enqueueMetricCommand(metric.getPath(), unixTime, counter.getValue(), metricTags.toString().trim());
    }
  }

  private void enqueueMetricCommand(String metricName, long unixTime, double value, String tags) {
    String cmd = getMetricCommand(metricName, unixTime, value, tags);
    queue.add(cmd);
    LOG.debug("Added metric command '{}' to the metric collector's dispatch queue.", cmd);
  }

  private static long getCurrentUnixTime() {
    return System.currentTimeMillis() / 1000L;
  }

  private static String getMetricCommand(String metricName, long unixTime, double value, String tags) {
    return "put" + " " + metricName + " " + unixTime + " " + value + " " + tags;
  }
}
