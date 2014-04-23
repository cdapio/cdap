package com.continuuity.internal.app.runtime;

import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation of a {@link com.continuuity.app.runtime.ProgramResourceReporter}
 * writes out resource metrics at a fixed rate that defaults to 60 seconds, but can be specified
 * in the constructor.
 */
public abstract class AbstractResourceReporter extends AbstractScheduledService implements ProgramResourceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramResourceReporter.class);
  private static final int DEFAULT_REPORT_INTERVAL = 20;
  protected static final String METRIC_CONTAINERS = "resources.used.containers";
  protected static final String METRIC_MEMORY_USAGE = "resources.used.memory";
  protected static final String METRIC_VIRTUAL_CORE_USAGE = "resources.used.vcores";

  protected final MetricsCollectionService collectionService;
  private final int reportInterval;

  private volatile ScheduledExecutorService executor;

  protected AbstractResourceReporter(MetricsCollectionService collectionService) {
    this(collectionService, DEFAULT_REPORT_INTERVAL);
  }

  protected AbstractResourceReporter(MetricsCollectionService collectionService, int interval) {
    this.collectionService = collectionService;
    this.reportInterval = interval;
  }

  protected void runOneIteration() throws Exception {
    reportResources();
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, reportInterval, TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("reporter-scheduler"));
    return executor;
  }

  protected void sendMetrics(String context, int containers, int memory, int vcores) {
    LOG.trace("reporting resources in context {}: (containers, memory, vcores) = ({}, {}, {})",
              context, containers, memory, vcores);
    MetricsCollector collector = collectionService.getCollector(MetricsScope.REACTOR, context, "0");
    collector.gauge(METRIC_CONTAINERS, containers);
    collector.gauge(METRIC_MEMORY_USAGE, memory);
    collector.gauge(METRIC_VIRTUAL_CORE_USAGE, vcores);
  }

  protected MetricsCollector getCollector(String context) {
    return collectionService.getCollector(MetricsScope.REACTOR, context, "0");
  }
}
