/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.ProgramResourceReporter;
import co.cask.cdap.common.metrics.MetricsCollector;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation of a {@link co.cask.cdap.app.runtime.ProgramResourceReporter}
 * writes out resource metrics at a fixed rate that defaults to 60 seconds, but can be specified
 * in the constructor.
 */
public abstract class AbstractResourceReporter extends AbstractScheduledService implements ProgramResourceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramResourceReporter.class);
  private static final int DEFAULT_REPORT_INTERVAL = 20;
  protected static final String METRIC_CONTAINERS = "resources.used.containers";
  protected static final String METRIC_MEMORY_USAGE = "resources.used.memory";
  protected static final String METRIC_VIRTUAL_CORE_USAGE = "resources.used.vcores";

  protected final MetricsCollector metricsCollector;

  private final LoadingCache<Map<String, String>, MetricsCollector> programMetricsCollectors;

  private final int reportInterval;

  private volatile ScheduledExecutorService executor;

  protected AbstractResourceReporter(MetricsCollector metricsCollector) {
    this(metricsCollector, DEFAULT_REPORT_INTERVAL);
  }

  protected AbstractResourceReporter(final MetricsCollector metricsCollector, int interval) {
    this.metricsCollector = metricsCollector;
    this.reportInterval = interval;
    this.programMetricsCollectors = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<Map<String, String>, MetricsCollector>() {
        @Override
        public MetricsCollector load(Map<String, String> key) throws Exception {
          return metricsCollector.childCollector(key);
        }
      });
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

  protected void sendMetrics(Map<String, String> context, int containers, int memory, int vcores) {
    LOG.trace("Reporting resources: (containers, memory, vcores) = ({}, {}, {})", containers, memory, vcores);
    MetricsCollector metricsCollector = programMetricsCollectors.getUnchecked(context);
    metricsCollector.gauge(METRIC_CONTAINERS, containers);
    metricsCollector.gauge(METRIC_MEMORY_USAGE, memory);
    metricsCollector.gauge(METRIC_VIRTUAL_CORE_USAGE, vcores);
  }

  protected MetricsCollector getCollector() {
    return metricsCollector;
  }
}
