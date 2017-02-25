/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default metrics collector implementation using <a href="http://metrics.dropwizard.io">Yammer Metrics</a>.
 *
 * <p>The reporting frequency for this collector can be configured by setting the
 * {@code data.tx.metrics.period} configuration property to the reporting frequency in seconds.
 * </p>
 */
public class DefaultMetricsCollector extends TxMetricsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsCollector.class);

  private final MetricRegistry metrics = new MetricRegistry();
  private JmxReporter jmxReporter;
  private ScheduledReporter reporter;
  private int reportPeriod;
  private ConcurrentMap<String, AtomicLong> gauges = Maps.newConcurrentMap();

  @Override
  public void configure(Configuration conf) {
    // initialize selected output reporter
    reportPeriod = conf.getInt(TxConstants.Metrics.REPORT_PERIOD_KEY, TxConstants.Metrics.REPORT_PERIOD_DEFAULT);
    LOG.info("Configured metrics report to emit every {} seconds", reportPeriod);
    // TODO: reporters should be pluggable based on injection
    jmxReporter = JmxReporter.forRegistry(metrics).build();
    reporter = Slf4jReporter.forRegistry(metrics)
                            .outputTo(LoggerFactory.getLogger("tephra-metrics"))
                            .convertRatesTo(TimeUnit.SECONDS)
                            .convertDurationsTo(TimeUnit.MILLISECONDS)
                            .build();
  }


  @Override
  public void gauge(String metricName, int value, String... tags) {
    AtomicLong gauge = gauges.get(metricName);
    if (gauge == null) {
      final AtomicLong newValue = new AtomicLong();
      if (gauges.putIfAbsent(metricName, newValue) == null) {
        // first to set the value, need to register the metric
        metrics.register(metricName, new Gauge<Long>() {
          @Override
          public Long getValue() {
            return newValue.get();
          }
        });
        gauge = newValue;
      } else {
        // someone else set it first
        gauge = gauges.get(metricName);
      }
    }
    gauge.set(value);
  }

  @Override
  public void histogram(String metricName, int value) {
    metrics.histogram(metricName).update(value);
  }

  @Override
  public void rate(String metricName) {
    metrics.meter(metricName).mark();
  }

  @Override
  public void rate(String metricName, int count) {
    metrics.meter(metricName).mark(count);
  }

  @Override
  protected void startUp() throws Exception {
    jmxReporter.start();
    reporter.start(reportPeriod, TimeUnit.SECONDS);
    LOG.info("Started metrics reporter");
  }

  @Override
  protected void shutDown() throws Exception {
    jmxReporter.stop();
    reporter.stop();
    LOG.info("Stopped metrics reporter");
  }
}
