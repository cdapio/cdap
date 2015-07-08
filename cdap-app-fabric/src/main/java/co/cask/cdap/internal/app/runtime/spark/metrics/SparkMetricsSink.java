/*
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

package co.cask.cdap.internal.app.runtime.spark.metrics;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.spark.Spark;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Sink} which collects Metrics from {@link Spark} program and used {@link SparkMetricsReporter} to report it
 * to {@link MetricsContext}
 * <p/>
 * This full qualified name of this class is given to spark through the metrics configuration file.
 */
public class SparkMetricsSink implements Sink {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMetricsSink.class);

  private final SparkMetricsReporter reporter;

  public SparkMetricsSink(Properties properties, MetricRegistry registry,
                          org.apache.spark.SecurityManager securityManager) {
    reporter = new SparkMetricsReporter(registry, TimeUnit.SECONDS, TimeUnit.SECONDS, MetricFilter.ALL);
    LOG.debug("Using SparkMetricsSink for reporting metrics: {}", properties);
  }

  @Override
  public void start() {
    reporter.start(1, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    reporter.stop();
  }

  @Override
  public void report() {
    reporter.report();
  }

  /**
   * Generate a properties file which is used to config Spark Metrics in {@link SparkConf}
   *
   * @param file the {@link File} where this file should be generated
   * @return the same File argument provided.
   */
  public static File generateSparkMetricsConfig(File file) throws IOException {
    Properties properties = new Properties();
    properties.setProperty("*.sink.cdap.class", SparkMetricsSink.class.getName());

    try (BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8)) {
      properties.store(writer, null);
      return file;
    }
  }
}
