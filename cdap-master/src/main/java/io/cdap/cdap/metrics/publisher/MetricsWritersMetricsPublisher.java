/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.metrics.publisher;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.metrics.process.DefaultMetricsWriterContext;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link MetricsPublisher} that writes the published metrics to multiple {@link MetricsWriter}s
 */
public class MetricsWritersMetricsPublisher extends AbstractMetricsPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsWritersMetricsPublisher.class);
  private Map<String, MetricsWriter> metricsWriters;
  private final CConfiguration cConf;
  private final MetricsWriterProvider writerProvider;
  private static final String METRICSWRITERS = "metricswriters";

  @Inject
  public MetricsWritersMetricsPublisher(MetricsWriterProvider writerProvider,
      CConfiguration cConf) {
    this.writerProvider = writerProvider;
    this.cConf = cConf;
  }

  @Override
  public void initialize() {
    if (isInitialized()) {
      LOG.debug("Metrics publisher is already initialized.");
      return;
    }
    LOG.info("Initializing MetricsWritersMetricsPublisher.");
    this.metricsWriters = this.writerProvider.loadMetricsWriters();
    initializeMetricWriters(this.metricsWriters, this.cConf);
  }

  private boolean isInitialized() {
    return this.metricsWriters != null;
  }

  private void initializeMetricWriters(Map<String, MetricsWriter> metricsWriters,
      CConfiguration cConf) {
    File baseDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), METRICSWRITERS);
    for (Map.Entry<String, MetricsWriter> entry : metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      File initStateFile = new File(baseDir, writer.getID());
      try {
        // Metrics context used by MetricsStoreMetricsWriter only, which we don't use here
        // So we can pass no-op context
        DefaultMetricsWriterContext metricsWriterContext =
            new DefaultMetricsWriterContext(new NoopMetricsContext(), cConf, writer.getID());
        writer.initialize(metricsWriterContext);
        if (!initStateFile.exists()) {
          if (!baseDir.exists()) {
            baseDir.mkdirs();
          }
          boolean result = initStateFile.createNewFile();
          LOG.info(
              "Initialization for metric writer {} succeeded. Result of creating initStateFile {} is {}.",
              writer.getID(), initStateFile.getName(), result);
        }
      } catch (Exception e) {
        //enforce at least one correct initialization
        if (!initStateFile.exists()) {
          throw new RuntimeException(
              "Initialization for metric writer " + writer.getID() + " failed. Please fix the "
                  + "errors " + "to proceed.", e);
        } else {
          LOG.error("Initialization for metric writer {} failed. Recheck the configuration.",
              writer.getID(), e);
        }
      }
    }
  }

  @Override
  public void publish(Collection<MetricValues> metrics) throws Exception {
    if (!isInitialized()) {
      throw new IllegalStateException("Initialize publisher before calling publish");
    }
    if (metrics.isEmpty()) {
      return;
    }
    for (Map.Entry<String, MetricsWriter> entry : this.metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      try {
        writer.write(metrics);
      } catch (Exception e) {
        LOG.error("Error encountered while writing metrics to {} metrics writer", writer.getID(),
            e);
        continue;
      }
      LOG.trace("{} metrics persisted using {} metrics writer.", metrics.size(), writer.getID());
    }
  }

  @Override
  public void close() {
    for (Map.Entry<String, MetricsWriter> entry : this.metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      try {
        writer.close();
      } catch (IOException e) {
        LOG.error("Error while closing metrics writer {}.", writer.getID(), e);
      }
    }
  }
}
