/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.metrics.process;

import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Process metrics by consuming metrics being published.
 */
public abstract class AbstractMetricsProcessorService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricsProcessorService.class);

  private final MetricDatasetFactory metricDatasetFactory;
  protected AbstractConsumerMetaTable metaTable;
  protected volatile boolean stopping = false;

  public AbstractMetricsProcessorService(MetricDatasetFactory metricDatasetFactory) {
    this.metricDatasetFactory = metricDatasetFactory;
  }

  @Override
  protected String getServiceName() {
    return this.getClass().getSimpleName();
  }

  protected AbstractConsumerMetaTable getMetaTable(Class<?> metricsProcessorServiceClass) {
    while (metaTable == null) {
      if (stopping) {
        LOG.info("We are shutting down, giving up on acquiring KafkaConsumerMetaTable.");
        break;
      }
      try {
        metaTable = metricDatasetFactory.createKafkaConsumerMeta(metricsProcessorServiceClass);
      } catch (Exception e) {
        LOG.warn("Cannot access kafka consumer metaTable, will retry in 1 sec.");
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    return metaTable;
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Shutdown is triggered.");
    stopping = true;
    super.triggerShutdown();
  }
}
