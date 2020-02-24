/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Manage MessagingMetricsProcessorServices with different MetricsWriters
 */
public class MessagingMetricsProcessorManagerService extends AbstractIdleService {

  private final List<MessagingMetricsProcessorService> metricsProcessorServices;
  private final List<MetricsWriter> metricsWriters;

  @Inject
  MessagingMetricsProcessorManagerService(CConfiguration cConf,
                                          MetricDatasetFactory metricDatasetFactory,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          @Assisted Set<Integer> topicNumbers,
                                          @Assisted MetricsContext metricsContext,
                                          @Assisted Integer instanceId) {
    this(cConf, metricDatasetFactory, messagingService,
         schemaGenerator, readerFactory, metricStore, topicNumbers, metricsContext,
         TimeUnit.SECONDS.toMillis(cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS)), instanceId);
  }

  @VisibleForTesting
  MessagingMetricsProcessorManagerService(CConfiguration cConf,
                                          MetricDatasetFactory metricDatasetFactory,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          Set<Integer> topicNumbers,
                                          MetricsContext metricsContext,
                                          long metricsProcessIntervalMillis,
                                          int instanceId) {
    this.metricsWriters = new ArrayList<>();
    this.metricsProcessorServices = new ArrayList<>();
    MetricStoreMetricsWriter metricsWriter = new MetricStoreMetricsWriter(metricStore);
    metricsWriter.initialize(metricsContext);
    this.metricsWriters.add(metricsWriter);

    for (MetricsWriter metricsExtension : this.metricsWriters) {
      metricsProcessorServices.add(new MessagingMetricsProcessorService(cConf,
                                                                        metricDatasetFactory,
                                                                        messagingService,
                                                                        schemaGenerator,
                                                                        readerFactory,
                                                                        metricsExtension,
                                                                        topicNumbers,
                                                                        metricsContext,
                                                                        metricsProcessIntervalMillis,
                                                                        instanceId));

    }
  }

  @Override
  protected void startUp() throws Exception {
    for (MessagingMetricsProcessorService processorService : metricsProcessorServices) {
      processorService.startAndWait();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    Exception exceptions = new Exception();
    for (MessagingMetricsProcessorService processorService : metricsProcessorServices) {
      try {
        processorService.stopAndWait();
      } catch (Exception e) {
        exceptions.addSuppressed(e);
      }
    }
    if (exceptions.getSuppressed().length > 0) {
      throw exceptions;
    }
  }
}

