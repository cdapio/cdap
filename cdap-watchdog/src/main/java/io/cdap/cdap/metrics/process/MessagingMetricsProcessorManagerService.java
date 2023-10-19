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
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage MessagingMetricsProcessorServices with different MetricsWriters
 */
public class MessagingMetricsProcessorManagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(
      MessagingMetricsProcessorManagerService.class);
  private static final String METRICSWRITERS = "metricswriters";
  private final List<MessagingMetricsProcessorService> metricsProcessorServices;
  private final List<MetricsWriter> metricsWriters;
  private final CConfiguration cConf;
  private final MetricDatasetFactory metricDatasetFactory;
  private final MessagingService messagingService;
  private final SchemaGenerator schemaGenerator;
  private final DatumReaderFactory readerFactory;
  private final MetricStore metricStore;
  private final MetricsWriterProvider metricsWriterProvider;
  private final Set<Integer> topicNumbers;
  private final MetricsContext metricsContext;
  private final long metricsProcessIntervalMillis;
  private final Integer instanceId;

  @Inject
  MessagingMetricsProcessorManagerService(CConfiguration cConf,
      MetricDatasetFactory metricDatasetFactory,
      MessagingService messagingService,
      SchemaGenerator schemaGenerator,
      DatumReaderFactory readerFactory,
      MetricStore metricStore,
      MetricsWriterProvider metricsWriterProvider,
      @Assisted Set<Integer> topicNumbers,
      @Assisted MetricsContext metricsContext,
      @Assisted Integer instanceId) {
    this(cConf, metricDatasetFactory, messagingService,
        schemaGenerator, readerFactory, metricStore, metricsWriterProvider, topicNumbers,
        metricsContext,
        TimeUnit.SECONDS.toMillis(
            cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS)), instanceId);
  }

  @VisibleForTesting
  MessagingMetricsProcessorManagerService(CConfiguration cConf,
      MetricDatasetFactory metricDatasetFactory,
      MessagingService messagingService,
      SchemaGenerator schemaGenerator,
      DatumReaderFactory readerFactory,
      MetricStore metricStore,
      MetricsWriterProvider metricsWriterProvider,
      Set<Integer> topicNumbers,
      MetricsContext metricsContext,
      long metricsProcessIntervalMillis,
      int instanceId) {
    this.metricsWriters = new ArrayList<>();
    this.metricsProcessorServices = new ArrayList<>();
    this.cConf = cConf;
    this.metricDatasetFactory = metricDatasetFactory;
    this.messagingService = messagingService;
    this.schemaGenerator = schemaGenerator;
    this.readerFactory = readerFactory;
    this.metricStore = metricStore;
    this.metricsWriterProvider = metricsWriterProvider;
    this.topicNumbers = topicNumbers;
    this.metricsContext = metricsContext;
    this.metricsProcessIntervalMillis = metricsProcessIntervalMillis;
    this.instanceId = instanceId;
  }

  @Override
  protected void startUp() throws Exception {
    MetricStoreMetricsWriter metricsWriter = new MetricStoreMetricsWriter(metricStore);
    DefaultMetricsWriterContext context = new DefaultMetricsWriterContext(metricsContext,
        cConf, metricsWriter.getID());
    metricsWriter.initialize(context);
    this.metricsWriters.add(metricsWriter);

    for (Map.Entry<String, MetricsWriter> metricsWriterEntry : metricsWriterProvider.loadMetricsWriters()
        .entrySet()) {
      MetricsWriter writer = metricsWriterEntry.getValue();
      this.metricsWriters.add(writer);
      DefaultMetricsWriterContext metricsWriterContext = new DefaultMetricsWriterContext(
          metricsContext,
          cConf, writer.getID());
      initializeMetricWriter(writer, metricsWriterContext);
    }

    String processorKey = String.format("metrics.processor.%s", instanceId);
    for (MetricsWriter metricsExtension : this.metricsWriters) {
      MetricsMetaKeyProvider topicIdMetricsKeyProvider = getKeyProvider(metricsExtension, cConf);
      metricsProcessorServices.add(new MessagingMetricsProcessorService(
          cConf,
          metricDatasetFactory,
          messagingService,
          schemaGenerator,
          readerFactory,
          metricsExtension,
          topicNumbers,
          metricsContext,
          metricsProcessIntervalMillis,
          instanceId,
          new DefaultMetadataHandler(processorKey, topicIdMetricsKeyProvider),
          topicIdMetricsKeyProvider));

    }

    for (MessagingMetricsProcessorService processorService : metricsProcessorServices) {
      processorService.startAndWait();
    }
  }

  @VisibleForTesting
  void initializeMetricWriter(MetricsWriter writer,
      DefaultMetricsWriterContext metricsWriterContext) throws Exception {
    File baseDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), METRICSWRITERS);
    File initStateFile = new File(baseDir, writer.getID());
    try {
      writer.initialize(metricsWriterContext);
      if (!initStateFile.exists()) {
        baseDir.mkdirs();
        boolean result = initStateFile.createNewFile();
        LOG.info(
            "Initialization for metric writer {} succeeded. Result of creating initStateFile {} is {}.",
            writer.getID(), initStateFile.getName(), result);
      }
    } catch (Exception e) {
      //enforce at least one correct initialization
      if (!initStateFile.exists()) {
        throw new Exception(
            "Initialization for metric writer " + writer.getID()
                + " failed. Please fix the errors to proceed.", e);
      } else {
        LOG.error("Initialization for metric writer {} failed. Recheck the configuration.",
            writer.getID(), e);
      }
    }
  }

  private MetricsMetaKeyProvider getKeyProvider(MetricsWriter writer, CConfiguration cConf) {
    boolean useSubscriberInKey = getUseSubscriberInKey(writer, cConf);
    return useSubscriberInKey ? new TopicSubscriberMetricsKeyProvider(writer.getID())
        : new TopicIdMetricsKeyProvider();
  }

  private boolean getUseSubscriberInKey(MetricsWriter writer, CConfiguration cConf) {
    String confKey = String.format(Constants.Metrics.WRITER_USE_SUBSCRIBER_METADATA_KEY,
        writer.getID());
    return cConf.getBoolean(confKey, false);
  }

  @Override
  protected void shutDown() throws Exception {
    Exception exceptions = new Exception();
    for (MessagingMetricsProcessorService processorService : metricsProcessorServices) {
      try {
        processorService.stopAndWait();
        for (MetricsWriter metricsWriter : metricsWriters) {
          metricsWriter.close();
        }
      } catch (Exception e) {
        exceptions.addSuppressed(e);
      }
    }
    if (exceptions.getSuppressed().length > 0) {
      throw exceptions;
    }
  }
}

