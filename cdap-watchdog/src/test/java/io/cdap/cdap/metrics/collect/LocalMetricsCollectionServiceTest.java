/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.metrics.collect;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorManagerService;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import io.cdap.cdap.metrics.process.MetricsConsumerMetaTable;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import io.cdap.cdap.metrics.store.MetricsCleanUpService;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LocalMetricsCollectionService} start/stop lifecycle.
 */
public class LocalMetricsCollectionServiceTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartPublishAndStopWithoutMessagingProcessor() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS, 60);
    cConf.setLong(Constants.Metrics.MINIMUM_RESOLUTION_RETENTION_SECONDS, 3600L);

    final List<MetricValues> persisted = Collections.synchronizedList(new ArrayList<MetricValues>());
    MetricStore metricStore = Mockito.mock(MetricStore.class);
    Mockito.doAnswer(invocation -> {
      Collection<? extends MetricValues> values = invocation.getArgument(0);
      persisted.addAll(values);
      return null;
    }).when(metricStore).add(Mockito.anyCollection());

    MetricsCleanUpService cleanUpService = createCleanUpService(cConf, metricStore);
    LocalMetricsCollectionService service =
        new LocalMetricsCollectionService(cConf, metricStore, cleanUpService);

    service.startAndWait();
    Assert.assertTrue(cleanUpService.isRunning());

    service.getContext(ImmutableMap.of("ns", "default")).increment("processed", 3L);
    service.stopAndWait();

    Assert.assertFalse(cleanUpService.isRunning());
    Assert.assertFalse(persisted.isEmpty());
  }

  @Test
  public void testStartAndStopWithMessagingMetricsProcessorAndFailurePropagation()
      throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Metrics.MESSAGING_TOPIC_NUM, 1);
    cConf.setInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS, 60);
    cConf.setLong(Constants.Metrics.MINIMUM_RESOLUTION_RETENTION_SECONDS, 3600L);

    MetricStore metricStore = Mockito.mock(MetricStore.class);
    final AtomicBoolean extensionWriterClosed = new AtomicBoolean(false);
    final IOException writerCloseFailure = new IOException("extension writer close failure");

    MetricsWriter failingCloseWriter = new MetricsWriter() {
      @Override
      public void initialize(MetricsWriterContext context) {
      }

      @Override
      public void write(Collection<MetricValues> metricValues) {
      }

      @Override
      public String getID() {
        return "failing_ext";
      }

      @Override
      public void close() throws IOException {
        extensionWriterClosed.set(true);
        throw writerCloseFailure;
      }
    };

    MessagingMetricsProcessorManagerService managerService =
        createManagerService(cConf, metricStore, failingCloseWriter);
    MessagingMetricsProcessorServiceFactory factory =
        (Set<Integer> topicNumbers, MetricsContext metricsContext, Integer instanceId) ->
            managerService;

    MetricsCleanUpService cleanUpService = createCleanUpService(cConf, metricStore);
    LocalMetricsCollectionService service =
        new LocalMetricsCollectionService(cConf, metricStore, cleanUpService);
    service.setMessagingMetricsProcessorFactory(factory);

    service.startAndWait();
    Assert.assertTrue(managerService.isRunning());
    Assert.assertTrue(cleanUpService.isRunning());

    try {
      service.stopAndWait();
      Assert.fail("Expected stopAndWait to propagate processor shutdown exception");
    } catch (RuntimeException expected) {
      Assert.assertTrue(extensionWriterClosed.get());
      // Clean-up service must still be stopped even when messagingMetricsProcessor fails on stop
      Assert.assertFalse(cleanUpService.isRunning());
    }
  }

  private static MetricsCleanUpService createCleanUpService(
      CConfiguration cConf, MetricStore metricStore) {
    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricStore.class).toInstance(metricStore);
          }
        });
    return injector.getInstance(MetricsCleanUpService.class);
  }

  private static MessagingMetricsProcessorManagerService createManagerService(
      CConfiguration cConf, MetricStore metricStore, MetricsWriter extensionWriter)
      throws Exception {
    Injector ioInjector = Guice.createInjector(new ConfigModule(cConf), new IOModule());
    final SchemaGenerator schemaGenerator = ioInjector.getInstance(SchemaGenerator.class);
    final DatumReaderFactory readerFactory = ioInjector.getInstance(DatumReaderFactory.class);

    MetricsTable metaTable = Mockito.mock(MetricsTable.class);
    MetricDatasetFactory datasetFactory = Mockito.mock(MetricDatasetFactory.class);
    Mockito.when(datasetFactory.createConsumerMeta())
        .thenReturn(new MetricsConsumerMetaTable(metaTable));

    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Mockito.when(messagingService.fetch(Mockito.any())).thenAnswer(invocation ->
        new AbstractCloseableIterator<RawMessage>() {
          @Override
          protected RawMessage computeNext() {
            return endOfData();
          }

          @Override
          public void close() {
          }
        });

    MetricsWriterProvider writerProvider =
        () -> Collections.singletonMap(extensionWriter.getID(), extensionWriter);

    Constructor<MessagingMetricsProcessorManagerService> ctor =
        MessagingMetricsProcessorManagerService.class.getDeclaredConstructor(
            CConfiguration.class,
            MetricDatasetFactory.class,
            MessagingService.class,
            SchemaGenerator.class,
            DatumReaderFactory.class,
            MetricStore.class,
            MetricsWriterProvider.class,
            Set.class,
            MetricsContext.class,
            long.class,
            int.class);
    ctor.setAccessible(true);
    return ctor.newInstance(
        cConf,
        datasetFactory,
        messagingService,
        schemaGenerator,
        readerFactory,
        metricStore,
        writerProvider,
        Collections.singleton(0),
        new io.cdap.cdap.api.metrics.NoopMetricsContext(),
        1000L,
        0);
  }
}
