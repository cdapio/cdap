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

package io.cdap.cdap.metrics.process;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link MessagingMetricsProcessorManagerService} start/stop lifecycle and
 * shutdown suppressed-exception aggregation.
 */
public class MessagingMetricsProcessorManagerLifecycleTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartAndStopLifecycleClosesWriters() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    final AtomicBoolean initialized = new AtomicBoolean(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
    MetricsWriter customWriter = new MetricsWriter() {
      @Override
      public void initialize(MetricsWriterContext context) {
        initialized.set(true);
      }

      @Override
      public void write(Collection<MetricValues> metricValues) {
      }

      @Override
      public String getID() {
        return "custom_writer";
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };

    MessagingMetricsProcessorManagerService manager = createManager(
        cConf, () -> Collections.singletonMap(customWriter.getID(), customWriter));

    manager.startAndWait();
    Assert.assertTrue(manager.isRunning());
    Assert.assertTrue(initialized.get());

    manager.stopAndWait();
    Assert.assertFalse(manager.isRunning());
    Assert.assertTrue(closed.get());
  }

  @Test
  public void testStopAggregatesWriterCloseExceptionsAsSuppressed() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    final IOException closeFailure = new IOException("writer close failure");
    MetricsWriter failingWriter = new MetricsWriter() {
      @Override
      public void initialize(MetricsWriterContext context) {
      }

      @Override
      public void write(Collection<MetricValues> metricValues) {
      }

      @Override
      public String getID() {
        return "failing_writer";
      }

      @Override
      public void close() throws IOException {
        throw closeFailure;
      }
    };

    MessagingMetricsProcessorManagerService manager = createManager(
        cConf, () -> Collections.singletonMap(failingWriter.getID(), failingWriter));

    manager.startAndWait();
    try {
      manager.stopAndWait();
      Assert.fail("Expected RuntimeException when MetricsWriter.close() fails");
    } catch (RuntimeException expected) {
      Throwable cause = expected.getCause();
      Assert.assertNotNull(cause);
      Assert.assertTrue(cause.getSuppressed().length >= 1);
      Assert.assertSame(closeFailure, cause.getSuppressed()[0]);
    }
  }

  private static MessagingMetricsProcessorManagerService createManager(
      CConfiguration cConf, MetricsWriterProvider writerProvider) throws Exception {
    Injector ioInjector = Guice.createInjector(new ConfigModule(cConf), new IOModule());
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

    return new MessagingMetricsProcessorManagerService(
        cConf,
        datasetFactory,
        messagingService,
        ioInjector.getInstance(SchemaGenerator.class),
        ioInjector.getInstance(DatumReaderFactory.class),
        Mockito.mock(MetricStore.class),
        writerProvider,
        Collections.singleton(0),
        new NoopMetricsContext(),
        1000L,
        0);
  }
}
