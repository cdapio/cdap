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

import com.google.common.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MessagingMetricsProcessorService} schema exception propagation and
 * lifecycle.
 */
public class MessagingMetricsProcessorServiceTest {

  @Test
  public void testCheckedUnsupportedTypeExceptionWrappedInRuntimeException() throws Exception {
    final UnsupportedTypeException schemaError =
        new UnsupportedTypeException("unsupported MetricValues schema");
    SchemaGenerator failingGenerator = Mockito.mock(SchemaGenerator.class);
    Mockito.when(failingGenerator.generate(MetricValues.class)).thenThrow(schemaError);

    TopicIdMetricsKeyProvider keyProvider = new TopicIdMetricsKeyProvider();
    try {
      new MessagingMetricsProcessorService(
          CConfiguration.create(),
          Mockito.mock(MetricDatasetFactory.class),
          Mockito.mock(MessagingService.class),
          failingGenerator,
          Mockito.mock(DatumReaderFactory.class),
          new MetricStoreMetricsWriter(Mockito.mock(MetricStore.class)),
          Collections.singleton(0),
          new NoopMetricsContext(),
          1000L,
          0,
          new DefaultMetadataHandler("metrics.processor.0", keyProvider),
          keyProvider);
      Assert.fail("Expected RuntimeException wrapping UnsupportedTypeException");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(schemaError, actual.getCause());
    }
  }

  @Test
  public void testUncheckedRuntimeExceptionIsNotDoubleWrapped() throws Exception {
    final IllegalArgumentException readerError = new IllegalArgumentException("bad reader type");
    Schema dummySchema = Schema.of(Schema.Type.STRING);
    SchemaGenerator generator = Mockito.mock(SchemaGenerator.class);
    Mockito.when(generator.generate(MetricValues.class)).thenReturn(dummySchema);

    DatumReaderFactory failingReaderFactory = Mockito.mock(DatumReaderFactory.class);
    Mockito.when(failingReaderFactory.create(TypeToken.of(MetricValues.class), dummySchema))
        .thenThrow(readerError);

    TopicIdMetricsKeyProvider keyProvider = new TopicIdMetricsKeyProvider();
    try {
      new MessagingMetricsProcessorService(
          CConfiguration.create(),
          Mockito.mock(MetricDatasetFactory.class),
          Mockito.mock(MessagingService.class),
          generator,
          failingReaderFactory,
          new MetricStoreMetricsWriter(Mockito.mock(MetricStore.class)),
          Collections.singleton(0),
          new NoopMetricsContext(),
          1000L,
          0,
          new DefaultMetadataHandler("metrics.processor.0", keyProvider),
          keyProvider);
      Assert.fail("Expected IllegalArgumentException without double-wrapping");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(readerError, actual);
    }
  }

  @Test
  public void testStartAndStopLifecycle() throws Exception {
    CConfiguration cConf = CConfiguration.create();
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

    TopicIdMetricsKeyProvider keyProvider = new TopicIdMetricsKeyProvider();
    MessagingMetricsProcessorService service = new MessagingMetricsProcessorService(
        cConf,
        datasetFactory,
        messagingService,
        ioInjector.getInstance(SchemaGenerator.class),
        ioInjector.getInstance(DatumReaderFactory.class),
        new MetricStoreMetricsWriter(Mockito.mock(MetricStore.class)),
        Collections.singleton(0),
        new NoopMetricsContext(),
        1000L,
        0,
        new DefaultMetadataHandler("metrics.processor.0", keyProvider),
        keyProvider);

    service.startAsync().awaitRunning();
    Assert.assertTrue(service.isRunning());
    service.stopAsync().awaitTerminated();
    Assert.assertFalse(service.isRunning());
  }
}
