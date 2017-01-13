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

package co.cask.cdap.metrics.collect.process;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.internal.io.ASMDatumWriterFactory;
import co.cask.cdap.internal.io.ASMFieldAccessorFactory;
import co.cask.cdap.internal.io.DatumWriter;
import co.cask.cdap.internal.io.ReflectionDatumReader;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.metrics.collect.MessagingMetricsCollectionService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorService}.
 */
public class MessagingMetricsProcessorServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorService.class);
  private static final String topicPrefix = "metrics";
  private static final int partitionSize = 10;
  private static final TopicId metricsTopic = NamespaceId.SYSTEM.topic(topicPrefix);

  private static MessagingService messagingService;

  @BeforeClass
  public static void init() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
        }
      }
    );
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
  }

  @AfterClass
  public static void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testMetricsProcessor() {
    // TODO
  }

}
