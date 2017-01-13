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

package co.cask.cdap.metrics;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.internal.io.ASMDatumWriterFactory;
import co.cask.cdap.internal.io.ASMFieldAccessorFactory;
import co.cask.cdap.internal.io.DatumWriter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * TestBase for testing {@link co.cask.cdap.metrics.collect.MessagingMetricsCollectionService} 
 * and {@link co.cask.cdap.metrics.process.MessagingMetricsProcessorService}
 */
public abstract class MessagingMetricsTestBase {
  
  protected String topicPrefix;
  protected int partitionSize;
  protected TopicId[] metricsTopics;

  protected MessagingService messagingService;
  protected TypeToken<MetricValues> metricValueType;
  protected Schema schema;
  protected DatumWriter<MetricValues> metricRecordDatumWriter;

  @Before
  public void init() throws IOException, UnsupportedTypeException {
    Injector injector = Guice.createInjector(
      new ConfigModule(getCConf()),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
        }
      }
    );
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    topicPrefix = cConf.get(Constants.Metrics.TOPIC_PREFIX);
    partitionSize = cConf.getInt(Constants.Metrics.KAFKA_PARTITION_SIZE);
    metricsTopics = new TopicId[partitionSize];
    for (int i = 0; i < partitionSize; i++) {
      metricsTopics[i] = NamespaceId.SYSTEM.topic(topicPrefix + "_" + i);
    }
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    metricValueType = TypeToken.of(MetricValues.class);
    schema = new ReflectionSchemaGenerator().generate(metricValueType.getType());
    metricRecordDatumWriter = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
      .create(metricValueType, schema);
  }

  @After
  public void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }
  
  protected abstract CConfiguration getCConf();
}
