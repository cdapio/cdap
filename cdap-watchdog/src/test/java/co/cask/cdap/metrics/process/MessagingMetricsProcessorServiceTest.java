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

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.metrics.MessagingMetricsTestBase;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorService}.
 */
public class MessagingMetricsProcessorServiceTest extends MessagingMetricsTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorServiceTest.class);

  // Intentionally set fetcher persist threshold to small values, so that MessagingMetricsProcessorService
  // internally can exhaust message iterator and complete the loop.
  private static final int MESSAGING_FETCHER_PERSIST_THRESHOLD = 1;
  private static final int PARTITION_SIZE = 2;
  private static final long START_TIME = System.currentTimeMillis() / 1000;

  private ByteArrayOutputStream encoderOutputStream = new ByteArrayOutputStream(1024);
  private Encoder encoder = new BinaryEncoder(encoderOutputStream);

  @Test
  public void testMetricsProcessor() throws Exception {
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < PARTITION_SIZE; i++) {
      partitions.add(i);
    }

    MetricStore metricStore = injector.getInstance(MetricStore.class);
    MessagingMetricsProcessorService metricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), topicPrefix,
                                           partitions, messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore,
                                           MESSAGING_FETCHER_PERSIST_THRESHOLD);

    final Map<String, String> metricsContext = new HashMap<>();
    metricsContext.put(Constants.Metrics.Tag.NAMESPACE, "NS_1");
    metricsContext.put(Constants.Metrics.Tag.APP, "APP_1");
    metricsContext.put(Constants.Metrics.Tag.FLOW, "FLOW_1");

    metricsProcessorService.setMetricsContext(new NoopMetricsContext(metricsContext));
    metricsProcessorService.startAndWait();
    Map<String, Long> expected = new HashMap<>();
    // Publish metrics and record expected metrics
    for (int i = 1; i <= 5; i++) {
      publishMetrics(i, metricsContext, expected);
    }

    Thread.sleep(5000);
    // Stop and restart metricsProcessorService
    metricsProcessorService.stopAndWait();
    metricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), topicPrefix,
                                           partitions, messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore,
                                           MESSAGING_FETCHER_PERSIST_THRESHOLD);
    metricsProcessorService.startAndWait();

    for (int i = 6; i <= 10; i++) {
      publishMetrics(i, metricsContext, expected);
    }

    Thread.sleep(5000);

    // Query metrics from the metricStore and compare them with the expected ones
    assertMetricsResult(metricStore, metricsContext, expected);

    metricsProcessorService.stopAndWait();
  }

  private void publishMetrics(int i, Map<String, String> metricsContext, Map<String, Long> expected) throws IOException, TopicNotFoundException {
    String metricName = "processed" + "_" + i;
    MetricValues metric =
      new MetricValues(metricsContext, metricName, START_TIME + i * 100, i, MetricType.GAUGE);
    recordWriter.encode(metric, encoder);
    messagingService.publish(StoreRequestBuilder.of(metricsTopics[i % PARTITION_SIZE])
                               .addPayloads(encoderOutputStream.toByteArray()).build());
    LOG.info("Published metric: {}", metric);
    encoderOutputStream.reset();
    // TODO correct context format
    expected.put("system." + metricName, (long) i);
  }

  private void assertMetricsResult(MetricStore metricStore, Map<String, String> metricsContext,
                                   Map<String, Long> expected) {
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      MetricDataQuery metricDataQuery =
        new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, metric.getKey(),
                            AggregationFunction.SUM,
                            metricsContext,
                            ImmutableList.<String>of());
      Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);
      MetricTimeSeries timeSeries = Iterables.getOnlyElement(query);
      List<TimeValue> timeValues = timeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      Assert.assertEquals(metric.getValue().longValue(), timeValue.getValue());
    }
  }

  @Override
  protected List<Module> getAdditionalModules() {
    List<Module> list = new ArrayList<>();
    list.add(new DataSetsModules().getStandaloneModules());
    list.add(new IOModule());
    list.add(Modules.override(
      new NonCustomLocationUnitTestModule().getModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
      }
    }));
    return list;
  }
}
