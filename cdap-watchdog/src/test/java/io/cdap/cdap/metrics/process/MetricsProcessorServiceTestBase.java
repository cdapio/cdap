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

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.Encoder;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.metrics.MetricsTestBase;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class MetricsProcessorServiceTestBase extends MetricsTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorServiceTestBase.class);

  static final String COUNTER_METRIC_NAME = "counter_metric";
  static final String GAUGE_METRIC_NAME_PREFIX = "gauge_metric";

  static final Map<String, String> METRICS_CONTEXT = ImmutableMap.<String, String>builder()
    .put(Constants.Metrics.Tag.NAMESPACE, "NS_1")
    .put(Constants.Metrics.Tag.APP, "APP_1")
    .put(Constants.Metrics.Tag.SERVICE, "SERVICE_1")
    .put(Constants.Metrics.Tag.RUN_ID, "RUN_1")
    .put(Constants.Metrics.Tag.HANDLER, "HANDLER_1").build();

  private final ByteArrayOutputStream encoderOutputStream = new ByteArrayOutputStream(1024);
  // Map containing expected metrics' names and values
  protected final Map<String, Long> expected = new HashMap<>();
  private final Encoder encoder = new BinaryEncoder(encoderOutputStream);

  void publishMessagingMetrics(int metricIndex, long startTimeSecs, Map<String, String> metricsContext,
                               Map<String, Long> expected, String expectedMetricPrefix,
                               MetricType metricType) {

    try {
      getMetricValuesAddToExpected(metricIndex, startTimeSecs,
                                   metricsContext, expected, expectedMetricPrefix, metricType);
      int numOfTopics = cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM);
      messagingService.publish(
        StoreRequestBuilder.of(NamespaceId.SYSTEM.topic(TOPIC_PREFIX + (metricIndex % numOfTopics)))
          .addPayload(encoderOutputStream.toByteArray()).build());
    } catch (Exception e) {
      LOG.error("Failed to publish metric with index {} to messaging service", metricIndex, e);
    } finally {
      encoderOutputStream.reset();
    }
  }

  /**
   * Returns expected {@link MetricValues} of the given {@link MetricType}. Add the {@link MetricValues} to the
   * {@code expected} metrics map. If the {@link MetricValues} is of type {@code MetricType.COUNTER} and is present
   * in {@code expected}, increment the existing value of it.
   *
   * @param expectedMetricPrefix The prefix added to metric names by {@link MetricStore}
   */
  private MetricValues getMetricValuesAddToExpected(int i, long startTimeSecs, Map<String, String> metricsContext,
                                                    Map<String, Long> expected, String expectedMetricPrefix,
                                                    MetricType metricType)
    throws TopicNotFoundException, IOException {
    MetricValues metric;
    if (MetricType.GAUGE.equals(metricType)) {
      String metricName = GAUGE_METRIC_NAME_PREFIX + i;
      metric =
        new MetricValues(metricsContext, metricName, startTimeSecs, i, metricType);
      expected.put(expectedMetricPrefix + metricName, (long) i);
    } else {
      metric =
        new MetricValues(metricsContext, COUNTER_METRIC_NAME, i, 1, metricType);
      String expectedCounterMetricName = expectedMetricPrefix + COUNTER_METRIC_NAME;
      Long currentValue = expected.get(expectedCounterMetricName);
      if (currentValue == null) {
        expected.put(expectedCounterMetricName, 1L);
      } else {
        expected.put(expectedCounterMetricName, currentValue + 1);
      }
    }

    recordWriter.encode(metric, encoder);
    return metric;
  }

  @Override
  protected List<Module> getAdditionalModules() {
    List<Module> list = new ArrayList<>();
    list.add(new DataSetsModules().getStandaloneModules());
    list.add(new IOModule());
    list.add(Modules.override(
      new NonCustomLocationUnitTestModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceAdminTestModule(),
      new MetricsStoreModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
      }
    }));
    return list;
  }
}
