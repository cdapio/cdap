/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.events;

import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.events.dummy.DummyEventWriter;
import io.cdap.cdap.metrics.collect.AggregatedMetricsCollectionService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.spi.events.ExecutionMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for metrics published by ProgramStatusEventPublisher
 */
public class ProgramStatusEventPublisherMetricsTest {

  private static final String MOCKED_NOTIFICATION_FILENAME = "mocked_pipeline_notification.json";

  @Test
  public void testMetrics() throws IOException {
    MetricsProvider mockMetricsProvider = getMockMetricsProvider();
    List<MetricValues> metricValuesList = new ArrayList<>();
    MetricsCollectionService mockMetricsCollectionService = getMockCollectionService(metricValuesList);
    mockMetricsCollectionService.startAndWait();
    ProgramStatusEventPublisher programStatusEventPublisher = new ProgramStatusEventPublisher(
      CConfiguration.create(), null, mockMetricsCollectionService, null, mockMetricsProvider
    );
    programStatusEventPublisher.initialize(Collections.singleton(new DummyEventWriter()));
    programStatusEventPublisher.processMessages(null, getMockNotification());
    mockMetricsCollectionService.stopAndWait();
    Assert.assertSame(1, metricValuesList.size());
    Assert.assertTrue(containsMetric(metricValuesList.get(0), Constants.Metrics.ProgramEvent.PUBLISHED_COUNT));
  }

  private MetricsProvider getMockMetricsProvider() {
    return runId -> new ExecutionMetrics[0];
  }

  private Iterator<ImmutablePair<String, Notification>> getMockNotification() throws IOException {
    InputStream resourceAsStream = ClassLoader.getSystemClassLoader().getResourceAsStream(MOCKED_NOTIFICATION_FILENAME);
    if (resourceAsStream == null) {
      Assert.fail(String.format("Expected test file %s not found", MOCKED_NOTIFICATION_FILENAME));
    }
    try (InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream)) {
      Notification notification = new Gson().fromJson(inputStreamReader, Notification.class);
      return Collections.singleton(new ImmutablePair<>("test", notification)).iterator();
    }
  }

  private MetricsCollectionService getMockCollectionService(Collection<MetricValues> collection) {
    return new AggregatedMetricsCollectionService(1000L) {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        Iterators.addAll(collection, metrics);
      }
    };
  }

  private boolean containsMetric(MetricValues metricValues, String metricName) {
    for (MetricValue metricValue : metricValues.getMetrics()) {
      if (metricValue.getName().equals(metricName)) {
        return true;
      }
    }
    return false;
  }
}
