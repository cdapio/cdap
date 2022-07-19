/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Module;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.internal.io.ReflectionDatumReader;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.metrics.MetricsTestBase;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Testing the basic properties of the {@link MessagingMetricsCollectionService}.
 */
public class MessagingMetricsCollectionServiceTest extends MetricsTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsCollectionServiceTest.class);

  @Test
  public void testMessagingPublish() throws TopicNotFoundException {

    MetricsCollectionService collectionService = new MessagingMetricsCollectionService(CConfiguration.create(),
                                                                                       messagingService,
                                                                                       recordWriter);
    collectionService.startAsync().awaitRunning();

    // publish metrics for different context
    for (int i = 1; i <= 3; i++) {
      collectionService.getContext(ImmutableMap.of("tag", "" + i)).increment("processed", i);
    }

    collectionService.stopAsync().awaitTerminated();

    // <Context, metricName, value>
    Table<String, String, Long> expected = HashBasedTable.create();
    expected.put("tag.1", "processed", 1L);
    expected.put("tag.2", "processed", 2L);
    expected.put("tag.3", "processed", 3L);

    ReflectionDatumReader<MetricValues> recordReader = new ReflectionDatumReader<>(schema, metricValueType);
    assertMetricsFromMessaging(schema, recordReader, expected);
  }

  private void assertMetricsFromMessaging(final Schema schema,
                                          ReflectionDatumReader recordReader,
                                          Table<String, String, Long> expected) throws TopicNotFoundException {

    // Consume from kafka
    final Map<String, MetricValues> metrics = Maps.newHashMap();
    for (int i = 0; i < cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM); i++) {
    TopicId topicId = NamespaceId.SYSTEM.topic(TOPIC_PREFIX + i);
      try (CloseableIterator<RawMessage> iterator = messagingService.prepareFetch(topicId).fetch()) {
        while (iterator.hasNext()) {
          RawMessage message = iterator.next();
          MetricValues metricsRecord = (MetricValues) recordReader.read(
            new BinaryDecoder(new ByteArrayInputStream(message.getPayload())), schema);
          StringBuilder flattenContext = new StringBuilder();
          // for verifying expected results, sorting tags
          Map<String, String> tags = Maps.newTreeMap();
          tags.putAll(metricsRecord.getTags());
          for (Map.Entry<String, String> tag : tags.entrySet()) {
            flattenContext.append(tag.getKey()).append(".").append(tag.getValue()).append(".");
          }
          // removing trailing "."
          if (flattenContext.length() > 0) {
            flattenContext.deleteCharAt(flattenContext.length() - 1);
          }
          metrics.put(flattenContext.toString(), metricsRecord);
        }
      } catch (IOException e) {
        LOG.info("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
      }
    }
    Assert.assertEquals(expected.rowKeySet().size(), metrics.size());

    checkReceivedMetrics(expected, metrics);
  }

  private void checkReceivedMetrics(Table<String, String, Long> expected, Map<String, MetricValues> actual) {
    for (String expectedContext : expected.rowKeySet()) {
      MetricValues metricValues = actual.get(expectedContext);
      Assert.assertNotNull("Missing expected value for " + expectedContext, metricValues);

      for (Map.Entry<String, Long> entry : expected.row(expectedContext).entrySet()) {
        boolean found = false;
        for (MetricValue metricValue : metricValues.getMetrics()) {
          if (entry.getKey().equals(metricValue.getName())) {
            Assert.assertEquals(entry.getValue().longValue(), metricValue.getValue());
            found = true;
            break;
          }
        }
        Assert.assertTrue(found);
      }
    }
  }

  @Override
  protected List<Module> getAdditionalModules() {
    return Collections.singletonList(new AuthorizationEnforcementModule().getNoOpModules());
  }
}
