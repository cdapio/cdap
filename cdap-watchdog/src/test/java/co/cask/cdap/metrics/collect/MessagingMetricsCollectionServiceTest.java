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
package co.cask.cdap.metrics.collect;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.internal.io.ReflectionDatumReader;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.metrics.MessagingMetricsTestBase;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Module;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Testing the basic properties of the {@link MessagingMetricsCollectionService}.
 */
public class MessagingMetricsCollectionServiceTest extends MessagingMetricsTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsCollectionServiceTest.class);

  private static final int PARTITION_SIZE = 10;

  @Test
  public void testMessagingPublish()
    throws UnsupportedTypeException, InterruptedException, TopicNotFoundException, IOException {

    MetricsCollectionService collectionService = new MessagingMetricsCollectionService(TOPIC_PREFIX,
                                                                                       PARTITION_SIZE,
                                                                                       messagingService,
                                                                                       recordWriter);
    collectionService.startAndWait();

    // publish metrics for different context
    for (int i = 1; i <= 3; i++) {
      collectionService.getContext(ImmutableMap.of("tag", "" + i)).increment("processed", i);
    }

    // Sleep to make sure metrics get published
    TimeUnit.SECONDS.sleep(2);

    collectionService.stopAndWait();

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
                                          Table<String, String, Long> expected)
    throws InterruptedException, TopicNotFoundException, IOException {

    // Consume from kafka
    final Map<String, MetricValues> metrics = Maps.newHashMap();
    ByteBufferInputStream is = new ByteBufferInputStream(null);
    for (int i = 0; i < PARTITION_SIZE; i++) {
    TopicId topicId = NamespaceId.SYSTEM.topic(TOPIC_PREFIX + i);
      try (CloseableIterator<RawMessage> iterator = messagingService.prepareFetch(topicId).fetch()) {
        while (iterator.hasNext()) {
          RawMessage message = iterator.next();
          MetricValues metricsRecord = (MetricValues) recordReader.read(
            new BinaryDecoder(is.reset(ByteBuffer.wrap(message.getPayload()))), schema);
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

  protected void checkReceivedMetrics(Table<String, String, Long> expected, Map<String, MetricValues> actual) {
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
    return new ArrayList<>();
  }
}
