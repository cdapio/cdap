/*
 * Copyright © 2016 Cask Data, Inc.
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
package io.cdap.cdap.messaging.spanner;

import static io.cdap.cdap.messaging.spanner.SpannerMessagingService.getMessageId;
import static java.lang.Thread.sleep;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.spanner.SpannerMessagingServiceTestUtil.MockMessagingServiceContext;
import io.cdap.cdap.messaging.spanner.SpannerMessagingServiceTestUtil.SpannerMessageFetchRequest;
import io.cdap.cdap.messaging.spanner.SpannerMessagingServiceTestUtil.SpannerStoreRequest;
import io.cdap.cdap.messaging.spanner.SpannerMessagingServiceTestUtil.SpannerTopicMetadata;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.spi.RawMessage.Builder;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for Cloud spanner implementation of the
 * {@link io.cdap.cdap.messaging.spi.MessagingService}. This test needs the following Java
 * properties to run. If they are not provided, tests will be ignored.
 *
 * <ul>
 *   <li>gcp.project - GCP project name</li>
 *   <li>gcp.spanner.instance - GCP spanner instance name</li>
 *   <li>gcp.spanner.database - GCP spanner database name</li>
 *   <li>(optional) gcp.credentials.path - Local file path to the service account
 *   json that has the "Cloud Spanner Database User" role</li>
 * </ul>
 * TODO [CDAP-21101] : These tests should be part of the continuous build & test runs.
 */
public class SpannerMessagingServiceTest {

  private static SpannerMessagingService service;

  static final List<TopicMetadata> SYSTEM_TOPICS = Arrays.asList(
      new SpannerTopicMetadata(new TopicId("system", "t1"), new HashMap<>()),
      new SpannerTopicMetadata(new TopicId("system", "t2"), new HashMap<>()));

  private static final SpannerTopicMetadata SIMPLE_TOPIC = new SpannerTopicMetadata(
      new TopicId("system", "topic1"), new HashMap<>());

  private static final SpannerTopicMetadata TOPIC_WITH_PROPERTIES = new SpannerTopicMetadata(
      new TopicId("system", "topic2"), new HashMap<String, String>() {
    {
      put("key1", "value1");
      put("key2", "value2");
    }
  });

  @BeforeClass
  public static void createSpannerMessagingService() throws Exception {
    String project = System.getProperty("gcp.project");
    String instance = System.getProperty("gcp.spanner.instance");
    String database = System.getProperty("gcp.spanner.database");
    String credentialsPath = System.getProperty("gcp.credentials.path");

    // GCP project, instance, and database must be provided
    Assume.assumeNotNull(project, instance, database);

    Map<String, String> configs = new HashMap<>();
    configs.put(SpannerUtil.PROJECT, project);
    configs.put(SpannerUtil.INSTANCE, instance);
    configs.put(SpannerUtil.DATABASE, database);

    if (credentialsPath != null) {
      configs.put(SpannerUtil.CREDENTIALS_PATH, credentialsPath);
    }

    configs.put(SpannerUtil.PUBLISH_DELAY_MILLIS, "2");
    configs.put(SpannerUtil.PUBLISH_BATCH_SIZE, "2");
    configs.put(SpannerUtil.PUBLISH_BATCH_TIMEOUT_MILLIS, "5");
    MessagingServiceContext context = new MockMessagingServiceContext(configs);

    service = new SpannerMessagingService();
    service.initialize(context);
  }

  @After
  public void cleanUp() throws Exception {
    try {
      service.deleteTopic(SIMPLE_TOPIC.getTopicId());
      service.deleteTopic(TOPIC_WITH_PROPERTIES.getTopicId());
      service.deleteTopic(SYSTEM_TOPICS.get(0).getTopicId());
      service.deleteTopic(SYSTEM_TOPICS.get(1).getTopicId());
    } catch (TopicNotFoundException e) {
      // no-op
    }
  }

  @Test
  public void testCreateTopic() throws Exception {
    service.createTopic(SIMPLE_TOPIC);
    Assert.assertEquals(SIMPLE_TOPIC.getProperties(),
        service.getTopicMetadataProperties(SIMPLE_TOPIC.getTopicId()));
  }

  @Test
  public void testInitialiseSuccessful() throws Exception {
    for (TopicMetadata topic : SYSTEM_TOPICS) {
      Assert.assertEquals(topic.getProperties(),
          service.getTopicMetadataProperties(topic.getTopicId()));
    }
  }

  @Test
  public void testCreateTopicWithProperties() throws Exception {
    service.createTopic(TOPIC_WITH_PROPERTIES);
    Assert.assertEquals(TOPIC_WITH_PROPERTIES.getProperties(),
        service.getTopicMetadataProperties(TOPIC_WITH_PROPERTIES.getTopicId()));
  }

  @Test(expected = TopicNotFoundException.class)
  public void testGetMetadataPropertiesInvalidTopic() throws Exception {
    TopicId topicId = new TopicId("system", "invalid");
    service.getTopicMetadataProperties(topicId);
  }

  @Test
  public void testListTopics() throws Exception {
    service.createTopic(SIMPLE_TOPIC);
    service.createTopic(TOPIC_WITH_PROPERTIES);
    List<TopicId> topics = service.listTopics(new NamespaceId("system"));
    Assert.assertEquals(Arrays.asList(new TopicId("system",
            SpannerMessagingService.getTableName(SYSTEM_TOPICS.get(0).getTopicId())),
        new TopicId("system",
            SpannerMessagingService.getTableName(SYSTEM_TOPICS.get(1).getTopicId())),
        new TopicId("system", SpannerMessagingService.getTableName(SIMPLE_TOPIC.getTopicId())),
        new TopicId("system",
            SpannerMessagingService.getTableName(TOPIC_WITH_PROPERTIES.getTopicId()))), topics);
  }

  @Test
  public void testListTopicsEmptyNamespace() throws Exception {
    List<TopicId> topics = service.listTopics(new NamespaceId("namespace"));
    Assert.assertEquals(new ArrayList<>(), topics);
  }

  @Test
  public void testService() throws Exception {
    service.createTopic(SIMPLE_TOPIC);

    List<String> messagesBatch = Arrays.asList("message_0", "message_1", "message_2");
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(messagesBatch.get(0))));
    sleep(1);
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(messagesBatch.get(1))));
    sleep(1);
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(messagesBatch.get(2))));

    List<RawMessage> expectedMessages = Arrays.asList(new Builder().setId(getMessageId(0, 0, 0))
            .setPayload(messagesBatch.get(0).getBytes(StandardCharsets.UTF_8)).build(),
        new RawMessage.Builder().setId(getMessageId(0, 0, 1))
            .setPayload(messagesBatch.get(1).getBytes(StandardCharsets.UTF_8)).build(),
        new RawMessage.Builder().setId(getMessageId(0, 0, 2))
            .setPayload(messagesBatch.get(2).getBytes(StandardCharsets.UTF_8)).build());

    try (CloseableIterator<RawMessage> messageIterator = service.fetch(
        new SpannerMessageFetchRequest(SIMPLE_TOPIC.getTopicId(), null, 10))) {
      assertMessages(expectedMessages, messageIterator);
    }
  }

  @Test
  public void testFetch_FromCertainTimestamp() throws Exception {
    service.createTopic(SIMPLE_TOPIC);

    List<String> messagesBatch = Arrays.asList("message_0", "message_1", "message_2");
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(messagesBatch.get(0))));
    long firstMsgTimestampMicros = System.currentTimeMillis() * 1000 + 4;
    sleep(5);
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(messagesBatch.get(1))));
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(messagesBatch.get(2))));

    List<RawMessage> expectedMessages = Arrays.asList(
        new RawMessage.Builder().setId(getMessageId(0, 0, 0))
            .setPayload(messagesBatch.get(1).getBytes(StandardCharsets.UTF_8)).build(),
        new RawMessage.Builder().setId(getMessageId(0, 0, 1))
            .setPayload(messagesBatch.get(2).getBytes(StandardCharsets.UTF_8)).build());

    byte[] startOffset = getMessageId(0, 0, firstMsgTimestampMicros);
    try (CloseableIterator<RawMessage> messageIterator = service.fetch(
        new SpannerMessageFetchRequest(SIMPLE_TOPIC.getTopicId(), startOffset, 10))) {
      assertMessages(expectedMessages, messageIterator);
    }
  }

  @Test
  public void testPublishAndFetchLargeMessage() throws Exception {
    service.createTopic(SIMPLE_TOPIC);

    char[] chars = new char[10 * 1024 * 1024]; // 10 MB
    Arrays.fill(chars, 'a');
    String payloadString = new String(chars);

    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(payloadString)));

    List<RawMessage> expectedMessages = Collections.singletonList(
        new Builder().setId(getMessageId(0, 0, 0))
            .setPayload(payloadString.getBytes(StandardCharsets.UTF_8)).build());

    try (CloseableIterator<RawMessage> messageIterator = service.fetch(
        new SpannerMessageFetchRequest(SIMPLE_TOPIC.getTopicId(), null, 10))) {
      assertMessages(expectedMessages, messageIterator);
    }
  }

  @Test
  public void testLargeMessage_WithLesserLimit() throws Exception {
    service.createTopic(SIMPLE_TOPIC);

    char[] chars = new char[20 * 1024 * 1024]; // 20 MB
    Arrays.fill(chars, 'a');
    String payloadString = new String(chars);

    // TXN     sequence_id  payload_sequence_id  publish_ts  payload  payload_parts_remaining
    // TXN1    0            0                    ts1          m1       0
    // TXN1    1            0                    ts1          m2       0
    // TXN1    2            0                    ts1          m3_p1    2
    // TXN1    2            1                    ts1          m3_p2    1
    // TXN1    2            2                    ts1          m3_p3    0
    // TXN1    3            0                    ts1          m4       0
    List<String> messagesBatch = Arrays.asList("message_0", "message_1", payloadString,
        "message_3");
    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(), messagesBatch));
    List<RawMessage> expectedMessages = Arrays.asList(new Builder().setId(getMessageId(0, 0, 0))
            .setPayload(messagesBatch.get(0).getBytes(StandardCharsets.UTF_8)).build(),
        new RawMessage.Builder().setId(getMessageId(1, 0, 0))
            .setPayload(messagesBatch.get(1).getBytes(StandardCharsets.UTF_8)).build(),
        new RawMessage.Builder().setId(getMessageId(2, 0, 0))
            .setPayload(messagesBatch.get(2).getBytes(StandardCharsets.UTF_8)).build(),
        new RawMessage.Builder().setId(getMessageId(3, 0, 0))
            .setPayload(messagesBatch.get(3).getBytes(StandardCharsets.UTF_8)).build());

    byte[] lastMessageId;
    try (CloseableIterator<RawMessage> messageIterator = service.fetch(
        new SpannerMessageFetchRequest(SIMPLE_TOPIC.getTopicId(), null, 3))) {
      lastMessageId = assertMessages(expectedMessages.subList(0, 2), messageIterator);
    }

    try (CloseableIterator<RawMessage> messageIterator = service.fetch(
        new SpannerMessageFetchRequest(SIMPLE_TOPIC.getTopicId(), lastMessageId, 3))) {
      lastMessageId = assertMessages(expectedMessages.subList(2, 3), messageIterator);
    }

    try (CloseableIterator<RawMessage> messageIterator = service.fetch(
        new SpannerMessageFetchRequest(SIMPLE_TOPIC.getTopicId(), lastMessageId, 3))) {
      assertMessages(expectedMessages.subList(3, 4), messageIterator);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPublish_ExceedMaxSize() throws Exception {
    service.createTopic(SIMPLE_TOPIC);

    char[] chars = new char[100 * 1024 * 1024]; // 100 MB
    Arrays.fill(chars, 'a');
    String payloadString = new String(chars);

    service.publish(new SpannerStoreRequest(SIMPLE_TOPIC.getTopicId(),
        Collections.singletonList(payloadString)));
  }

  /**
   * This method iterates through the fetched messages and verifies that they match the expected
   * messages in terms of message count, message ID, and message payload. Since message timestamps
   * cannot be directly compared due to potential clock skew, this method converts timestamps in the
   * message IDs to comparable integers for comparison purposes.
   *
   * @return - message ID of the last fetched message so that it could be used as the startOffset.
   */
  private byte[] assertMessages(List<RawMessage> expectedMessages,
      CloseableIterator<RawMessage> messageIterator) {
    List<RawMessage> fetchedMessages = new ArrayList<>();
    long currTimestamp = -1;
    long publishTimestamp = -1;
    byte[] lastMessageId = new byte[0];
    while (messageIterator.hasNext()) {
      RawMessage message = messageIterator.next();
      byte[] id = message.getId();
      if (id != null) {
        lastMessageId = id;
        int offset = 0;
        long startTime = Bytes.toLong(id, offset);
        if (currTimestamp != startTime) {
          publishTimestamp++;
          currTimestamp = startTime;
        }

        offset += Bytes.SIZEOF_LONG;
        long seqID = Bytes.toShort(id, offset);
        offset += Bytes.SIZEOF_SHORT;
        long payloadSeqID = Bytes.toShort(id, offset);
        fetchedMessages.add(
            new RawMessage.Builder().setId(getMessageId(seqID, payloadSeqID, publishTimestamp))
                .setPayload(message.getPayload()).build());
      }
    }

    Assert.assertEquals("Message count do not match", expectedMessages.size(),
        fetchedMessages.size());
    for (int i = 0; i < expectedMessages.size(); i++) {
      RawMessage expected = expectedMessages.get(i);
      RawMessage actual = fetchedMessages.get(i);

      Assert.assertArrayEquals("Message IDs do not match at index " + i, expected.getId(),
          actual.getId());
      Assert.assertArrayEquals("Message payloads do not match at index " + i, expected.getPayload(),
          actual.getPayload());
    }
    return lastMessageId;
  }
}