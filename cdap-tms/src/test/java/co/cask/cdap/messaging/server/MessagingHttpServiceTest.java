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

package co.cask.cdap.messaging.server;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.TopicAlreadyExistsException;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.client.ClientMessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.messaging.data.Message;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.http.HttpHandler;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.apache.tephra.Transaction;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link MessagingHttpService}.
 */
public class MessagingHttpServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static CConfiguration cConf;
  private static MessagingHttpService httpService;
  private static MessagingService client;

  @BeforeClass
  public static void init() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE, 128);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());

          Multibinder<HttpHandler> handlerBinder =
            Multibinder.newSetBinder(binder(), HttpHandler.class,
                                     Names.named(Constants.MessagingSystem.HANDLER_BINDING_NAME));

          handlerBinder.addBinding().to(MetadataHandler.class);
          handlerBinder.addBinding().to(StoreHandler.class);
          handlerBinder.addBinding().to(FetchHandler.class);

          bind(MessagingHttpService.class);
        }
      }
    );

    httpService = injector.getInstance(MessagingHttpService.class);
    httpService.startAndWait();

    client = new ClientMessagingService(injector.getInstance(DiscoveryServiceClient.class));
  }

  @AfterClass
  public static void finish() {
    httpService.stopAndWait();
  }

  @Test
  public void testMetadataEndpoints() throws Exception {
    NamespaceId nsId = new NamespaceId("metadata");
    TopicId topic1 = nsId.topic("t1");
    TopicId topic2 = nsId.topic("t2");

    // Get a non exist topic should fail
    try {
      client.getTopic(topic1);
      Assert.fail("Expected TopicNotFoundException");
    } catch (TopicNotFoundException e) {
      // Expected
    }

    // Create the topic t1
    client.createTopic(new TopicMetadata(topic1));

    // Create an existing topic should fail
    try {
      client.createTopic(new TopicMetadata(topic1));
      Assert.fail("Expect TopicAlreadyExistsException");
    } catch (TopicAlreadyExistsException e) {
      // Expected
    }

    // Get the topic properties. Verify TTL is the same as the default one
    Assert.assertEquals(cConf.getInt(Constants.MessagingSystem.TOPIC_DEFAULT_TTL_SECONDS),
                        client.getTopic(topic1).getTTL());

    // Update the topic t1 with new TTL
    client.updateTopic(new TopicMetadata(topic1, "ttl", "5"));

    // Get the topic t1 properties. Verify TTL is updated
    Assert.assertEquals(5, client.getTopic(topic1).getTTL());

    // Try to add another topic t2 with invalid ttl, it should fail
    try {
      client.createTopic(new TopicMetadata(topic2, "ttl", "xyz"));
      Assert.fail("Expect BadRequestException");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Add topic t2 with valid ttl
    client.createTopic(new TopicMetadata(topic2, "ttl", "5"));

    // Get the topic t2 properties. It should have TTL set based on what provided
    Assert.assertEquals(5, client.getTopic(topic2).getTTL());

    // Listing topics under namespace ns1
    List<TopicId> topics = client.listTopics(nsId);
    Assert.assertEquals(Arrays.asList(topic1, topic2), topics);

    // Delete both topics
    client.deleteTopic(topic1);
    client.deleteTopic(topic2);

    // Delete a non exist topic should fail
    try {
      client.deleteTopic(topic1);
      Assert.fail("Expect TopicNotFoundException");
    } catch (TopicNotFoundException e) {
      // Expected
    }

    // Update a non exist topic should fail
    try {
      client.updateTopic(new TopicMetadata(topic1));
      Assert.fail("Expect TopicNotFoundException");
    } catch (TopicNotFoundException e) {
      // Expected
    }

    // Listing topics under namespace ns1 again, it should be empty
    Assert.assertTrue(client.listTopics(nsId).isEmpty());
  }

  @Test
  public void testBasicPubSub() throws Exception {
    TopicId topicId = new NamespaceId("ns1").topic("testBasicPubSub");

    // Publish to a non-existing topic should get not found exception
    try {
      client.publish(StoreRequestBuilder.of(topicId).addPayloads("a").build());
      Assert.fail("Expected TopicNotFoundException");
    } catch (TopicNotFoundException e) {
      // Expected
    }

    // Consume from a non-existing topic should get not found exception
    try {
      client.prepareFetch(topicId).fetch();
      Assert.fail("Expected TopicNotFoundException");
    } catch (TopicNotFoundException e) {
      // Expected
    }

    client.createTopic(new TopicMetadata(topicId));

    // Publish a non-transactional message with empty payload should result in failure
    try {
      client.publish(StoreRequestBuilder.of(topicId).build());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Publish a non-tx message, no RollbackDetail is returned
    Assert.assertNull(client.publish(StoreRequestBuilder.of(topicId).addPayloads("m0", "m1").build()));

    // Publish a transactional message, a RollbackDetail should be returned
    RollbackDetail rollbackDetail = client.publish(StoreRequestBuilder.of(topicId)
                                                     .addPayloads("m2").setTransaction(1L).build());
    Assert.assertNotNull(rollbackDetail);

    // Rollback the published message
    client.rollback(topicId, rollbackDetail);

    // Fetch messages non-transactionally
    List<Message> messages = new ArrayList<>();
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId).fetch()) {
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(2, messages.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals("m" + i, Bytes.toString(messages.get(i).getPayload()));
    }

    // Fetch again from a given message offset exclusively.
    // Expects no messages fetched
    MessageId startMessageId = messages.get(1).getId();
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId)
                                                     .setStartMessage(startMessageId, false)
                                                     .fetch()) {
      Assert.assertFalse(iterator.hasNext());
    }

    // Fetch with start time. It should get both m0 and m1 since they are published in the same request, hence
    // having the same publish time
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId)
                                                     .setStartTime(startMessageId.getPublishTimestamp())
                                                     .fetch()) {
      messages.clear();
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(2, messages.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals("m" + i, Bytes.toString(messages.get(i).getPayload()));
    }

    // Publish 2 messages, one transactionally, one without transaction
    client.publish(StoreRequestBuilder.of(topicId).addPayloads("m3").setTransaction(2L).build());
    client.publish(StoreRequestBuilder.of(topicId).addPayloads("m4").build());

    // Consume without transactional, it should see m3 and m4
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId)
                                                     .setStartMessage(startMessageId, false)
                                                     .fetch()) {
      messages.clear();
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(2, messages.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals("m" + (i + 3), Bytes.toString(messages.get(i).getPayload()));
    }

    // Consume using a transaction that doesn't have tx = 2L visible. It should get no message as it should block on m3
    Transaction transaction = new Transaction(3L, 3L, new long[0], new long[]{2L}, 2L);
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId)
                                                     .setStartMessage(startMessageId, false)
                                                     .setTransaction(transaction)
                                                     .fetch()) {
      Assert.assertFalse(iterator.hasNext());
    }

    // Consume using a transaction that has tx = 2L in the invalid list. It should skip m3 and got m4
    transaction = new Transaction(3L, 3L, new long[]{2L}, new long[0], 0L);
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId)
                                                     .setStartMessage(startMessageId, false)
                                                     .setTransaction(transaction)
                                                     .fetch()) {
      messages.clear();
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(1, messages.size());
    Assert.assertEquals("m4", Bytes.toString(messages.get(0).getPayload()));

    // Consume using a transaction that has tx = 2L committed. It should get m3 and m4
    transaction = new Transaction(3L, 3L, new long[0], new long[0], 0L);
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId)
                                                     .setStartMessage(startMessageId, false)
                                                     .setTransaction(transaction)
                                                     .fetch()) {
      messages.clear();
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(2, messages.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals("m" + (i + 3), Bytes.toString(messages.get(i).getPayload()));
    }

    client.deleteTopic(topicId);
  }

  @Test
  public void testChunkConsume() throws Exception {
    // This test is to verify the message fetching body producer works correctly
    TopicId topicId = new NamespaceId("ns1").topic("testChunkConsume");

    client.createTopic(new TopicMetadata(topicId));

    // Publish 10 messages, each payload is half the size of the chunk size
    int payloadSize = cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE) / 2;
    for (int i = 0; i < 10; i++) {
      String payload = Strings.repeat(Integer.toString(i), payloadSize);
      client.publish(StoreRequestBuilder.of(topicId).addPayloads(payload).build());
    }

    // Fetch messages. All of them should be fetched correctly
    List<Message> messages = new ArrayList<>();
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId).fetch()) {
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(10, messages.size());
    for (int i = 0; i < 10; i++) {
      Message message = messages.get(i);
      Assert.assertEquals(payloadSize, message.getPayload().length);
      String payload = Strings.repeat(Integer.toString(i), payloadSize);
      Assert.assertEquals(payload, Bytes.toString(message.getPayload()));
    }

    client.deleteTopic(topicId);
  }

  @Test
  public void testPayloadTable() throws Exception {
    // This test is to verify storing transaction messages to the payload table
    TopicId topicId = new NamespaceId("ns1").topic("testPayloadTable");

    client.createTopic(new TopicMetadata(topicId));

    // Try to store to Payload table with empty iterator, expected failure
    try {
      client.storePayload(StoreRequestBuilder.of(topicId).setTransaction(1L).build());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Store 20 payloads to the payload table, with 2 payloads per request
    for (int i = 0; i < 10; i++) {
      String payload = Integer.toString(i);
      client.storePayload(StoreRequestBuilder.of(topicId).addPayloads(payload, payload).setTransaction(1L).build());
    }

    // Try to consume and there should be no messages
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId).fetch()) {
      Assert.assertFalse(iterator.hasNext());
    }

    // Publish an empty payload message to the message table. This simulates a tx commit.
    client.publish(StoreRequestBuilder.of(topicId).setTransaction(1L).build());

    // Consume again and there should be 20 messages
    List<Message> messages = new ArrayList<>();
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId).fetch()) {
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(20, messages.size());
    for (int i = 0; i < 20; i += 2) {
      String payload1 = Bytes.toString(messages.get(i).getPayload());
      String payload2 = Bytes.toString(messages.get(i + 1).getPayload());
      Assert.assertEquals(payload1, payload2);
      Assert.assertEquals(Integer.toString(i / 2), payload1);
    }

    // Consume with a limit
    messages.clear();
    try (CloseableIterator<Message> iterator = client.prepareFetch(topicId).setLimit(6).fetch()) {
      Iterators.addAll(messages, iterator);
    }
    Assert.assertEquals(6, messages.size());
    for (int i = 0; i < 6; i += 2) {
      String payload1 = Bytes.toString(messages.get(i).getPayload());
      String payload2 = Bytes.toString(messages.get(i + 1).getPayload());
      Assert.assertEquals(payload1, payload2);
      Assert.assertEquals(Integer.toString(i / 2), payload1);
    }

    client.deleteTopic(topicId);
  }
}
