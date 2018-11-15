/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.messaging.server.MessagingHttpService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.Transaction;
import org.apache.tephra.TxConstants;
import org.iq80.leveldb.util.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TMSPerfTest {

    private static CConfiguration cConf;
    private static MessagingHttpService httpService;
    private static MessagingService client;
    private static File tempDir;

    public static void init() {
        tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        System.out.println(tempDir);
        cConf = CConfiguration.create();
        cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempDir.getAbsolutePath());
        cConf.setInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE, 128);
        // Set max life time to a high value so that dummy tx ids that we create in the tests still work
        cConf.setLong(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10000000000L);

        Injector injector = Guice.createInjector(
                new ConfigModule(cConf),
                new DiscoveryRuntimeModule().getInMemoryModules(),
                new MessagingServerRuntimeModule().getStandaloneModules(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
                    }
                }
        );

        httpService = injector.getInstance(MessagingHttpService.class);
        httpService.startAndWait();

//        client = new ClientMessagingService(injector.getInstance(DiscoveryServiceClient.class));
        client = injector.getInstance(MessagingService.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteRecursively(tempDir)));
    }

    public void testBasicPubSub() throws Exception {
        TopicId topicId = new NamespaceId("ns1").topic("testBasicPubSub");

        // Publish to a non-existing topic should get not found exception
        try {
            client.publish(StoreRequestBuilder.of(topicId).addPayload("a").build());
//            Assert.fail("Expected TopicNotFoundException");
        } catch (TopicNotFoundException e) {
            // Expected
        }

        // Consume from a non-existing topic should get not found exception
        try {
            client.prepareFetch(topicId).fetch();
//            Assert.fail("Expected TopicNotFoundException");
        } catch (TopicNotFoundException e) {
            // Expected
        }

        client.createTopic(new TopicMetadata(topicId));

        // Publish a non-transactional message with empty payload should result in failure
        try {
            client.publish(StoreRequestBuilder.of(topicId).build());
//            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Publish a non-tx message, no RollbackDetail is returned
//        Assert.assertNull(
                client.publish(StoreRequestBuilder.of(topicId).addPayload("m0").addPayload("m1").build());
//        );

        // Publish a transactional message, a RollbackDetail should be returned
        RollbackDetail rollbackDetail = client.publish(StoreRequestBuilder.of(topicId)
                .addPayload("m2").setTransaction(1L).build());
//        Assert.assertNotNull(rollbackDetail);

        // Rollback the published message
        client.rollback(topicId, rollbackDetail);

        // Fetch messages non-transactionally (should be able to read all the messages since rolled back messages
        // are still visible until ttl kicks in)
        List<RawMessage> messages = new ArrayList<>();
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId).fetch()) {
            Iterators.addAll(messages, iterator);
        }
//        Assert.assertEquals(3, messages.size());
        for (int i = 0; i < 3; i++) {
//            Assert.assertEquals("m" + i, Bytes.toString(messages.get(i).getPayload()));
        }

        // Consume transactionally. It should get only m0 and m1 since m2 has been rolled back
        List<RawMessage> txMessages = new ArrayList<>();
        Transaction transaction = new Transaction(3L, 3L, new long[0], new long[]{2L}, 2L);
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartTime(0)
                .setTransaction(transaction)
                .fetch()) {
            Iterators.addAll(txMessages, iterator);
        }
//        Assert.assertEquals(2, txMessages.size());
        for (int i = 0; i < 2; i++) {
//            Assert.assertEquals("m" + i, Bytes.toString(messages.get(i).getPayload()));
        }

        // Fetch again from a given message offset exclusively.
        // Expects one message to be fetched
        byte[] startMessageId = messages.get(1).getId();
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartMessage(startMessageId, false)
                .fetch()) {
            // It should have only one message (m2)
//            Assert.assertTrue(iterator.hasNext());
            RawMessage msg = iterator.next();
//            Assert.assertEquals("m2", Bytes.toString(msg.getPayload()));
        }

        // Fetch again from the last message offset exclusively
        // Expects no message to be fetched
        startMessageId = messages.get(2).getId();
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartMessage(startMessageId, false)
                .fetch()) {
//            Assert.assertFalse(iterator.hasNext());
        }

        // Fetch with start time. It should get both m0 and m1 since they are published in the same request, hence
        // having the same publish time
        startMessageId = messages.get(1).getId();
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartTime(new MessageId(startMessageId)
                        .getPublishTimestamp())
                .setLimit(2)
                .fetch()) {
            messages.clear();
            Iterators.addAll(messages, iterator);
        }
//        Assert.assertEquals(2, messages.size());
        for (int i = 0; i < 2; i++) {
//            Assert.assertEquals("m" + i, Bytes.toString(messages.get(i).getPayload()));
        }

        // Publish 2 messages, one transactionally, one without transaction
        client.publish(StoreRequestBuilder.of(topicId).addPayload("m3").setTransaction(2L).build());
        client.publish(StoreRequestBuilder.of(topicId).addPayload("m4").build());

        // Consume without transactional, it should see m2, m3 and m4
        startMessageId = messages.get(1).getId();
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartMessage(startMessageId, false)
                .fetch()) {
            messages.clear();
            Iterators.addAll(messages, iterator);
        }
//        Assert.assertEquals(3, messages.size());
        for (int i = 0; i < 3; i++) {
//            Assert.assertEquals("m" + (i + 2), Bytes.toString(messages.get(i).getPayload()));
        }

        transaction = new Transaction(3L, 3L, new long[0], new long[]{2L}, 2L);
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartMessage(startMessageId, false)
                .setTransaction(transaction)
                .fetch()) {
//            Assert.assertFalse(iterator.hasNext());
        }

        // Consume using a transaction that has tx = 2L in the invalid list. It should skip m3 and got m4
        transaction = new Transaction(3L, 3L, new long[]{2L}, new long[0], 0L);
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartMessage(startMessageId, false)
                .setTransaction(transaction)
                .fetch()) {
            messages.clear();
            Iterators.addAll(messages, iterator);
        }
//        Assert.assertEquals(1, messages.size());
//        Assert.assertEquals("m4", Bytes.toString(messages.get(0).getPayload()));

        // Consume using a transaction that has tx = 2L committed. It should get m3 and m4
        transaction = new Transaction(3L, 3L, new long[0], new long[0], 0L);
        try (CloseableIterator<RawMessage> iterator = client.prepareFetch(topicId)
                .setStartMessage(startMessageId, false)
                .setTransaction(transaction)
                .fetch()) {
            messages.clear();
            Iterators.addAll(messages, iterator);
        }
//        Assert.assertEquals(2, messages.size());
        for (int i = 0; i < 2; i++) {
//            Assert.assertEquals("m" + (i + 3), Bytes.toString(messages.get(i).getPayload()));
        }

        client.deleteTopic(topicId);
    }


    public static void main(String[] args) throws Exception {
        TMSPerfTest tmsPerfTest = new TMSPerfTest();
        tmsPerfTest.init();
//        tmsPerfTest.testBasicPubSub();

        TMSPerfService tmsPerfService = new TMSPerfService(cConf, client);
        tmsPerfService.startAndWait();
        while (tmsPerfService.isRunning()) {
            TimeUnit.SECONDS.sleep(1);
        }

    }
}
