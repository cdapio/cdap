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

package co.cask.cdap.test.messaging;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.MessagingAdmin;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for applications interactions with TMS.
 */
public class MessagingAppTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final NamespaceId NAMESPACE = new NamespaceId("messageTest");
  private static File artifactJar;

  @BeforeClass
  public static void init() throws Exception {
    artifactJar = createArtifactJar(MessagingApp.class);
  }

  @Before
  public void beforeTest() throws Exception {
    super.beforeTest();
    getNamespaceAdmin().create(new NamespaceMeta.Builder().setName(NAMESPACE).build());
    getMessagingAdmin(NAMESPACE).createTopic(MessagingApp.CONTROL_TOPIC);
  }

  @Test
  public void testWithWorker() throws Exception {
    ApplicationManager appManager = deployWithArtifact(NAMESPACE, MessagingApp.class, artifactJar);

    final WorkerManager workerManager = appManager.getWorkerManager(
      MessagingApp.MessagingWorker.class.getSimpleName()).start();

    MessagingContext messagingContext = getMessagingContext();
    final MessagingAdmin messagingAdmin = getMessagingAdmin(NAMESPACE);

    // Wait for the worker to create the topic
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          messagingAdmin.getTopicProperties(MessagingApp.TOPIC);
          return true;
        } catch (TopicNotFoundException e) {
          return false;
        }
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Publish a message
    String message = "message";

    MessagePublisher messagePublisher = messagingContext.getMessagePublisher();
    messagePublisher.publish(NAMESPACE.getNamespace(), MessagingApp.TOPIC, message);

    // The worker will publish back a message with payload as concat(message, message)
    final MessageFetcher messageFetcher = messagingContext.getMessageFetcher();
    Tasks.waitFor(message + message, new Callable<String>() {
      @Override
      public String call() throws Exception {
        try (CloseableIterator<Message> iterator =
               messageFetcher.fetch(NAMESPACE.getNamespace(), MessagingApp.TOPIC, Integer.MAX_VALUE, 0L)) {
          Message message = Iterators.getLast(iterator, null);
          return message == null ? null : message.getPayloadAsString();
        }
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Publish concat(message + message) to the app
    messagePublisher.publish(NAMESPACE.getNamespace(), MessagingApp.TOPIC, message + message);

    // The worker will publish back a message with payload as concat(message, message, message, message)
    // in a transaction, which it will block on a message to the CONTROL_TOPIC, hence the following wait for should
    // timeout.
    try {
      Tasks.waitFor(message + message + message + message, new Callable<String>() {
        @Override
        public String call() throws Exception {
          try (CloseableIterator<Message> iterator =
                 messageFetcher.fetch(NAMESPACE.getNamespace(), MessagingApp.TOPIC, Integer.MAX_VALUE, 0L)) {
            Message message = Iterators.getLast(iterator, null);
            return message == null ? null : message.getPayloadAsString();
          }
        }
      }, 2, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Assert.fail("Expected timeout exception");
    } catch (TimeoutException e) {
      // expected
    }

    // Now publish a message to the control topic, to unblock the transaction block.
    messagePublisher.publish(NAMESPACE.getNamespace(), MessagingApp.CONTROL_TOPIC, message);

    // Should expect a new message as concat(message, message, message, message)
    Tasks.waitFor(message + message + message + message, new Callable<String>() {
      @Override
      public String call() throws Exception {
        try (CloseableIterator<Message> iterator =
               messageFetcher.fetch(NAMESPACE.getNamespace(), MessagingApp.TOPIC, Integer.MAX_VALUE, 0L)) {
          Message message = Iterators.getLast(iterator, null);
          return message == null ? null : message.getPayloadAsString();
        }
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);


    // Wait for the worker to finish and verify that it completes successfully.
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.SECONDS);
  }

  @Test
  public void testTxPublishFetch() throws Exception {
    ApplicationManager appManager = deployWithArtifact(NAMESPACE, MessagingApp.class, artifactJar);
    MessagingAdmin messagingAdmin = getMessagingAdmin(NAMESPACE);

    final WorkerManager workerManager = appManager.getWorkerManager(
      MessagingApp.TransactionalMessagingWorker.class.getSimpleName());

    // Run the TransactionalMessagingWorker twice, one with getting publisher/fetcher inside TX, one outside.
    for (boolean getInTx : Arrays.asList(true, false)) {
      messagingAdmin.createTopic(MessagingApp.TOPIC);

      workerManager.start(Collections.singletonMap("get.in.tx", Boolean.toString(getInTx)));

      // Wait for the worker to finish and verify that it completes successfully.
      int workerRunCount = getInTx ? 1 : 2;
      workerManager.waitForRuns(ProgramRunStatus.COMPLETED, workerRunCount, 60, TimeUnit.SECONDS);
      messagingAdmin.deleteTopic(MessagingApp.TOPIC);
    }
  }

  @Test
  public void testSparkMessaging() throws Exception {
    ApplicationManager appManager = deployWithArtifact(NAMESPACE, MessagingApp.class, artifactJar);

    final SparkManager sparkManager = appManager.getSparkManager(MessagingSpark.class.getSimpleName()).start();

    final MessageFetcher fetcher = getMessagingContext().getMessageFetcher();
    final AtomicReference<String> messageId = new AtomicReference<>();

    // Wait for the Spark to create the topic
    final MessagingAdmin messagingAdmin = getMessagingAdmin(NAMESPACE.getNamespace());
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          messagingAdmin.getTopicProperties(MessagingApp.TOPIC);
          return true;
        } catch (TopicNotFoundException e) {
          return false;
        }
      }
    }, 60, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Expect a "start" message, followed by "block". There shouldn't be anything in between.
    // This is to verify failed transaction is not publishing anything.
    for (String expected : Arrays.asList("start", "block")) {
      Tasks.waitFor(expected, new Callable<String>() {
        @Override
        public String call() throws Exception {
          try (CloseableIterator<Message> iterator =
                 fetcher.fetch(NAMESPACE.getNamespace(), MessagingApp.TOPIC, 1, messageId.get())) {
            if (!iterator.hasNext()) {
              return null;
            }
            Message message = iterator.next();
            messageId.set(message.getId());
            return message.getPayloadAsString();
          }
        }
      }, 60, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    }

    // Publish a control message to unblock the Spark execution
    getMessagingContext().getMessagePublisher().publish(NAMESPACE.getNamespace(), MessagingApp.CONTROL_TOPIC, "go");

    // Expects a result message as "result-15", where 15 is the sum of 1,2,3,4,5
    Tasks.waitFor("result-15", new Callable<String>() {
      @Override
      public String call() throws Exception {
        try (CloseableIterator<Message> iterator =
               fetcher.fetch(NAMESPACE.getNamespace(), MessagingApp.TOPIC, 1, messageId.get())) {
          if (!iterator.hasNext()) {
            return null;
          }
          Message message = iterator.next();
          messageId.set(message.getId());
          return message.getPayloadAsString();
        }
      }
    }, 60, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);
  }
}
