/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.notifications.kafka;

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.client.AbstractNotificationSubscriber;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.tephra.TransactionSystemClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Kafka implementation of a {@link co.cask.cdap.notifications.client.NotificationClient.Subscriber}.
 */
public class KafkaNotificationSubscriber extends AbstractNotificationSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationSubscriber.class);

  private final KafkaConsumer.Preparer kafkaPreparer;

  protected KafkaNotificationSubscriber(NotificationFeedClient feedClient, KafkaConsumer.Preparer kafkaPreparer,
                                        DatasetFramework dsFramework, TransactionSystemClient transactionSystemClient) {
    super(feedClient, dsFramework, transactionSystemClient);
    this.kafkaPreparer = kafkaPreparer;
  }

  @Override
  protected void doSubscribe(NotificationFeed feed) {
    String topic = KafkaNotificationUtils.getKafkaTopic(feed);

    // TODO there is a bug in twill, that when the topic doesn't exist, add latest will not make subscription
    // start from offset 0 - but that will be fixed soon
    kafkaPreparer.addLatest(topic, 0);
  }

  @Override
  protected String decodeMessageKey(ByteBuffer buffer) throws IOException {
    return KafkaMessageSerializer.decodeMessageKey(buffer);
  }

  @Override
  protected String buildMessageKey(NotificationFeed feed) {
    return KafkaMessageSerializer.buildKafkaMessageKey(feed);
  }

  @Override
  protected Object decodePayload(ByteBuffer buffer, Type type) throws IOException {
    return KafkaMessageSerializer.decode(buffer, type);
  }

  @Override
  public synchronized Cancellable consume() {
    if (isConsuming()) {
      throw new UnsupportedOperationException("Subscriber is already consuming.");
    }
    startConsuming();
    return kafkaPreparer.consume(new KafkaConsumer.MessageCallback() {
      @Override
      public void onReceived(Iterator<FetchedMessage> messages) {
        int count = 0;
        while (messages.hasNext()) {
          FetchedMessage message = messages.next();
          ByteBuffer payload = message.getPayload();
          try {
            handlePayload(payload);
            count++;
          } catch (IOException e) {
            LOG.error("Could not decode Kafka message {} using Gson. Make sure that the " +
                        "getNotificationFeedType() method is correctly set.", message, e);
          }
        }
        LOG.debug("Successfully handled {} messages from kafka", count);
      }

      @Override
      public void finished() {
        LOG.info("Subscription finished.");
      }
    });
  }

}
