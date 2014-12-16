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
import co.cask.cdap.notifications.client.NotificationClient;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.service.NotificationFeedException;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.Inject;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;

/**
 * Kafka implementation of the {@link co.cask.cdap.notifications.client.NotificationClient}.
 */
public class KafkaNotificationClient implements NotificationClient {

  private final KafkaClient kafkaClient;
  private final DatasetFramework dsFramework;
  private final NotificationFeedClient feedClient;
  private final TransactionSystemClient transactionSystemClient;
  private final KafkaPublisher.Ack ack;

  private KafkaPublisher kafkaPublisher;

  @Inject
  public KafkaNotificationClient(KafkaClient kafkaClient, DatasetFramework dsFramework,
                                 NotificationFeedClient feedClient,
                                 TransactionSystemClient transactionSystemClient) {
    this.kafkaClient = kafkaClient;
    this.dsFramework = dsFramework;
    this.feedClient = feedClient;
    this.transactionSystemClient = transactionSystemClient;
    this.ack = KafkaPublisher.Ack.LEADER_RECEIVED;
  }

  @Override
  public <N> Publisher<N> createPublisher(NotificationFeed feed) throws NotificationFeedException {
    KafkaPublisher kafkaPublisher = getKafkaPublisher();
    if (kafkaPublisher == null) {
      throw new NotificationFeedException("Unable to get kafka publisher, will not be able to publish Notification.");
    }

    // This call will make sure that the feed exists - at this point it really should
    // Because only the resource owner can publish changes from the resource
    // And the resource owner should have created the feed first hand.
    feedClient.getFeed(feed);

    String topic = KafkaNotificationUtils.getKafkaTopic(feed);
    KafkaPublisher.Preparer preparer = kafkaPublisher.prepare(topic);
    return new KafkaNotificationPublisher<N>(feed, preparer);
  }

  @Override
  public Subscriber createSubscriber() {
    KafkaConsumer.Preparer kafkaPreparer = kafkaClient.getConsumer().prepare();
    return new KafkaNotificationSubscriber(feedClient, kafkaPreparer, dsFramework, transactionSystemClient);
  }

  private KafkaPublisher getKafkaPublisher() {
    if (kafkaPublisher != null) {
      return kafkaPublisher;
    }
    try {
      kafkaPublisher = kafkaClient.getPublisher(ack, Compression.SNAPPY);
    } catch (IllegalStateException e) {
      // can happen if there are no kafka brokers because the kafka server is down.
      kafkaPublisher = null;
    }
    return kafkaPublisher;
  }
}
