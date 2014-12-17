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

import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.client.AbstractNotificationPublisher;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.kafka.client.KafkaPublisher;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Kafka implementation of a {@link co.cask.cdap.notifications.client.NotificationClient.Publisher}.
 *
 * @param <N> Type of the Notifications being published.
 */
public class KafkaNotificationPublisher<N> extends AbstractNotificationPublisher<N> {

  private final NotificationFeed feed;
  private final KafkaPublisher.Preparer preparer;

  public KafkaNotificationPublisher(NotificationFeed feed, KafkaPublisher.Preparer preparer) {
    super(feed);
    this.feed = feed;
    this.preparer = preparer;
  }

  @Override
  protected ListenableFuture<Void> doPublish(N notification) throws IOException {
    ByteBuffer bb = ByteBuffer.wrap(KafkaMessageSerializer.encode(feed, notification));
    preparer.add(bb, KafkaMessageSerializer.buildKafkaMessageKey(feed));
    return Futures.transform(preparer.send(), Functions.<Void>constant(null));
  }
}
