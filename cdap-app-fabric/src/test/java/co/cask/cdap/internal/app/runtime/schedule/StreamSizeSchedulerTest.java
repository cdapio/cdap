/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class StreamSizeSchedulerTest extends SchedulerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamSizeSchedulerTest.class);

  private static NotificationService notificationService;

  @BeforeClass
  public static void setup() throws Exception {
    notificationService = injector.getInstance(NotificationService.class);
  }

  @Override
  protected StreamMetricsPublisher createMetricsPublisher(final Id.Stream streamId) {
    final Id.NotificationFeed feed = new Id.NotificationFeed.Builder()
      .setNamespaceId(streamId.getNamespaceId())
      .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
      .setName(streamId.getId() + "Size")
      .build();

    return new StreamMetricsPublisher() {

      long totalSize;
      long publishTime = 1000000;

      @Override
      public void increment(long size) throws Exception {
        totalSize += size;
        // making sure every notification is 1 second later than the previous.
        publishTime += 1000;
        metricStore.add(new MetricValues(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, streamId.getNamespaceId(),
                                                        Constants.Metrics.Tag.STREAM, streamId.getId()),
                                        "collect.bytes", TimeUnit.MILLISECONDS.toSeconds(publishTime),
                                        size, MetricType.COUNTER));
        LOG.info("Publishing notification for {} at time {} with total stream size {}.", feed, publishTime, totalSize);
        notificationService.publish(feed, new StreamSizeNotification(publishTime, totalSize));
      }
    };
  }
}
