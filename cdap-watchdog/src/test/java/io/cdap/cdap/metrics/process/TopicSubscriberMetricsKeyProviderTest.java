/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link TopicSubscriberMetricsKeyProvider}
 */
public class TopicSubscriberMetricsKeyProviderTest {

  @Test
  public void testKeys() {
    String subscriberId = "test-subscriber";
    String namespace = "test";
    String topicName = "topic0";
    TopicId topicId = new TopicId(namespace, topicName);
    Map<TopicId, MetricsMetaKey> actual = new TopicSubscriberMetricsKeyProvider(subscriberId).getKeys(
      Collections.singletonList(topicId));
    Map<TopicId, MetricsMetaKey> expected = new HashMap<>();
    expected.put(new TopicId(namespace, topicName), new TopicSubscriberMetaKey(topicId, subscriberId));
    Assert.assertEquals(expected, actual);
  }
}
