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

package co.cask.cdap.messaging;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tests for {@link MessagingUtils} and {@link MessagingServiceUtils}.
 */
public class MessagingUtilsTest {

  @Test
  public void testTopicConversion() throws Exception {
    TopicId id = new TopicId("n1", "t1");
    byte[] topicBytes = MessagingUtils.toMetadataRowKey(id);
    TopicId topicId = MessagingUtils.toTopicId(topicBytes);
    Assert.assertEquals(id, topicId);
  }

  @Test
  public void testGenerations() throws Exception {
    Assert.assertTrue(MessagingUtils.isOlderGeneration(3, 5));
    Assert.assertTrue(MessagingUtils.isOlderGeneration(3, -3));
    Assert.assertFalse(MessagingUtils.isOlderGeneration(6, 5));
    Assert.assertFalse(MessagingUtils.isOlderGeneration(6, 6));
    Assert.assertFalse(MessagingUtils.isOlderGeneration(6, -5));
  }

  @Test
  public void testSystemTopics() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "  topic-1, topic_2 ,prefix:10,invalid.name");

    Set<TopicId> topics = MessagingServiceUtils.getSystemTopics(cConf, true);
    Set<TopicId> expected = new LinkedHashSet<>();
    expected.add(NamespaceId.SYSTEM.topic("topic-1"));
    expected.add(NamespaceId.SYSTEM.topic("topic_2"));
    for (int i = 0; i < 10; i++) {
      expected.add(NamespaceId.SYSTEM.topic("prefix" + i));
    }

    Assert.assertEquals(expected, topics);
  }
}
