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

package co.cask.cdap.messaging.store;

import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for Metadata Table tests.
 */
public abstract class MetadataTableTest {

  @Test
  public void testTopicManagement() throws Exception {
    try (MetadataTable table = getTable()) {
      Assert.assertTrue(table.listTopics().isEmpty());
      Assert.assertTrue(table.listTopics(NamespaceId.DEFAULT).isEmpty());

      try {
        table.getMetadata(NamespaceId.DEFAULT.topic("t1"));
        Assert.fail("Expected exception on topic that doesn't exist");
      } catch (TopicNotFoundException e) {
        // expected
      }

      // Create topic default:t1
      table.createTopic(new TopicMetadata(NamespaceId.DEFAULT.topic("t1"), "ttl", 10));
      Assert.assertEquals(1, table.listTopics(NamespaceId.DEFAULT).size());

      // Create topic default:t2
      TopicMetadata topicMetadata = new TopicMetadata(NamespaceId.DEFAULT.topic("t2"), "ttl", 20);
      table.createTopic(topicMetadata);
      Assert.assertEquals(topicMetadata, table.getMetadata(NamespaceId.DEFAULT.topic("t2")));

      // Create topic system:t3
      table.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic("t3"), "ttl", 30));

      // List default namespace, should get 2
      Assert.assertEquals(2, table.listTopics(NamespaceId.DEFAULT).size());

      // List all topics, should get 3
      Assert.assertEquals(3, table.listTopics().size());

      // Delete t1
      table.deleteTopic(NamespaceId.DEFAULT.topic("t1"));
      Assert.assertEquals(1, table.listTopics(NamespaceId.DEFAULT).size());
      Assert.assertEquals(2, table.listTopics().size());

      // Delete t2
      table.deleteTopic(NamespaceId.DEFAULT.topic("t2"));
      Assert.assertTrue(table.listTopics(NamespaceId.DEFAULT).isEmpty());
      Assert.assertEquals(1, table.listTopics(NamespaceId.SYSTEM).size());

      // Delete t3
      table.deleteTopic(NamespaceId.SYSTEM.topic("t3"));
      Assert.assertTrue(table.listTopics(NamespaceId.DEFAULT).isEmpty());
      Assert.assertTrue(table.listTopics(NamespaceId.SYSTEM).isEmpty());
      Assert.assertTrue(table.listTopics().isEmpty());
    }
  }

  protected abstract MetadataTable getTable() throws Exception;
}
