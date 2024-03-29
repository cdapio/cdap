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

package io.cdap.cdap.messaging.store;

import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for Metadata Table tests.
 */
public abstract class MetadataTableTest {

  @Test
  public void testBasic() throws Exception {
    try (MetadataTable table = createMetadataTable()) {
      Assert.assertTrue(table.listTopics().isEmpty());
      Assert.assertTrue(table.listTopics(NamespaceId.DEFAULT).isEmpty());

      try {
        table.getMetadata(NamespaceId.DEFAULT.topic("t1"));
        Assert.fail("Expected exception on topic that doesn't exist");
      } catch (TopicNotFoundException e) {
        // expected
      }

      // Create topic default:t1
      table.createTopic(new DefaultTopicMetadata(NamespaceId.DEFAULT.topic("t1"), "ttl", 10));
      Assert.assertEquals(1, table.listTopics(NamespaceId.DEFAULT).size());

      // Create topic default:t2
      TopicMetadata topicMetadata = new DefaultTopicMetadata(NamespaceId.DEFAULT.topic("t2"), "ttl", 20);
      table.createTopic(topicMetadata);
      Assert.assertEquals(
          topicMetadata.getTopicId(),
          table.getMetadata(NamespaceId.DEFAULT.topic("t2")).getTopicId());
      Assert.assertEquals(
          topicMetadata.getTTL(), table.getMetadata(NamespaceId.DEFAULT.topic("t2")).getTTL());
      Assert.assertEquals(1, table.getMetadata(NamespaceId.DEFAULT.topic("t2")).getGeneration());

      // Create topic system:t3
      table.createTopic(new DefaultTopicMetadata(NamespaceId.SYSTEM.topic("t3"), "ttl", 30));

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

  @Test
  public void testGenerations() throws Exception {
    try (MetadataTable table = createMetadataTable()) {
      TopicId topicId = NamespaceId.DEFAULT.topic("gtopic");
      for (int i = 1; i <= 50; i++) {
        table.createTopic(new DefaultTopicMetadata(topicId, "ttl", 1));

        TopicMetadata metadata = table.getMetadata(topicId);
        Assert.assertEquals(i, metadata.getGeneration());
        Assert.assertEquals(1, metadata.getTTL());

        table.deleteTopic(topicId);

        try {
          table.getMetadata(topicId);
          Assert.fail("Expected TopicNotFoundException");
        } catch (TopicNotFoundException ex) {
          // Expected
        }
      }
    }
  }

  @Test
  public void testCRUD() throws Exception {
    try (MetadataTable table = createMetadataTable()) {
      TopicId topicId = NamespaceId.DEFAULT.topic("topic");

      // Update a non-existing topic should fail.
      try {
        table.updateTopic(new DefaultTopicMetadata(topicId, "ttl", 10));
        Assert.fail("Expected TopicNotFoundException");
      } catch (TopicNotFoundException e) {
        // Expected
      }

      // Create a topic and validate
      table.createTopic(new DefaultTopicMetadata(topicId, "ttl", 10));
      Assert.assertEquals(10, table.getMetadata(topicId).getTTL());

      // Update the property and validate
      table.updateTopic(new DefaultTopicMetadata(topicId, "ttl", 30));
      Assert.assertEquals(30, table.getMetadata(topicId).getTTL());

      // Create the same topic again should fail
      try {
        table.createTopic(new DefaultTopicMetadata(topicId, "ttl", 10));
        Assert.fail("Expected TopicAlreadyExistsException");
      } catch (TopicAlreadyExistsException e) {
        // Expected
      }

      // It shouldn't affect the topic at all if creation failed
      Assert.assertEquals(30, table.getMetadata(topicId).getTTL());

      // Delete the topic
      table.deleteTopic(topicId);
      try {
        table.getMetadata(topicId);
        Assert.fail("Expected TopicNotFoundException");
      } catch (TopicNotFoundException e) {
        // Expected
      }

      // Delete again should raise a TopicNotFoundException
      try {
        table.deleteTopic(topicId);
        Assert.fail("Expected TopicNotFoundException");
      } catch (TopicNotFoundException e) {
        // Expected
      }
    }
  }

  protected abstract MetadataTable createMetadataTable() throws Exception;
}
