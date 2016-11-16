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

import co.cask.cdap.messaging.TopicAlreadyExistsException;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Table to store information about the topics and their properties.
 */
public interface MetadataTable extends Closeable {

  /**
   * Fetch the metadata of the {@link TopicId}.
   *
   * @param topicId message topic
   * @return metadata of the {@link TopicId}
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to retrieve metadata
   */
  TopicMetadata getMetadata(TopicId topicId) throws TopicNotFoundException, IOException;

  /**
   * Create a topic with properties.
   *
   * @param topicMetadata metadata of the topic
   * @throws TopicAlreadyExistsException if the topic already exist
   * @throws IOException if failed to create topic
   */
  void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException;

  /**
   * Updates the topic properties.
   *
   * @param topicMetadata metadata of the topic.
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to update metadata
   */
  void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException;

  /**
   * Delete a topic.
   *
   * @param topicId message topic
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to delete the topic
   */
  void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException;

  /**
   * List all the topics in a namespace.
   *
   * @param namespaceId namespace
   * @return {@link List} of topics in that namespace
   * @throws IOException if failed to retrieve topics
   */
  List<TopicId> listTopics(NamespaceId namespaceId) throws IOException;

  /**
   * List all topics in the messaging system.
   *
   * @return {@link List} of all topics.
   * @throws IOException if failed to retrieve topics
   */
  List<TopicId> listTopics() throws IOException;
}
