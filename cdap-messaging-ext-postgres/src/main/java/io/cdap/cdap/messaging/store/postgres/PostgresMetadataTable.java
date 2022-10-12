/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.postgres;

import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.TopicId;
import io.cdap.cdap.messaging.store.MetadataTable;

import java.io.IOException;
import java.util.List;

public class PostgresMetadataTable implements MetadataTable {
  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws TopicNotFoundException, IOException {
    return null;
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {

  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {

  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {

  }

  @Override
  public List<TopicId> listTopics(String namespaceId) throws IOException {
    return null;
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
