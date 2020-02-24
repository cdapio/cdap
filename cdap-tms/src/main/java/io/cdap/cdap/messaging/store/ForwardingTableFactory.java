/*
 * Copyright © 2017 Cask Data, Inc.
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

import io.cdap.cdap.messaging.TopicMetadata;

import java.io.IOException;

/**
 * A {@link TableFactory} that forwards all operations to another {@link TableFactory}.
 */
public abstract class ForwardingTableFactory implements TableFactory {

  /**
   * Returns the {@link TableFactory} that this class forwards to.
   */
  public abstract TableFactory getDelegate();

  @Override
  public MetadataTable createMetadataTable() throws IOException {
    return getDelegate().createMetadataTable();
  }

  @Override
  public MessageTable createMessageTable(TopicMetadata topicMetadata) throws IOException {
    return getDelegate().createMessageTable(topicMetadata);
  }

  @Override
  public PayloadTable createPayloadTable(TopicMetadata topicMetadata) throws IOException {
    return getDelegate().createPayloadTable(topicMetadata);
  }

  @Override
  public void close() throws IOException {
    getDelegate().close();
  }
}
