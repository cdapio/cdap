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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.proto.Id;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumer}.
 */
public interface StreamConsumerFactory {

  /**
   * Creates a {@link StreamConsumer}.
   *
   * @param streamId Id of the stream
   * @param namespace application namespace for the state table.
   * @param consumerConfig consumer configuration.
   * @return a new instance of {@link StreamConsumer}.
   */
  StreamConsumer create(Id.Stream streamId, String namespace, ConsumerConfig consumerConfig) throws IOException;

  /**
   * Deletes all consumer states for the given namespace and group ids.
   *
   * @param streamId Id of the stream
   * @param namespace application namespace for the state table.
   * @param groupIds set of group id that needs to have states cleared.
   */
  void dropAll(Id.Stream streamId, String namespace, Iterable<Long> groupIds) throws IOException;
}
