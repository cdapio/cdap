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

import co.cask.cdap.proto.Id;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumerStateStore} instance for different streams.
 */
public interface StreamConsumerStateStoreFactory {

  /**
   * Creates a {@link StreamConsumerStateStore} for the given stream.
   *
   * @param streamConfig Configuration of the stream.
   * @return a new state store instance.
   */
  StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException;

  /**
   * Deletes all consumer state stores.
   */
  void dropAllInNamespace(Id.Namespace namespace) throws IOException;
}
