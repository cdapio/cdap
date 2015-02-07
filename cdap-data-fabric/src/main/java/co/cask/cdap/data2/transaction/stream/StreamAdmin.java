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
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 *
 */
public interface StreamAdmin {

  /**
   * Deletes all entries for all streams.
   */
  void dropAll() throws Exception;

  /**
   * Sets the number of consumer instances for the given consumer group in a stream.
   * @param streamName Name of the stream.
   * @param groupId The consumer group to alter.
   * @param instances Number of instances.
   */
  void configureInstances(Id.Stream streamName, long groupId, int instances) throws Exception;

  /**
   * Sets the consumer groups information for the given stream.
   * @param streamName Name of the stream.
   * @param groupInfo A map from groupId to number of instances of each group.
   */
  void configureGroups(Id.Stream streamName, Map<Long, Integer> groupInfo) throws Exception;

  /**
   * Performs upgrade action for all streams.
   */
  void upgrade() throws Exception;

  /**
   * Returns the configuration of the given stream.
   * @param streamName Name of the stream.
   * @return A {@link StreamConfig} instance.
   * @throws IOException If the stream doesn't exists.
   */
  StreamConfig getConfig(Id.Stream streamName) throws IOException;

  /**
   * Overwrites existing configuration for the given stream.
   * @param config New configuration of the stream.
   */
  void updateConfig(StreamConfig config) throws IOException;

  /**
   * Get the size of the data persisted for the stream with config {@code streamConfig}.
   *
   * @param streamConfig configuration of the stream to get the size of data for
   * @return the size of the data persisted for the stream which config is the {@code streamName}
   * @throws IOException in case of any error in fetching the size
   */
  long fetchStreamSize(StreamConfig streamConfig) throws IOException;

  /////////////////////////
  /**
   * @param name entity name
   * @return true if entity with given name exists, otherwise false
   * @throws Exception if check fails
   */
  boolean exists(Id.Stream name) throws Exception;

  /**
   * Creates entity if doesn't exist. If entity exists does nothing.
   * @param name name of the entity to create
   * @throws Exception if creation fails
   */
  void create(Id.Stream name) throws Exception;

  /**
   * Creates entity if doesn't exist. If entity exists does nothing.
   * @param name name of the entity to create
   * @param props additional properties
   * @throws Exception if creation fails
   */
  void create(Id.Stream name, @Nullable Properties props) throws Exception;

  /**
   * Wipes out entity data.
   * @param name entity name
   * @throws Exception if cleanup fails
   */
  void truncate(Id.Stream name) throws Exception;

  /**
   * Deletes entity from the system completely.
   * @param name entity name
   * @throws Exception if deletion fails
   */
  void drop(Id.Stream name) throws Exception;

}
