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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.proto.Id;

import javax.annotation.Nullable;

/**
 * This interface defines method to write to lineage store.
 * It is needed to break circular dependency.
 */
public interface LineageStoreWriter {

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  void addAccess(Id.Run run, Id.DatasetInstance datasetInstance, AccessType accessType, long accessTimeMillis);

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   * @param component program component such as flowlet id, etc.
   */
  void addAccess(Id.Run run, Id.DatasetInstance datasetInstance,
                 AccessType accessType, long accessTimeMillis,
                 @Nullable Id.NamespacedId component);

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  void addAccess(Id.Run run, Id.Stream stream, AccessType accessType, long accessTimeMillis);

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   * @param component program component such as flowlet id, etc.
   */
  void addAccess(Id.Run run, Id.Stream stream,
                 AccessType accessType, long accessTimeMillis,
                 @Nullable Id.NamespacedId component);
}
