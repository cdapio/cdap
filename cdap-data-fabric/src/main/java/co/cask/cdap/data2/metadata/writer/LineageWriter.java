/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;

import javax.annotation.Nullable;

/**
 * Defines an interface to record program-dataset access records.
 */
public interface LineageWriter {
  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   */
  default void addAccess(ProgramRunId run, DatasetId datasetInstance, AccessType accessType) {
    addAccess(run, datasetInstance, accessType, null);
  }

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param component program component such as flowlet id, etc.
   */
  void addAccess(ProgramRunId run, DatasetId datasetInstance,
                 AccessType accessType, @Nullable NamespacedEntityId component);

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   */
  default void addAccess(ProgramRunId run, StreamId stream, AccessType accessType) {
    addAccess(run, stream, accessType, null);
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param component program component such as flowlet id, etc.
   */
  void addAccess(ProgramRunId run, StreamId stream,
                 AccessType accessType, @Nullable NamespacedEntityId component);
}
