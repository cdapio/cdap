/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;

/**
 * This interface exposes functionality for publishing entity metadata.
 */
public interface MetadataPublisher {

  /**
   * Publishes the {@link MetadataOperation} from the given publisher's {@link EntityId}.
   *
   * @param publisher the id of the publisher - typically a {@link ProgramRunId}
   * @param metadataOperation the {@link MetadataOperation}
   */
  void publish(EntityId publisher, MetadataOperation metadataOperation);

  /**
   * Publishes the {@link DatasetInstanceOperation}.
   *
   * @param entityId the {@link EntityId} that the operation happened. It must be of either an instance of
   *                 {@link DatasetId} or {@link NamespaceId}. If it is {@link NamespaceId}, only
   *                 {@link DatasetInstanceOperation.Type#DELETE} is supported, which is for deleting all
   *                 datasets in the given namespace.
   * @param datasetInstanceOperation the {@link DatasetInstanceOperation} to publish
   */
  void publish(EntityId entityId, DatasetInstanceOperation datasetInstanceOperation);
}
