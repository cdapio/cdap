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

package io.cdap.cdap.metadata;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.DatasetInstanceOperation;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.internal.app.runtime.ThrowingRunnable;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.spi.data.StructuredTableContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetadataMessageProcessor} for processing message type of {@link MetadataMessage.Type#DATASET_OPERATION}.
 */
final class DatasetOperationMessageProcessor implements MetadataMessageProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetOperationMessageProcessor.class);
  private static final Gson GSON = new Gson();

  private final DatasetFramework datasetFramework;

  DatasetOperationMessageProcessor(DatasetFramework datasetFramework) {
    this.datasetFramework = datasetFramework;
  }

  @Override
  public void processMessage(MetadataMessage message, StructuredTableContext context) {
    EntityId entityId = message.getEntityId();
    DatasetInstanceOperation operation = message.getPayload(GSON, DatasetInstanceOperation.class);
    Principal principal = operation.getPrincipal();

    try {
      switch (operation.getType()) {
        case CREATE:
          if (!(entityId instanceof DatasetId)) {
            LOG.warn("Ignoring create dataset message without DatasetId {}", message);
            return;
          }
          runWithPrincipal(principal, () -> DatasetsUtil.createIfNotExists(datasetFramework, (DatasetId) entityId,
                                                                           operation.getDatasetTypeName(),
                                                                           operation.getProperties()));
          break;
        case UPDATE:
          try {
            if (!(entityId instanceof DatasetId)) {
              LOG.warn("Ignoring update dataset message without DatasetId {}", message);
              return;
            }
            runWithPrincipal(principal, () -> datasetFramework.updateInstance((DatasetId) entityId,
                                                                              operation.getProperties()));
          } catch (IncompatibleUpdateException e) {
            // Can't do much here since the remote runtime doesn't really knows about incompatibility
            // (e.g. two concurrent runs change the properties in incompatible way).
            // We just let the first one win, since there is no point in retrying as it could be just keep failing
            LOG.warn("Ignoring incompatible dataset properties change on dataset {} with new properties {}",
                     entityId, operation.getProperties());
          }
          break;
        case DELETE:
          if (entityId instanceof DatasetId) {
            runWithPrincipal(principal, () -> datasetFramework.deleteInstance((DatasetId) entityId));
          } else if (entityId instanceof NamespaceId) {
            runWithPrincipal(principal, () -> datasetFramework.deleteAllInstances((NamespaceId) entityId));
          } else {
            LOG.warn("Ignoring delete dataset message without DatasetId or NamespaceId {}", message);
          }
          break;
        default:
          LOG.warn("Ignoring unsupported dataset operation type {}", operation.getType());
      }
    } catch (Exception e) {
      // Just bubble all exceptions. The MetadataSubscriberService will retry.
      throw Throwables.propagate(e);
    }
  }

  /**
   * Executes the given {@link ThrowingRunnable} by setting the {@link SecurityRequestContext} based on the given
   * {@link Principal}.
   */
  private void runWithPrincipal(Principal principal, ThrowingRunnable runnable) throws Exception {
    String oldUserId = SecurityRequestContext.getUserId();
    try {
      SecurityRequestContext.setUserId(principal.getName());
      runnable.run();
    } finally {
      SecurityRequestContext.setUserId(oldUserId);
    }
  }
}
