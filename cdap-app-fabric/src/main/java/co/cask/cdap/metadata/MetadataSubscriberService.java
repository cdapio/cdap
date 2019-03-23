/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.LineageTable;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageTable;
import co.cask.cdap.data2.metadata.writer.DataAccessLineage;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.data2.metadata.writer.MetadataOperation;
import co.cask.cdap.data2.metadata.writer.MetadataOperationTypeAdapter;
import co.cask.cdap.data2.registry.DatasetUsage;
import co.cask.cdap.data2.registry.UsageTable;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import co.cask.cdap.metadata.profile.ProfileMetadataMessageProcessor;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.codec.OperationTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataConstants;
import co.cask.cdap.spi.metadata.MetadataDirective;
import co.cask.cdap.spi.metadata.MetadataKind;
import co.cask.cdap.spi.metadata.MetadataMutation;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Service responsible for consuming metadata messages from TMS and persist it to metadata store.
 * This is a wrapping service to host multiple {@link AbstractMessagingSubscriberService}s for lineage, usage
 * and metadata subscriptions.
 * No transactions should be started in any of the overrided methods since they are already wrapped in a transaction.
 */
public class MetadataSubscriberService extends AbstractMessagingSubscriberService<MetadataMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataSubscriberService.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapter(MetadataOperation.class, new MetadataOperationTypeAdapter())
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  // directives for (re-)creation of system metadata:
  // - keep description if new metadata does not contain it
  // - preserve creation-time if it exists in current metadata
  private static final Map<ScopedNameOfKind, MetadataDirective> CREATE_DIRECTIVES = ImmutableMap.of(
    new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, MetadataConstants.DESCRIPTION_KEY),
    MetadataDirective.KEEP,
    new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, MetadataConstants.CREATION_TIME_KEY),
    MetadataDirective.PRESERVE);

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private final MetadataStorage metadataStorage;
  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  private final int maxRetriesOnConflict;

  private String conflictMessageId = null;
  private int conflictCount = 0;

  @Inject
  MetadataSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            DatasetFramework datasetFramework,
                            MetricsCollectionService metricsCollectionService,
                            MetadataStorage metadataStorage,
                            TransactionRunner transactionRunner) {
    super(
      NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC)),
      cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
      cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
      cConf.getLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS),
      RetryStrategies.fromConfiguration(cConf, "system.metadata."),
      metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
        Constants.Metrics.Tag.INSTANCE_ID, "0",
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.TOPIC, cConf.get(Constants.Metadata.MESSAGING_TOPIC),
        Constants.Metrics.Tag.CONSUMER, "metadata.writer"
      )));

    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.datasetFramework = datasetFramework;
    this.metadataStorage = metadataStorage;
    this.transactionRunner = transactionRunner;
    this.maxRetriesOnConflict = cConf.getInt(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT);
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Override
  protected MetadataMessage decodeMessage(Message message) {
    return GSON.fromJson(message.getPayloadAsString(), MetadataMessage.class);
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws IOException, TableNotFoundException {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(), "metadata.writer");
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId)
    throws IOException, TableNotFoundException {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(), "metadata.writer", messageId);
  }

  @Override
  protected boolean shouldRunInSeparateTx(ImmutablePair<String, MetadataMessage> message) {
    // if this message caused a conflict last time we tried, stop here to commit all messages processed so far
    if (message.getFirst().equals(conflictMessageId)) {
      return true;
    }
    // operations at the instance or namespace level can take time. Stop here to process in a new transaction
    EntityType entityType = message.getSecond().getEntityId().getEntityType();
    return entityType.equals(EntityType.INSTANCE) || entityType.equals(EntityType.NAMESPACE);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, MetadataMessage>> messages)
    throws IOException, ConflictException {
    Map<MetadataMessage.Type, MetadataMessageProcessor> processors = new HashMap<>();

    // Loop over all fetched messages and process them with corresponding MetadataMessageProcessor
    while (messages.hasNext()) {
      ImmutablePair<String, MetadataMessage> next = messages.next();
      String messageId = next.getFirst();
      MetadataMessage message = next.getSecond();

      MetadataMessageProcessor processor = processors.computeIfAbsent(message.getType(), type -> {
        switch (type) {
          case LINEAGE:
            return new DataAccessLineageProcessor();
          case FIELD_LINEAGE:
            return new FieldLineageProcessor();
          case USAGE:
            return new UsageProcessor();
          case WORKFLOW_TOKEN:
          case WORKFLOW_STATE:
            return new WorkflowProcessor();
          case METADATA_OPERATION:
            return new MetadataOperationProcessor(cConf);
          case DATASET_OPERATION:
            return new DatasetOperationMessageProcessor(datasetFramework);
          case PROFILE_ASSIGNMENT:
          case PROFILE_UNASSIGNMENT:
          case ENTITY_CREATION:
          case ENTITY_DELETION:
            return new ProfileMetadataMessageProcessor(metadataStorage, structuredTableContext);
          default:
            return null;
        }
      });

      // Intellij would warn here that the condition is always false - because the switch above covers all cases.
      // But if there is ever an unexpected message, we can't throw exception, that would leave the message there.
      // noinspection ConstantConditions
      if (processor == null) {
        LOG.warn("Unsupported metadata message type {}. Message ignored.", message.getType());
        continue;
      }
      try {
        processor.processMessage(message, structuredTableContext);
        conflictCount = 0;
      } catch (ConflictException e) {
        if (messageId.equals(conflictMessageId)) {
          conflictCount++;
          if (conflictCount >= maxRetriesOnConflict) {
            LOG.warn("Skipping metadata message {} after processing it has caused {} consecutive conflicts: {}",
                     message, conflictCount, e.getMessage());
            continue;
          }
        } else {
          conflictMessageId = messageId;
          conflictCount = 1;
        }
        throw e;
      }
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing {@link DataAccessLineage}.
   */
  private final class DataAccessLineageProcessor implements MetadataMessageProcessor {

    DataAccessLineageProcessor() {}

    @Override
    public void processMessage(MetadataMessage message, StructuredTableContext context) throws IOException {
      if (!(message.getEntityId() instanceof ProgramRunId)) {
        LOG.warn("Missing program run id from the lineage access information. Ignoring the message {}", message);
        return;
      }

      DataAccessLineage lineage = message.getPayload(GSON, DataAccessLineage.class);
      ProgramRunId programRunId = (ProgramRunId) message.getEntityId();
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.addAccess(programRunId, lineage.getDatasetId(), lineage.getAccessType(), lineage.getAccessTime());
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing field lineage.
   */
  private final class FieldLineageProcessor implements MetadataMessageProcessor {

    FieldLineageProcessor() {}


    @Override
    public void processMessage(MetadataMessage message, StructuredTableContext context) throws IOException {
      if (!(message.getEntityId() instanceof ProgramRunId)) {
        LOG.warn("Missing program run id from the field lineage information. Ignoring the message {}", message);
        return;
      }

      ProgramRunId programRunId = (ProgramRunId) message.getEntityId();
      FieldLineageInfo info;
      try {
        info = message.getPayload(GSON, FieldLineageInfo.class);
      } catch (Throwable t) {
        LOG.warn("Error while deserializing the field lineage information message received from TMS. Ignoring : {}",
                 message, t);
        return;
      }
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      fieldLineageTable.addFieldLineageInfo(programRunId, info);
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing {@link DatasetUsage}.
   */
  private final class UsageProcessor implements MetadataMessageProcessor {

    UsageProcessor() {}

    @Override
    public void processMessage(MetadataMessage message, StructuredTableContext context) throws IOException {
      if (!(message.getEntityId() instanceof ProgramId)) {
        LOG.warn("Missing program id from the usage information. Ignoring the message {}", message);
        return;
      }
      DatasetUsage usage = message.getPayload(GSON, DatasetUsage.class);
      ProgramId programId = (ProgramId) message.getEntityId();
      UsageTable usageTable = new UsageTable(context);
      usageTable.register(programId, usage.getDatasetId());
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing workflow state updates.
   */
  private final class WorkflowProcessor implements MetadataMessageProcessor {

    WorkflowProcessor() {}

    @Override
    public void processMessage(MetadataMessage message, StructuredTableContext context) throws IOException {
      if (!(message.getEntityId() instanceof ProgramRunId)) {
        LOG.warn("Missing program run id from the workflow state information. Ignoring the message {}", message);
        return;
      }

      ProgramRunId programRunId = (ProgramRunId) message.getEntityId();

      switch (message.getType()) {
        case WORKFLOW_TOKEN:
          AppMetadataStore.create(context)
            .setWorkflowToken(programRunId, message.getPayload(GSON, BasicWorkflowToken.class));
          break;
        case WORKFLOW_STATE:
          AppMetadataStore.create(context)
            .addWorkflowNodeState(programRunId, message.getPayload(GSON, WorkflowNodeStateDetail.class));
          break;
        default:
          // This shouldn't happen
          LOG.warn("Unknown message type for workflow state information. Ignoring the message {}", message);
      }
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for metadata operations.
   * It receives operations and applies them to the metadata store.
   */
  private class MetadataOperationProcessor extends MetadataValidator implements MetadataMessageProcessor {

    MetadataOperationProcessor(CConfiguration cConf) {
      super(cConf);
    }

    @Override
    public void processMessage(MetadataMessage message, StructuredTableContext context) throws IOException {
      MetadataOperation operation = message.getPayload(GSON, MetadataOperation.class);
      MetadataEntity entity = operation.getEntity();
      LOG.trace("Received {}", operation);
      // TODO: Authorize that the operation is allowed. Currently MetadataMessage does not carry user info
      switch (operation.getType()) {
        case CREATE: {
          // all the new metadata is in System scope - no validation
          MetadataOperation.Create create = (MetadataOperation.Create) operation;
          MetadataMutation mutation = new MetadataMutation.Create(
            entity, new Metadata(MetadataScope.SYSTEM, create.getTags(), create.getProperties()), CREATE_DIRECTIVES);
          metadataStorage.apply(mutation);
          break;
        }
        case DROP: {
          metadataStorage.apply(new MetadataMutation.Drop(operation.getEntity()));
          break;
        }
        case PUT: {
          MetadataOperation.Put put = (MetadataOperation.Put) operation;
          try {
            Set<String> tags = put.getTags() != null ? put.getTags() : Collections.emptySet();
            Map<String, String> props = put.getProperties() != null ? put.getProperties() : Collections.emptyMap();
            if (MetadataScope.USER.equals(put.getScope())) {
              validateProperties(entity, props);
              validateTags(entity, tags);
            }
            metadataStorage.apply(new MetadataMutation.Update(entity, new Metadata(put.getScope(), tags, props)));
          } catch (InvalidMetadataException e) {
            LOG.warn("Ignoring invalid metadata operation {} from TMS: {}", operation,
                     GSON.toJson(message.getRawPayload()), e);
          }
          break;
        }
        case DELETE: {
          MetadataOperation.Delete delete = (MetadataOperation.Delete) operation;
          Set<ScopedNameOfKind> toDelete = new HashSet<>();
          if (delete.getProperties() != null) {
            delete.getProperties().forEach(
              name -> toDelete.add(new ScopedNameOfKind(MetadataKind.PROPERTY, delete.getScope(), name)));
          }
          if (delete.getTags() != null) {
            delete.getTags().forEach(
              name -> toDelete.add(new ScopedNameOfKind(MetadataKind.TAG, delete.getScope(), name)));
          }
          metadataStorage.apply(new MetadataMutation.Remove(entity, toDelete));
          break;
        }
        case DELETE_ALL: {
          MetadataScope scope = ((MetadataOperation.DeleteAll) operation).getScope();
          metadataStorage.apply(new MetadataMutation.Remove(entity, scope));
          break;
        }
        case DELETE_ALL_PROPERTIES: {
          MetadataScope scope = ((MetadataOperation.DeleteAllProperties) operation).getScope();
          metadataStorage.apply(new MetadataMutation.Remove(entity, scope, MetadataKind.PROPERTY));
          break;
        }
        case DELETE_ALL_TAGS: {
          MetadataScope scope = ((MetadataOperation.DeleteAllTags) operation).getScope();
          metadataStorage.apply(new MetadataMutation.Remove(entity, scope, MetadataKind.TAG));
          break;
        }
        default:
          LOG.warn("Ignoring MetadataOperation of unknown type {} for entity {}", operation.getType(), entity);
      }
    }
  }
}
