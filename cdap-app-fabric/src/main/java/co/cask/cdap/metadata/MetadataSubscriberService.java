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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.LineageTable;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageDataset;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.data2.metadata.writer.DataAccessLineage;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.data2.metadata.writer.MetadataOperation;
import co.cask.cdap.data2.metadata.writer.MetadataOperationTypeAdapter;
import co.cask.cdap.data2.registry.DatasetUsage;
import co.cask.cdap.data2.registry.UsageDataset;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
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
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
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
import org.apache.tephra.TransactionSystemClient;
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
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;

  private DatasetId fieldLineageDatasetId = FieldLineageDataset.FIELD_LINEAGE_DATASET_ID;
  private DatasetId usageDatasetId = UsageDataset.USAGE_INSTANCE_ID;

  @Inject
  MetadataSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            DatasetFramework datasetFramework, TransactionSystemClient txClient,
                            MetricsCollectionService metricsCollectionService,
                            MetadataStorage metadataStorage,
                            TransactionRunner transactionRunner) {
    super(
      NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC)),
      true, cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
      cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
      cConf.getInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT),
      cConf.getLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS),
      RetryStrategies.fromConfiguration(cConf, "system.metadata."),
      metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
        Constants.Metrics.Tag.INSTANCE_ID, "0",
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.TOPIC, cConf.get(Constants.Metadata.MESSAGING_TOPIC),
        Constants.Metrics.Tag.CONSUMER, "metadata.writer"
      ))
    );

    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.datasetFramework = datasetFramework;
    this.metadataStorage = metadataStorage;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
    this.transactionRunner = transactionRunner;
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected Transactional getTransactional() {
    return transactional;
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
  protected boolean shouldRunInSeparateTx(MetadataMessage message) {
    EntityType entityType = message.getEntityId().getEntityType();
    return entityType.equals(EntityType.INSTANCE) || entityType.equals(EntityType.NAMESPACE);
  }

  @Override
  protected void processMessages(DatasetContext datasetContext, StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, MetadataMessage>> messages) throws IOException {
    Map<MetadataMessage.Type, MetadataMessageProcessor> processors = new HashMap<>();

    // Loop over all fetched messages and process them with corresponding MetadataMessageProcessor
    while (messages.hasNext()) {
      MetadataMessage message = messages.next().getSecond();

      MetadataMessageProcessor processor = processors.computeIfAbsent(message.getType(), type -> {
        switch (type) {
          case LINEAGE:
            return new DataAccessLineageProcessor();
          case FIELD_LINEAGE:
            return new FieldLineageProcessor(datasetContext);
          case USAGE:
            return new UsageProcessor(datasetContext);
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

      // look like intellij doesn't understand return from closures and consider it as function return.
      // noinspection ConstantConditions
      if (processor == null) {
        LOG.warn("Unsupported metadata message type {}. Message ignored.", message.getType());
        continue;
      }

      processor.processMessage(message);
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing {@link DataAccessLineage}.
   */
  private final class DataAccessLineageProcessor implements MetadataMessageProcessor {

    DataAccessLineageProcessor() {}

    @Override
    public void processMessage(MetadataMessage message) {
      if (!(message.getEntityId() instanceof ProgramRunId)) {
        LOG.warn("Missing program run id from the lineage access information. Ignoring the message {}", message);
        return;
      }

      DataAccessLineage lineage = message.getPayload(GSON, DataAccessLineage.class);
      ProgramRunId programRunId = (ProgramRunId) message.getEntityId();
      TransactionRunners.run(transactionRunner, context -> {
        LineageTable lineageTable = LineageTable.create(context);
        lineageTable.addAccess(programRunId, lineage.getDatasetId(), lineage.getAccessType(), lineage.getAccessTime());
      });
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing field lineage.
   */
  private final class FieldLineageProcessor implements MetadataMessageProcessor {

    private final FieldLineageDataset fieldLineageDataset;

    FieldLineageProcessor(DatasetContext datasetContext) {
      this.fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(datasetContext, datasetFramework,
                                                                            fieldLineageDatasetId);
    }

    @Override
    public void processMessage(MetadataMessage message) {
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
      fieldLineageDataset.addFieldLineageInfo(programRunId, info);
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing {@link DatasetUsage}.
   */
  private final class UsageProcessor implements MetadataMessageProcessor {

    private final UsageDataset usageDataset;

    UsageProcessor(DatasetContext datasetContext) {
      this.usageDataset = UsageDataset.getUsageDataset(datasetContext, datasetFramework, usageDatasetId);
    }

    @Override
    public void processMessage(MetadataMessage message) {
      if (!(message.getEntityId() instanceof ProgramId)) {
        LOG.warn("Missing program id from the usage information. Ignoring the message {}", message);
        return;
      }
      DatasetUsage usage = message.getPayload(GSON, DatasetUsage.class);
      ProgramId programId = (ProgramId) message.getEntityId();
      usageDataset.register(programId, usage.getDatasetId());
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing workflow state updates.
   */
  private final class WorkflowProcessor implements MetadataMessageProcessor {

    WorkflowProcessor() {}

    @Override
    public void processMessage(MetadataMessage message) {
      if (!(message.getEntityId() instanceof ProgramRunId)) {
        LOG.warn("Missing program run id from the workflow state information. Ignoring the message {}", message);
        return;
      }

      ProgramRunId programRunId = (ProgramRunId) message.getEntityId();

      switch (message.getType()) {
        case WORKFLOW_TOKEN:
          TransactionRunners.run(transactionRunner, context -> {
            AppMetadataStore.create(context)
              .setWorkflowToken(programRunId, message.getPayload(GSON, BasicWorkflowToken.class));
          });
          break;
        case WORKFLOW_STATE:
          TransactionRunners.run(transactionRunner, context -> {
            AppMetadataStore.create(context)
              .addWorkflowNodeState(programRunId, message.getPayload(GSON, WorkflowNodeStateDetail.class));
          });
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
    public void processMessage(MetadataMessage message) throws IOException {
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
