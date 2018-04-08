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

package co.cask.cdap.metadata;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.LineageDataset;
import co.cask.cdap.data2.metadata.writer.DataAccessLineage;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.data2.registry.DatasetUsage;
import co.cask.cdap.data2.registry.UsageDataset;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Service responsible for consuming metadata messages from TMS and persist it to metadata store.
 * This is a wrapping service to host multiple {@link AbstractMessagingSubscriberService}s for lineage, usage
 * and metadata subscriptions.
 */
public class MetadataSubscriberService extends AbstractMessagingSubscriberService<MetadataMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataSubscriberService.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;

  private DatasetId lineageDatasetId = LineageDataset.LINEAGE_DATASET_ID;
  private DatasetId usageDatasetId = UsageDataset.USAGE_INSTANCE_ID;

  @Inject
  MetadataSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            DatasetFramework datasetFramework, TransactionSystemClient txClient,
                            MetricsCollectionService metricsCollectionService) {
    super(
      NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC)),
      true, cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
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
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Sets the {@link DatasetId} for the {@link LineageDataset}. This method is only for testing.
   */
  @VisibleForTesting
  MetadataSubscriberService setLineageDatasetId(DatasetId lineageDatasetId) {
    this.lineageDatasetId = lineageDatasetId;
    return this;
  }

  /**
   * Sets the {@link DatasetId} for the {@link UsageDataset}. This method is only for testing.
   */
  @VisibleForTesting
  MetadataSubscriberService setUsageDatasetId(DatasetId usageDatasetId) {
    this.usageDatasetId = usageDatasetId;
    return this;
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
  protected MetadataMessage decodeMessage(Message message) throws Exception {
    return GSON.fromJson(message.getPayloadAsString(), MetadataMessage.class);
  }

  @Nullable
  @Override
  protected String loadMessageId(DatasetContext datasetContext) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(cConf, datasetContext, datasetFramework);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(), "metadata.writer");
  }

  @Override
  protected void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(cConf, datasetContext, datasetFramework);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(), "metadata.writer", messageId);
  }

  @Override
  protected void processMessages(DatasetContext datasetContext,
                                 Iterator<ImmutablePair<String, MetadataMessage>> messages) throws Exception {
    Map<MetadataMessage.Type, MetadataMessageProcessor> processors = new HashMap<>();

    // Loop over all fetched messages and process them with corresponding MetadataMessageProcessor
    while (messages.hasNext()) {
      MetadataMessage message = messages.next().getSecond();

      MetadataMessageProcessor processor = processors.computeIfAbsent(message.getType(), type -> {
        switch (type) {
          case LINEAGE:
            return new DataAccessLineageProcessor(datasetContext);
          case USAGE:
            return new UsageProcessor(datasetContext);
          case WORKFLOW_TOKEN:
          case WORKFLOW_STATE:
            return new WorkflowProcessor(datasetContext);
          default:
            return null;
        }
      });

      if (processor == null) {
        LOG.warn("Unsupported metadata message type {}. Message ignored.", message.getType());
        continue;
      }

      processor.processMessage(message);
    }
  }

  /**
   * Private interface to help dispatch and process different types of {@link MetadataMessage}.
   */
  private interface MetadataMessageProcessor {

    /**
     * Processes one {@link MetadataMessage}.
     */
    void processMessage(MetadataMessage message);
  }

  /**
   * The {@link MetadataMessageProcessor} for processing {@link DataAccessLineage}.
   */
  private final class DataAccessLineageProcessor implements MetadataMessageProcessor {

    private final LineageDataset lineageDataset;

    DataAccessLineageProcessor(DatasetContext datasetContext) {
      this.lineageDataset = LineageDataset.getLineageDataset(datasetContext, datasetFramework, lineageDatasetId);
    }

    @Override
    public void processMessage(MetadataMessage message) {
      if (message.getRunId() == null) {
        LOG.warn("Missing program run id from the lineage access information. Ignoring the message {}", message);
        return;
      }

      DataAccessLineage lineage = message.getPayload(GSON, DataAccessLineage.class);
      ProgramRunId programRunId = message.getProgramId().run(message.getRunId());

      if (lineage.getDatasetId() != null) {
        lineageDataset.addAccess(programRunId, lineage.getDatasetId(),
                                 lineage.getAccessType(), lineage.getAccessTime(), lineage.getComponentId());
      } else if (lineage.getStreamId() != null) {
        lineageDataset.addAccess(programRunId, lineage.getStreamId(),
                                 lineage.getAccessType(), lineage.getAccessTime(), lineage.getComponentId());
      } else {
        // This shouldn't happen
        LOG.warn("Missing dataset id from the lineage access information. Ignoring the message {}", message);
      }
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
      DatasetUsage usage = message.getPayload(GSON, DatasetUsage.class);
      if (usage.getDatasetId() != null) {
        usageDataset.register(message.getProgramId(), usage.getDatasetId());
      } else if (usage.getStreamId() != null) {
        usageDataset.register(message.getProgramId(), usage.getStreamId());
      } else {
        // This shouldn't happen
        LOG.warn("Missing dataset id from the usage information. Ignoring the message {}", message);
      }
    }
  }

  /**
   * The {@link MetadataMessageProcessor} for processing workflow state updates.
   */
  private final class WorkflowProcessor implements MetadataMessageProcessor {

    private final AppMetadataStore appMetadataStore;

    WorkflowProcessor(DatasetContext datasetContext) {
      this.appMetadataStore = AppMetadataStore.create(cConf, datasetContext, datasetFramework);
    }

    @Override
    public void processMessage(MetadataMessage message) {
      if (message.getRunId() == null) {
        LOG.warn("Missing program run id from the workflow state information. Ignoring the message {}", message);
        return;
      }

      ProgramRunId programRunId = message.getProgramId().run(message.getRunId());

      switch (message.getType()) {
        case WORKFLOW_TOKEN:
          appMetadataStore.setWorkflowToken(programRunId, message.getPayload(GSON, BasicWorkflowToken.class));
        break;
        case WORKFLOW_STATE:
          appMetadataStore.addWorkflowNodeState(programRunId, message.getPayload(GSON, WorkflowNodeStateDetail.class));
        break;
        default:
          // This shouldn't happen
          LOG.warn("Unknown message type for workflow state information. Ignoring the message {}", message);
      }
    }
  }
}
