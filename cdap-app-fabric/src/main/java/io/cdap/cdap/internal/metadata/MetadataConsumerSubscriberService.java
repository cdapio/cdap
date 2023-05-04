/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.data2.metadata.lineage.field.EndpointFieldDeserializer;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.data2.metadata.writer.MetadataOperation;
import io.cdap.cdap.data2.metadata.writer.MetadataOperationTypeAdapter;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.metadata.MetadataMessageProcessor;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.codec.OperationTypeAdapter;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.metadata.Asset;
import io.cdap.cdap.spi.metadata.LineageInfo;
import io.cdap.cdap.spi.metadata.MetadataConsumer;
import io.cdap.cdap.spi.metadata.MetadataConsumerMetrics;
import io.cdap.cdap.spi.metadata.ProgramRun;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for consuming metadata messages from TMS and consume it in ext modules if configured.
 * This is a wrapping service to host multiple {@link AbstractMessagingSubscriberService}s for lineage subscriptions.
 * No transactions should be started in any of the overridden methods since they are already wrapped in a transaction.
 */
public class MetadataConsumerSubscriberService extends AbstractMessagingSubscriberService<MetadataMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataConsumerSubscriberService.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapter(MetadataOperation.class, new MetadataOperationTypeAdapter())
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();
  private static final String FQN = "fqn";

  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  private final int maxRetriesOnConflict;
  private final CConfiguration cConf;
  private final MetadataConsumerExtensionLoader provider;
  private final MetricsCollectionService metricsCollectionService;

  private String conflictMessageId;
  private int conflictCount;

  @Inject
  MetadataConsumerSubscriberService(CConfiguration cConf, MessagingService messagingService,
                                    MetricsCollectionService metricsCollectionService,
                                    TransactionRunner transactionRunner,
                                    MetadataConsumerExtensionLoader provider) {
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
        Constants.Metrics.Tag.CONSUMER, Constants.Metadata.METADATA_WRITER_SUBSCRIBER
      )));
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
    this.maxRetriesOnConflict = cConf.getInt(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT);
    this.cConf = cConf;
    this.provider = provider;
    this.metricsCollectionService = metricsCollectionService;
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
    return message.decodePayload(r -> GSON.fromJson(r, MetadataMessage.class));
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws IOException, TableNotFoundException {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(),
                                                    Constants.Metadata.METADATA_WRITER_SUBSCRIBER);
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId)
    throws IOException, TableNotFoundException {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(),
                                            Constants.Metadata.METADATA_WRITER_SUBSCRIBER, messageId);
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
  protected void preProcess() {
    // no-op
  }

  @Override
  protected void doStartUp() throws Exception {
    super.doStartUp();
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
        if (type == MetadataMessage.Type.FIELD_LINEAGE) {
          return new FieldLineageProcessor(this.cConf, this.provider.loadMetadataConsumers(),
                                           this.metricsCollectionService);
        }
        return null;
      });
      // Intellij would warn here that the condition is always false - because the switch above covers all cases.
      // But if there is ever an unexpected message, we can't throw exception, that would leave the message there.
      if (processor == null) {
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
   * The {@link MetadataMessageProcessor} for processing field lineage.
   */
  private static final class FieldLineageProcessor implements MetadataMessageProcessor {

    private final CConfiguration cConf;
    private final Map<String, MetadataConsumer> consumers;
    private final MetricsCollectionService metricsCollectionService;

    FieldLineageProcessor(CConfiguration cConf, Map<String, MetadataConsumer> consumers,
                          MetricsCollectionService metricsCollectionService) {
      this.cConf = cConf;
      this.consumers = consumers;
      this.metricsCollectionService = metricsCollectionService;
    }

    @Override
    public void processMessage(MetadataMessage message, StructuredTableContext context) {
      if (!(message.getEntityId() instanceof ProgramRunId)) {
        LOG.warn("Missing program run id from the field lineage information. Ignoring the message {}", message);
        return;
      }

      ProgramRunId programRunId = (ProgramRunId) message.getEntityId();
      FieldLineageInfo fieldLineageInfo;
      try {
        Gson gson = new GsonBuilder()
          .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
          .registerTypeAdapter(MetadataOperation.class, new MetadataOperationTypeAdapter())
          .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
          .registerTypeAdapter(EndPointField.class, new EndpointFieldDeserializer())
          .create();

        fieldLineageInfo = message.getPayload(gson, FieldLineageInfo.class);
      } catch (Throwable t) {
        LOG.warn("Error while deserializing the field lineage information message received from TMS. Ignoring : {}",
                 message, t);
        return;
      }
      ProgramRun run;
      LineageInfo info;
      try {
        // create ProgramRun and LineageInfo for MetadataConsumer
        long startTimeMs = RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS);
        long endTimeMs = System.currentTimeMillis();
        LOG.info("------startTimeMs - {}-----", startTimeMs);
        LOG.info("------endTimeMs - {}-----", endTimeMs);
        run = getProgramRunForConsumer(programRunId, startTimeMs, endTimeMs);
        LOG.info("------run - {}-----", run);
        info = getLineageInfoForConsumer(fieldLineageInfo, startTimeMs, endTimeMs);
        LOG.info("------info - {}-----", info);
        LOG.info("------sources - {}-----", info.getSources());
        LOG.info("------targets - {}-----", info.getTargets());
        LOG.info("------source to targets - {}-----", info.getSourceToTargets());
        LOG.info("------target to sources - {}-----", info.getTargetToSources());
        LOG.info("------lineage ID - {}-----", info.getLineageId());
        LOG.info("------start time ms - {}-----", info.getStartTimeMs());
        LOG.info("------end time ms - {}-----", info.getEndTimeMs());
      } catch (IllegalArgumentException e) {
        LOG.warn("Error while processing field-lineage information received from TMS. Ignoring : {}", message, e);
        return;
      }
      this.consumers.forEach((key, consumer) -> {
        LOG.info("------creating consumer context-----");
        DefaultMetadataConsumerContext metadataConsumerContext =
          new DefaultMetadataConsumerContext(cConf, consumer.getName(), this.metricsCollectionService,
                                             run.getNamespace(), run.getApplication());
        LOG.info("------consumer context - {}-----", metadataConsumerContext);
        LOG.info("------consumer context properties - {}-----", metadataConsumerContext.getProperties());
        MetadataConsumerMetrics metrics = metadataConsumerContext.getMetrics(Collections.emptyMap());
        LOG.info("------consumer context metrics - {}-----", metrics);
        try {
          metrics.increment("metadata.consumer.calls.attempted", 1);
          // if there is any error from the implementation, log and continue here
          // as we are already retrying at the service level
          LOG.info("------incremented calls metrics-----");
          consumer.consumeLineage(metadataConsumerContext, run, info);
          LOG.info("------successfully called consumer without errors-----");
        } catch (Throwable e) {
          LOG.error("Error calling the metadata consumer {}: {}", consumer.getName(), e.getMessage());
          metrics.increment("metadata.consumer.calls.failed", 1);
          LOG.info("------incremented error metrics-----");
        }
      });
    }

    private ProgramRun getProgramRunForConsumer(ProgramRunId programRunId, long startTimeMs, long endTimeMs) {
      return ProgramRun.builder(programRunId.getRun())
        .setProgramId(programRunId.getParent().toString())
        .setApplication(programRunId.getApplication())
        .setNamespace(programRunId.getNamespace())
        .setStatus(ProgramRunStatus.COMPLETED.name())
        .setStartTimeMs(startTimeMs)
        .setEndTimeMs(endTimeMs)
        .build();
    }

    private LineageInfo getLineageInfoForConsumer(FieldLineageInfo lineage, long startTimeMs,
        long endTimeMs) {
      LOG.info("------fll sources - {}-----", lineage.getSources());
      LOG.info("------fll dests - {}-----", lineage.getDestinations());
      LOG.info("------fll dest fields- {}-----", lineage.getDestinationFields());
      LOG.info("------fll operations - {}-----", lineage.getOperations());
      LOG.info("------fll inc summ - {}-----", lineage.getIncomingSummary());
      LOG.info("------fll out summ- {}-----", lineage.getOutgoingSummary());
      LOG.info("------fll checksum - {}-----", lineage.getChecksum());
      return LineageInfo.builder()
          .setStartTimeMs(startTimeMs)
          .setEndTimeMs(endTimeMs)
          .setSources(lineage.getSources().stream().map(this::getAssetForEndpoint)
              .collect(Collectors.toSet()))
          .setTargets(lineage.getDestinations().stream().map(this::getAssetForEndpoint)
              .collect(Collectors.toSet()))
          .setTargetToSources(getAssetsMapFromEndpointFieldsMap(lineage.getIncomingSummary()))
          .setSourceToTargets(getAssetsMapFromEndpointFieldsMap(lineage.getOutgoingSummary()))
          .build();
    }

    private Asset getAssetForEndpoint(EndPoint endPoint) {
      // if the instance is upgraded and the pipeline is not, then there is a chance that fqn is null
      // throw illegal argument exception so that such messages can be ignored as it does not make sense to retry
      Map<String, String> properties = endPoint.getProperties();
      if (!properties.containsKey(FQN) || (properties.containsKey(FQN) && properties.get(FQN) == null)) {
        throw new IllegalArgumentException("FQN for the asset is null");
      }
      return new Asset(properties.get(FQN));
    }

    private Map<Asset, Set<Asset>> getAssetsMapFromEndpointFieldsMap(Map<EndPointField, Set<EndPointField>>
                                                                       endPointFieldSetMap) {
      return endPointFieldSetMap.entrySet().stream()
        .collect(Collectors.toMap(entry -> getAssetForEndpoint(entry.getKey().getEndPoint()),
                                  entry -> entry.getValue().stream()
                                    .filter(EndPointField::isValid)
                                    .map(endPointField ->
                                           getAssetForEndpoint(endPointField.getEndPoint()))
                                    .collect(Collectors.toSet()),
                                  (first, second) -> Stream.of(first, second).flatMap(Set::stream)
                                    .collect(Collectors.toSet())));
    }
  }
}
