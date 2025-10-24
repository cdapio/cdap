/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewMessage;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for consuming preview messages from TMS and persist it to preview store.
 */
public class PreviewDataSubscriberService extends
    AbstractMessagingSubscriberService<PreviewMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewDataSubscriberService.class);
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
      .create();

  private final PreviewStore previewStore;
  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  private final int maxRetriesOnError;
  private final PreviewRunStopper previewRunStopper;
  private int errorCount;
  private String erroredMessageId;
  private MetricsCollectionService metricsCollectionService;

  /**
   * Constructor.
   */
  @Inject
  PreviewDataSubscriberService(CConfiguration cConf,
      @Named(PreviewConfigModule.GLOBAL_TMS) MessagingService messagingService,
      @Named(PreviewConfigModule.GLOBAL_METRICS) MetricsCollectionService
          metricsCollectionService,
      PreviewStore previewStore,
      TransactionRunner transactionRunner,
      PreviewRunStopper previewRunStopper) {
    super(
        NamespaceId.SYSTEM.topic(cConf.get(Constants.Preview.MESSAGING_TOPIC)),
        cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
        cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
        cConf.getLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS),
        RetryStrategies.fromConfiguration(cConf, "system.preview."),
        metricsCollectionService.getContext(ImmutableMap.of(
            Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
            Constants.Metrics.Tag.INSTANCE_ID, "0",
            Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
            Constants.Metrics.Tag.TOPIC, cConf.get(Constants.Preview.MESSAGING_TOPIC),
            Constants.Metrics.Tag.CONSUMER, "preview.writer"
        )));

    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.previewStore = previewStore;
    this.transactionRunner = transactionRunner;
    this.maxRetriesOnError = cConf.getInt(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT);
    this.metricsCollectionService = metricsCollectionService;
    this.previewRunStopper = previewRunStopper;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(), "preview.writer");
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    appMetadataStore.persistSubscriberState(getTopicId().getTopic(), "preview.writer", messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
      Iterator<ImmutablePair<String, PreviewMessage>> messages) throws Exception {
    Map<PreviewMessage.Type, PreviewMessageProcessor> processors = new HashMap<>();

    // Loop over all fetched messages and process them with corresponding PreviewMessageProcessor
    while (messages.hasNext()) {
      ImmutablePair<String, PreviewMessage> next = messages.next();
      String messageId = next.getFirst();
      PreviewMessage message = next.getSecond();
      if (!validateMessage(message)) {
        continue;
      }

      PreviewMessageProcessor processor = processors.computeIfAbsent(message.getType(), type -> {
        switch (type) {
          case DATA:
            return new PreviewDataProcessor();
          case STATUS:
            return new PreviewStatusWriter();
          case PROGRAM_RUN_ID:
            return new PreviewProgramRunIdWriter();
          default:
            return null;
        }
      });

      // noinspection ConstantConditions
      if (processor == null) {
        LOG.warn("Unsupported preview message type {}. Message ignored.", message.getType());
        continue;
      }
      try {
        processor.processMessage(message);
        errorCount = 0;
      } catch (Exception e) {
        if (messageId.equals(erroredMessageId)) {
          errorCount++;
          if (errorCount >= maxRetriesOnError) {
            LOG.warn(
                "Skipping preview message {} after processing it has caused {} consecutive errors: {}",
                message, errorCount, e.getMessage());
            continue;
          }
        } else {
          erroredMessageId = messageId;
          errorCount = 1;
        }
        throw e;
      }
    }
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected PreviewMessage decodeMessage(Message message) {
    return message.decodePayload(r -> GSON.fromJson(r, PreviewMessage.class));
  }

  /**
   * Emit the preview run time metric.
   */
  private void emitRunTimeMetric(PreviewStatus previewStatus, ApplicationId applicationId,
      ProgramRunId programRunId) {
    long runTime = (previewStatus.getEndTime() - previewStatus.getStartTime()) / 1000;
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.STATUS,
        previewStatus.getStatus().name());
    MetricsContext metricsContext = ProgramRunners.createProgramMetricsContext(programRunId, tags,
        metricsCollectionService);
    metricsContext.gauge(Constants.Metrics.Preview.RUN_TIME_SECONDS, runTime);
  }

  /**
   * The {@link PreviewMessageProcessor} for processing preview data.
   */
  private final class PreviewDataProcessor implements PreviewMessageProcessor {

    @Override
    public void processMessage(PreviewMessage message) {
      ApplicationId applicationId = (ApplicationId) message.getEntityId();
      PreviewDataPayload payload;
      try {
        payload = message.getPayload(GSON, PreviewDataPayload.class);
      } catch (Throwable t) {
        LOG.warn(
            "Error while deserializing the preview data message received from TMS. Ignoring : {}",
            message, t);
        return;
      }
      previewStore.put(applicationId, payload.getTracerName(), payload.getPropertyName(),
          payload.getPropertyValue());
    }
  }

  /**
   * The {@link PreviewMessageProcessor} for writing preview status to store.
   */
  private final class PreviewStatusWriter implements PreviewMessageProcessor {

    @Override
    public void processMessage(PreviewMessage message) {
      ApplicationId applicationId = (ApplicationId) message.getEntityId();
      PreviewStatus payload;
      try {
        payload = message.getPayload(GSON, PreviewStatus.class);
      } catch (Throwable t) {
        LOG.warn(
            "Error while deserializing the preview status message received from TMS. Ignoring : {}",
            message, t);
        return;
      }
      PreviewStatus existing = previewStore.getPreviewStatus(applicationId);
      if (existing != null && existing.getStatus().isEndState()) {
        LOG.warn(
            "Preview status for application {} is already in end state {}. Ignoring the update with state {}.",
            applicationId, existing.getStatus(), payload.getStatus());
        return;
      }
      previewStore.setPreviewStatus(applicationId, payload);
      if (payload.getStatus().isEndState()) {
        emitRunTimeMetric(payload, applicationId, previewStore.getProgramRunId(applicationId));
      }
    }
  }

  /**
   * The {@link PreviewMessageProcessor} for writing preview run id to store.
   */
  private final class PreviewProgramRunIdWriter implements PreviewMessageProcessor {

    @Override
    public void processMessage(PreviewMessage message) {
      ProgramRunId payload;
      try {
        payload = message.getPayload(GSON, ProgramRunId.class);
      } catch (Throwable t) {
        LOG.warn(
            "Error while deserializing the preview program run information received from TMS. Ignoring : {}",
            message, t);
        return;
      }
      previewStore.setProgramId(payload);
    }
  }

  /**
   * Validates an incoming {@link PreviewMessage} to enforce security policies. This method performs
   * the following checks:
   * <ul>
   *   <li>Ensures the message is associated with a valid {@link ApplicationId}.</li>
   *   <li>Allows messages that do not contain publisher information to pass (for non-authenticated flows).</li>
   *   <li>If publisher information is present, it validates that the message's {@code ApplicationId}
   *       matches the one registered for that publisher in the {@link PreviewStore}.</li>
   * </ul>
   * If a mismatch is found, it is treated as a violation, and an attempt is made to
   * terminate the offending runner.
   *
   * @param message the message to be validated.
   * @return {@code true} if the message is valid, {@code false} otherwise.
   */
  private boolean validateMessage(PreviewMessage message) {
    if (!(message.getEntityId() instanceof ApplicationId)) {
      LOG.warn("Missing application id from the preview run information. Ignoring message: {}",
          message);
      return false;
    }

    byte[] messageRunnerInfo = message.getPublisherInfo();
    // If there's no publisher info, the message is considered valid but unauthenticated.
    // This allows for backward compatibility or simpler, non-secure, non-distributed environments.
    if (messageRunnerInfo == null) {
      return true;
    }

    ApplicationId appIdFromMessage = (ApplicationId) message.getEntityId();
    byte[] registeredRunnerInfo = previewStore.getPreviewRequestPollerInfo(appIdFromMessage);

    // Happy Path: If no runner is registered for this app yet, or if the messageRunnerInfo from the message
    // matches the registered messageRunnerInfo, the message is valid.
    if (Arrays.equals(registeredRunnerInfo, messageRunnerInfo)) {
      return true;
    }

    // Failure Path: The publisher information does not match.
    LOG.warn(
        "Authentication failure: A message for application '{}' was received with a non-registered "
            + "runner. Terminating the runner.", appIdFromMessage);
    terminateRunner(messageRunnerInfo);

    return false;
  }

  /**
   * This method finds the application the suspicious runner was registered to, marks its status as
   * KILLED, and then stops the runner's container.
   */
  private void terminateRunner(byte[] runnerInfo) {
    try {
      ApplicationId appId = previewStore.getApplicationId(runnerInfo);
      if (appId != null) {
        PreviewStatus status = previewStore.getPreviewStatus(appId);
        if (status != null && !status.getStatus().isEndState()) {
          previewStore.setPreviewStatus(appId,
              new PreviewStatus(PreviewStatus.Status.KILLED,
                  status.getSubmitTime(), new BasicThrowable(new IllegalStateException(
                  "Preview run stopped due to an authentication failure."
                      + "Please try running preview again.")), null, null));
        }
      }
      previewRunStopper.stop(runnerInfo);
    } catch (Exception e) {
      LOG.warn(
          "Failed to stop runner after an authentication failure. The invalid message was still rejected.",
          e);
    }
  }
}
