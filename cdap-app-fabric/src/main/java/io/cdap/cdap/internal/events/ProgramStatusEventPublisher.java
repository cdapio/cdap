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

package io.cdap.cdap.internal.events;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.MetricRetrievalException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerInfos;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.events.EventWriter;
import io.cdap.cdap.spi.events.ExecutionMetrics;
import io.cdap.cdap.spi.events.ProgramStatusEvent;
import io.cdap.cdap.spi.events.ProgramStatusEventDetails;
import io.cdap.cdap.spi.events.StartMetadata;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EventPublisher} implementation for program status
 */
public class ProgramStatusEventPublisher extends AbstractNotificationSubscriberService
    implements EventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(EventPublishManager.class);

  private static final String SUBSCRIBER_NAME = "program_status_event_publisher";
  private static final Gson GSON = new Gson();
  private static final String EVENT_VERSION = "v1";
  private static final String PROGRAM_STATUS_EVENT_PUBLISHER = "program.status.event.publisher";
  private final String instanceName;
  private final String projectName;
  private final CConfiguration cConf;
  private final MetricsProvider metricsProvider;
  private Collection<EventWriter> eventWriters;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  protected ProgramStatusEventPublisher(CConfiguration cConf,
      MessagingService messagingService,
      MetricsCollectionService metricsCollectionService,
      TransactionRunner transactionRunner,
      MetricsProvider metricsProvider) {
    super(PROGRAM_STATUS_EVENT_PUBLISHER, cConf,
        cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
        cConf.getInt(Constants.Event.PROGRAM_STATUS_FETCH_SIZE),
        cConf.getInt(Constants.Event.PROGRAM_STATUS_POLL_INTERVAL_SECONDS), messagingService,
        metricsCollectionService,
        transactionRunner);
    this.cConf = cConf;
    this.instanceName = cConf.get(Constants.Event.INSTANCE_NAME);
    this.projectName = cConf.get(Constants.Event.PROJECT_NAME);
    this.metricsProvider = metricsProvider;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public void initialize(Collection<EventWriter> eventWriters) {
    this.eventWriters = eventWriters;
    this.eventWriters.forEach(eventWriter -> {
      DefaultEventWriterContext eventWriterContext = new DefaultEventWriterContext(cConf,
          eventWriter.getID());
      try {
        eventWriter.initialize(eventWriterContext);
      } catch (Throwable e) {
        LOG.error("Error initializing event writer {}: {}", eventWriter.getID(), e.getMessage());
      }
    });
  }

  @Override
  public void startPublish() {
    super.startAndWait();
  }

  @Override
  public void stopPublish() {
    this.stop();
  }

  @Override
  public String getID() {
    return "ProgramStatusEventPublisher";
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws Exception {
    return AppMetadataStore.create(context)
        .retrieveSubscriberState(getTopicId().getTopic(), SUBSCRIBER_NAME);
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
    AppMetadataStore.create(context)
        .persistSubscriberState(getTopicId().getTopic(), SUBSCRIBER_NAME, messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
      Iterator<ImmutablePair<String, Notification>> messages) {
    messages.forEachRemaining(message -> {
      Notification notification = message.getSecond();
      if (!notification.getNotificationType().equals(Notification.Type.PROGRAM_STATUS)) {
        return;
      }
      Map<String, String> properties = notification.getProperties();
      //get program run ID
      String programStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);
      if (programStatus == null) {
        return;
      }
      ProgramRunStatus programRunStatus = ProgramRunStatus.valueOf(programStatus);
      String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);

      //Should event publish happen for this status
      if (!shouldPublish(programRunId)) {
        return;
      }
      String userArgsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
      String sysArgsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      Type argsMapType = new TypeToken<Map<String, String>>() {
      }.getType();
      ProgramStatusEventDetails.Builder builder = ProgramStatusEventDetails
          .getBuilder(programRunId.getRun(), programRunId.getApplication(),
              programRunId.getProgram(),
              programRunId.getNamespace(),
              programStatus,
              RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS));
      StartMetadata startMetadata = TriggerInfos.getStartMetadata(
          GSON.fromJson(sysArgsString, argsMapType));
      builder = builder
          .withUserArgs(GSON.fromJson(userArgsString, argsMapType))
          .withSystemArgs(GSON.fromJson(sysArgsString, argsMapType))
          .withStartMetadata(startMetadata);
      if (programRunStatus.isEndState()) {
        populateErrorDetailsAndMetrics(builder, properties, programRunStatus, programRunId);
      } else {
        writeInEventWriters(builder);
      }
    });
  }

  private void writeInEventWriters(ProgramStatusEventDetails.Builder builder) {
    long publishTime = System.currentTimeMillis();
    ProgramStatusEventDetails programStatusEventDetails = builder.build();
    ProgramStatusEvent programStatusEvent = new ProgramStatusEvent(publishTime, EVENT_VERSION,
        instanceName,
        projectName, programStatusEventDetails);
    List<ProgramStatusEvent> listProgramStatus = new ArrayList<>();
    listProgramStatus.add(programStatusEvent);
    emitMetrics(programStatusEvent);
    this.eventWriters.forEach(eventWriter -> eventWriter.write(listProgramStatus));
  }

  private void emitMetrics(ProgramStatusEvent programStatusEvent) {
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.NAMESPACE,
        programStatusEvent.getEventDetails().getNamespace());
    metricTags.put(Constants.Metrics.Tag.STATUS, programStatusEvent.getEventDetails().getStatus());
    metricTags.put(Constants.Metrics.Tag.PROGRAM,
        programStatusEvent.getEventDetails().getProgramName());
    metricTags.put(Constants.Metrics.Tag.APP,
        programStatusEvent.getEventDetails().getApplicationName());
    metricsCollectionService.getContext(metricTags)
        .increment(Constants.Metrics.ProgramEvent.PUBLISHED_COUNT, 1L);
  }

  private boolean shouldPublish(ProgramRunId programRunId) {
    return !NamespaceId.SYSTEM.equals(programRunId.getNamespaceId());
  }

  private void populateErrorDetailsAndMetrics(final ProgramStatusEventDetails.Builder builder,
      Map<String, String> properties,
      ProgramRunStatus status, ProgramRunId runId) {
    if (properties.containsKey(ProgramOptionConstants.PROGRAM_ERROR)) {
      ProgramStatusEventDetails.Builder newBuilder =
          builder.withError(properties.get(ProgramOptionConstants.PROGRAM_ERROR));
      writeInEventWriters(newBuilder);
      return;
    }
    if (status.isEndState()) {
      CompletableFuture.supplyAsync(() -> {
            try {
              return metricsProvider.retrieveMetrics(runId);
            } catch (MetricRetrievalException e) {
              LOG.error("Error retrieving metrics from provider. ", e);
              return new ExecutionMetrics[]{};
            }
          })
          .thenAccept(metrics -> {
            ProgramStatusEventDetails.Builder newBuilder = builder.withPipelineMetrics(metrics);
            writeInEventWriters(newBuilder);
          });
    }
  }
}
