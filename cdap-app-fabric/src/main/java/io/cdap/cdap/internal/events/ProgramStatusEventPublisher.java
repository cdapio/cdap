/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.events.EventWriter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link EventPublisher} implementation for program status
 */
public class ProgramStatusEventPublisher extends AbstractNotificationSubscriberService
  implements EventPublisher<ProgramStatusEvent> {

  private static final String SUBSCRIBER_NAME = "program_status_event_publisher";
  private static final Gson GSON = new Gson();
  private static final String EVENT_VERSION = "v1";

  private Collection<EventWriter<ProgramStatusEvent>> eventWriters;
  private String instanceName;
  private String projectName;
  private CConfiguration cConf;

  @Inject
  protected ProgramStatusEventPublisher(String name, CConfiguration cConf,
                                        MessagingService messagingService,
                                        MetricsCollectionService metricsCollectionService,
                                        TransactionRunner transactionRunner) {
    super(name, cConf,
            cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
            cConf.getInt(Constants.Event.PROGRAM_STATUS_FETCH_SIZE),
          cConf.getInt(Constants.Event.PROGRAM_STATUS_POLL_INTERVAL_SECONDS), messagingService,
          metricsCollectionService,
          transactionRunner);
    this.cConf = cConf;
    this.instanceName = cConf.get(Constants.Event.INSTANCE_NAME);
    this.projectName = cConf.get(Constants.Event.PROJECT_NAME);
  }

  @Override
  public void initialize(Collection<EventWriter<ProgramStatusEvent>> eventWriters) {
    this.eventWriters = eventWriters;
    this.eventWriters.forEach(eventWriter -> {
      DefaultEventWriterContext eventWriterContext = new DefaultEventWriterContext(cConf, eventWriter.getID());
      System.out.println("Initalizing EventWriter in Publisher...");
      eventWriter.initialize(eventWriterContext);
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
  public String getEventWriterPath() {
    return null;
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws Exception {
    return AppMetadataStore.create(context).retrieveSubscriberState(getTopicId().getTopic(), SUBSCRIBER_NAME);
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
    AppMetadataStore.create(context).persistSubscriberState(getTopicId().getTopic(), SUBSCRIBER_NAME, messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, Notification>> messages) {
    List<ProgramStatusEvent> programStatusEvents = new ArrayList<>();
    long publishTime = System.currentTimeMillis();
    messages.forEachRemaining(message -> {
      Notification notification = message.getSecond();
      System.out.println("Notification type of the message: " + notification.getNotificationType().name());
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
      System.out.println("Status of the event: " + programRunStatus.name());
      //Should event publish happen for this status
      if (!shouldPublish(programRunStatus)) {
        return;
      }
      String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);
      System.out.println("Retrieving event: " + programRunId.getRun());
      ProgramStatusEventDetails.Builder builder = ProgramStatusEventDetails
        .getBuilder(programRunId.getRun(), programRunId.getProgram(), programRunId.getNamespace(), programStatus,
                    RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS));
      String userArgsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
      String sysArgsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      Type argsMapType = new TypeToken<Map<String, String>>() {
      }.getType();
      ProgramStatusEventDetails programStatusEventDetails = builder
        .withUserArgs(GSON.fromJson(userArgsString, argsMapType))
        .withSystemArgs(GSON.fromJson(sysArgsString, argsMapType)).build();
      ProgramStatusEvent programStatusEvent = new ProgramStatusEvent(publishTime, EVENT_VERSION, instanceName,
                                                                     projectName, programStatusEventDetails);
      programStatusEvents.add(programStatusEvent);
    });

    this.eventWriters.stream().forEach(eventWriter -> eventWriter.write(programStatusEvents));
  }

  private boolean shouldPublish(ProgramRunStatus programRunStatus) {
    return (programRunStatus == ProgramRunStatus.STARTING) || programRunStatus.isEndState();
  }
}
