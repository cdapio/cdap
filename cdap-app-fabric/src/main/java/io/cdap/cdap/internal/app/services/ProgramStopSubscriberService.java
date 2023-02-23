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

package io.cdap.cdap.internal.app.services;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Subscribes to the Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC topic, which gets published to
 * after the state store has been updated. Looks for stopping messages and performs the actual program stop.
 */
public class ProgramStopSubscriberService extends AbstractNotificationSubscriberService {
  private static final String SUBSCRIBER = "program.stopper";
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStopSubscriberService.class);
  private static final Gson GSON = new Gson();
  private final ProgramRuntimeService programRuntimeService;
  private final ProgramStateWriter programStateWriter;

  @Inject
  ProgramStopSubscriberService(CConfiguration cConf, MessagingService messagingService,
                               MetricsCollectionService metricsCollectionService,
                               TransactionRunner transactionRunner,
                               ProgramRuntimeService programRuntimeService,
                               ProgramStateWriter programStateWriter) {
    super("program.stop.service", cConf,
          cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
          cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS),
          messagingService, metricsCollectionService, transactionRunner);
    this.programRuntimeService = programRuntimeService;
    this.programStateWriter = programStateWriter;
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws IOException {
    return AppMetadataStore.create(context).retrieveSubscriberState(getTopicId().getTopic(), SUBSCRIBER);
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws IOException {
    AppMetadataStore.create(context).persistSubscriberState(getTopicId().getTopic(), SUBSCRIBER, messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext context,
                                 Iterator<ImmutablePair<String, Notification>> messages) throws Exception {
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> pair = messages.next();
      Notification notification = pair.getSecond();
      if (notification.getNotificationType() != Notification.Type.PROGRAM_STATUS) {
        continue;
      }
      processNotification(notification, context);
    }
  }

  /**
   * Processes a given {@link Notification} from TMS and updates the {@link AppMetadataStore}.
   *
   * @param notification the {@link Notification} to process
   * @param context the table context for accessing the store
   * @throws IOException if failed to write to the store
   */
  private void processNotification(Notification notification, StructuredTableContext context) throws IOException {
    Map<String, String> properties = notification.getProperties();

    ProgramRunId programRunId;
    ProgramRunStatus programRunStatus;
    try {
      programRunId = GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_RUN_ID),
                                   ProgramRunId.class);
      if (programRunId == null) {
        throw new IllegalArgumentException("Missing program run id from notification");
      }
      programRunStatus = ProgramRunStatus.valueOf(properties.get(ProgramOptionConstants.PROGRAM_STATUS));
    } catch (Exception e) {
      // This shouldn't happen. If it does, we can only log and ignore the event.
      LOG.warn("Ignore notification due to unable to get program run id and program run status from notification {}",
               notification, e);
      return;
    }

    if (programRunStatus != ProgramRunStatus.STOPPING) {
      return;
    }

    AppMetadataStore store = AppMetadataStore.create(context);
    RunRecordDetail runRecord = store.getRun(programRunId);
    if (runRecord == null) {
      // shouldn't happen, but nothing to do but move on
      LOG.warn("Ignoring stop message for application {} program {} run {} because the run record could not be found.",
               programRunId.getApplication(), programRunId.getProgram(), programRunId.getRun());
      return;
    }

    ProgramRunStatus programStatus = runRecord.getStatus();
    if (programStatus.isEndState()) {
      // if the program run completed already ignore the stop request
      // can happen in race conditions where a stop is issued, but the program ends on its own before
      // the stop message here is processed.
      LOG.debug("Ignoring stop message for application {} program {} run {} because it is already in an end state.",
                programRunId.getApplication(), programRunId.getProgram(), programRunId.getRun());
      return;
    }

    // otherwise, look for the controller in the ProgramRuntimeService
    ProgramRuntimeService.RuntimeInfo runtimeInfo =
      programRuntimeService.lookup(programRunId.getParent(), RunIds.fromString(programRunId.getRun()));
    // once the program reaches the starting state, runtimeInfo will be non-null unless the program completed
    // already but the run state didn't get updated yet
    // it can also be null if the stop was issued when the program was in the pending state, in which case it
    // was never started
    // in both situations, there isn't any program to actual stop, and the right thing to do is
    // transition the program to the killed state
    if (runtimeInfo == null) {
      LOG.info("Could not find a program controller for application {} program {} run {}. " +
                 "Writing a message that the program state should be killed.",
               programRunId.getApplication(), programRunId.getProgram(), programRunId.getRun());
      programStateWriter.killed(programRunId);
      return;
    }

    // otherwise, use the controller and try to stop the program run gracefully
    LOG.info("Stopping application {} program {} run {}", programRunId.getApplication(), programRunId.getProgram(),
             programRunId.getRun());
    runtimeInfo.getController().stop();
  }
}
