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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A TMS subscriber service to replicate program status from the
 * {@link Constants.AppFabric#PROGRAM_STATUS_RECORD_EVENT_TOPIC} to a local storage.
 * It is for the {@link DirectRuntimeRequestValidator} to validate incoming requests that it is coming from
 * a running program.
 */
public class RuntimeProgramStatusSubscriberService extends AbstractNotificationSubscriberService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeProgramStatusSubscriberService.class);
  private static final Gson GSON = new Gson();

  @Inject
  RuntimeProgramStatusSubscriberService(CConfiguration cConf, MessagingService messagingService,
                                        MetricsCollectionService metricsCollectionService,
                                        TransactionRunner transactionRunner) {
    super("runtime.program.status", cConf,
          cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
          cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS),
          messagingService, metricsCollectionService, transactionRunner);
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws IOException {
    return getAppMetadataStore(context).retrieveSubscriberState(getTopicId().getTopic(), "runtime");
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId) throws IOException {
    getAppMetadataStore(context).persistSubscriberState(getTopicId().getTopic(), "runtime", messageId);
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
      processNotification(pair.getFirst().getBytes(StandardCharsets.UTF_8), notification, getAppMetadataStore(context));
    }
  }

  /**
   * Processes a given {@link Notification} from TMS and updates the {@link AppMetadataStore}.
   *
   * @param sourceId the message id in TMS
   * @param notification the {@link Notification} to process
   * @param store the {@link AppMetadataStore} to write to
   * @throws IOException if failed to write to the store
   */
  private void processNotification(byte[] sourceId, Notification notification,
                                   AppMetadataStore store) throws IOException {
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

    // Runtime server only needs the programRunId and status. We use the AppMetadataStore to record to help
    // handling state transition correctly, but we omit certain fields when calling those record* methods since
    // they are not used by the runtime server.
    LOG.debug("Received program {} of status {} {}", programRunId, programRunStatus, Bytes.toString(sourceId));
    switch (programRunStatus) {
      case STARTING: {
        ProgramOptions programOptions = ProgramOptions.fromNotification(notification, GSON);
        store.recordProgramProvisioning(programRunId, programOptions.getUserArguments().asMap(),
                                        programOptions.getArguments().asMap(), sourceId, null);
        store.recordProgramProvisioned(programRunId, 0, sourceId);
        store.recordProgramStart(programRunId, null, programOptions.getArguments().asMap(), sourceId);
        break;
      }
      case RUNNING:
        store.recordProgramRunning(programRunId,
                                   Optional.ofNullable(properties.get(ProgramOptionConstants.LOGICAL_START_TIME))
                                     .map(Long::parseLong).orElse(System.currentTimeMillis()),
                                   null, sourceId);
        break;
      case SUSPENDED:
        store.recordProgramSuspend(programRunId, sourceId,
                                   Optional.ofNullable(properties.get(ProgramOptionConstants.SUSPEND_TIME))
                                     .map(Long::parseLong).orElse(System.currentTimeMillis()));
        break;
      case RESUMING:
        store.recordProgramResumed(programRunId, sourceId,
                                   Optional.ofNullable(properties.get(ProgramOptionConstants.RESUME_TIME))
                                     .map(Long::parseLong).orElse(System.currentTimeMillis()));
        break;
      case COMPLETED:
      case FAILED:
      case KILLED:
        store.recordProgramStop(programRunId,
                                Optional.ofNullable(properties.get(ProgramOptionConstants.END_TIME))
                                  .map(Long::parseLong).orElse(System.currentTimeMillis()),
                                programRunStatus, null, sourceId);
        // We don't need to retain records for terminated programs, hence just delete it
        store.deleteRunIfTerminated(programRunId, sourceId);
        break;
      case REJECTED: {
        ProgramOptions programOptions = ProgramOptions.fromNotification(notification, GSON);
        ProgramDescriptor programDescriptor =
          GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);

        store.recordProgramRejected(programRunId, programOptions.getUserArguments().asMap(),
                                    programOptions.getArguments().asMap(), sourceId,
                                    programDescriptor.getArtifactId().toApiArtifactId());
        // We don't need to retain records for terminated programs, hence just delete it
        store.deleteRunIfTerminated(programRunId,  sourceId);
        break;
      }
    }
  }

  /**
   * Returns an instance of {@link AppMetadataStore}.
   */
  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }
}
