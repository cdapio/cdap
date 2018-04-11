/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Service that receives program status notifications and persists to the store
 */
public class ProgramNotificationSubscriberService extends AbstractNotificationSubscriberService {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);

  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  // These attributes are used to fetch the AppMetadataStore
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final byte[] APP_VERSION_UPGRADE_KEY = Bytes.toBytes("version.default.store");

  private final CConfiguration cConf;
  private final AtomicBoolean upgradeComplete;
  private final DatasetFramework datasetFramework;
  private final String recordedProgramStatusPublishTopic;

  @Inject
  ProgramNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                       DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                       MetricsCollectionService metricsCollectionService) {
    super("program.status", cConf, cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC), false,
          cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS),
          messagingService, datasetFramework, txClient, metricsCollectionService);
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.upgradeComplete = new AtomicBoolean(false);
    this.recordedProgramStatusPublishTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
  }

  @Nullable
  @Override
  protected String loadMessageId(DatasetContext datasetContext) throws Exception {
    return getAppMetadataStore(datasetContext).retrieveSubscriberState(getTopicId().getTopic());
  }

  @Override
  protected void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception {
    getAppMetadataStore(datasetContext).persistSubscriberState(getTopicId().getTopic(), messageId);
  }

  @Override
  protected void processMessages(DatasetContext datasetContext,
                                 Iterator<ImmutablePair<String, Notification>> messages)  throws Exception {
    AppMetadataStore appMetadataStore = getAppMetadataStore(datasetContext);
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      processNotification(appMetadataStore, messagePair.getFirst().getBytes(StandardCharsets.UTF_8),
                          messagePair.getSecond());
    }
  }

  private void processNotification(AppMetadataStore appMetadataStore,
                                   byte[] messageIdBytes, Notification notification) throws Exception {
    Map<String, String> properties = notification.getProperties();
    // Required parameters
    String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
    String programStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

    // Ignore notifications which specify an invalid ProgramRunId or ProgramRunStatus, which shouldn't happen
    if (programRun == null || programStatus == null) {
      LOG.warn("Ignore notification that misses program run state information, {}", notification);
      return;
    }

    ProgramRunStatus programRunStatus;
    try {
      programRunStatus = ProgramRunStatus.valueOf(programStatus);
    } catch (IllegalArgumentException e) {
      LOG.warn("Ignore notification with invalid program run status {} for program {}, {}",
               programStatus, programRun, notification);
      return;
    }

    ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);

    LOG.trace("Processing program status notification: {}", notification);
    String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
    long endTimeSecs = getTimeSeconds(notification.getProperties(), ProgramOptionConstants.END_TIME);

    ProgramRunStatus recordedStatus;
    switch(programRunStatus) {
      case STARTING:
        long startTimeSecs = getTimeSeconds(notification.getProperties(), ProgramOptionConstants.START_TIME);
        String userArgumentsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
        String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
        if (userArgumentsString == null || systemArgumentsString == null) {
          LOG.warn("Ignore program starting notification for program {} without {} arguments, {}",
                   programRunId, (userArgumentsString == null) ? "user" : "system", notification);
          return;
        }
        if (startTimeSecs == -1) {
          LOG.warn("Ignore program starting notification for program {} without start time, {}",
                   programRunId, notification);
          return;
        }
        String artifactIdString = notification.getProperties().get(ProgramOptionConstants.ARTIFACT_ID);
        ArtifactId artifactId = null;
        // can be null for notifications before upgrading that were not processed earlier
        if (artifactIdString != null) {
          artifactId = GSON.fromJson(artifactIdString, ArtifactId.class);
        }
        Map<String, String> userArguments = GSON.fromJson(userArgumentsString, STRING_STRING_MAP);
        Map<String, String> systemArguments = GSON.fromJson(systemArgumentsString, STRING_STRING_MAP);
        // TODO: CDAP-13096 move to provisioner status subscriber
        // for now, save these states here to follow correct lifecycle
        ProgramRunClusterStatus clusterStatus =
          appMetadataStore.recordProgramProvisioning(programRunId, startTimeSecs, userArguments, systemArguments,
                                                                           messageIdBytes, artifactId);
        appMetadataStore.recordProgramProvisioned(programRunId, 0, messageIdBytes);
        recordedStatus = appMetadataStore.recordProgramStart(programRunId, twillRunId,
                                                             systemArguments, messageIdBytes);
        if (recordedStatus == null && clusterStatus != null) {
          publishRecordedStatus(notification, programRunId, ProgramRunStatus.STARTING);
        }
        break;
      case RUNNING:
        long logicalStartTimeSecs = getTimeSeconds(notification.getProperties(),
                                                   ProgramOptionConstants.LOGICAL_START_TIME);
        if (logicalStartTimeSecs == -1) {
          LOG.warn("Ignore program running notification for program {} without {} specified, {}",
                   programRunId, ProgramOptionConstants.LOGICAL_START_TIME, notification);
          return;
        }
        recordedStatus =
          appMetadataStore.recordProgramRunning(programRunId, logicalStartTimeSecs, twillRunId, messageIdBytes);
        break;
      case SUSPENDED:
        long suspendTime = getTimeSeconds(notification.getProperties(),
                                                   ProgramOptionConstants.SUSPEND_TIME);
        // since we are adding suspend time recently, there might be old suspended notificications for which time
        // can be -1.
        recordedStatus = appMetadataStore.recordProgramSuspend(programRunId, messageIdBytes, suspendTime);
        break;
      case RESUMING:
        long resumeTime = getTimeSeconds(notification.getProperties(),
                                          ProgramOptionConstants.RESUME_TIME);
        // since we are adding suspend time recently, there might be old suspended notificications for which time
        // can be -1.
        recordedStatus = appMetadataStore.recordProgramResumed(programRunId, messageIdBytes, resumeTime);
        break;
      case COMPLETED:
      case KILLED:
        if (endTimeSecs == -1) {
          LOG.warn("Ignore program killed notification for program {} without end time specified, {}",
                   programRunId, notification);
          return;
        }
        recordedStatus =
          appMetadataStore.recordProgramStop(programRunId, endTimeSecs, programRunStatus, null, messageIdBytes);
        break;
      case FAILED:
        if (endTimeSecs == -1) {
          LOG.warn("Ignore program failed notification for program {} without end time specified, {}",
                   programRunId, notification);
          return;
        }
        BasicThrowable cause = decodeBasicThrowable(properties.get(ProgramOptionConstants.PROGRAM_ERROR));
        recordedStatus =
          appMetadataStore.recordProgramStop(programRunId, endTimeSecs, programRunStatus, cause, messageIdBytes);
        break;
      default:
        // This should not happen
        LOG.error("Unsupported program status {} for program {}, {}", programRunStatus, programRunId, notification);
        return;
    }
    if (recordedStatus != null) {
      publishRecordedStatus(notification, programRunId, recordedStatus);
    }
  }


  private void publishRecordedStatus(Notification notification,
                                     ProgramRunId programRunId, ProgramRunStatus status) throws Exception {
    Map<String, String> notificationProperties = new HashMap<>();
    notificationProperties.putAll(notification.getProperties());
    notificationProperties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
    notificationProperties.put(ProgramOptionConstants.PROGRAM_STATUS, status.name());
    Notification programStatusNotification =
      new Notification(Notification.Type.PROGRAM_STATUS, notificationProperties);
    getMessagingContext().getMessagePublisher().publish(NamespaceId.SYSTEM.getNamespace(),
                                                        recordedProgramStatusPublishTopic,
                                                        GSON.toJson(programStatusNotification));
  }

  /**
   * Helper method to extract the time from the given properties map, or return -1 if no value was found
   *
   * @param properties the properties map
   * @param option the key to lookup in the properties map
   * @return the time in seconds, or -1 if not found
   */
  private long getTimeSeconds(Map<String, String> properties, String option) {
    String timeString = properties.get(option);
    return (timeString == null) ? -1 : TimeUnit.MILLISECONDS.toSeconds(Long.valueOf(timeString));
  }

  /**
   * Decodes a {@link BasicThrowable} from a given json string.
   *
   * @param encoded the json representing of the {@link BasicThrowable}
   * @return the decode {@link BasicThrowable}; A {@code null} will be returned
   *         if the encoded string is {@code null} or on decode failure.
   */
  @Nullable
  private BasicThrowable decodeBasicThrowable(@Nullable String encoded) {
    try {
      return (encoded == null) ? null : GSON.fromJson(encoded, BasicThrowable.class);
    } catch (JsonSyntaxException e) {
      // This shouldn't happen normally, unless the BasicThrowable changed in an incompatible way
      return null;
    }
  }

  /**
   * Returns an instance of {@link AppMetadataStore}.
   */
  private AppMetadataStore getAppMetadataStore(DatasetContext context) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(context, datasetFramework, APP_META_INSTANCE_ID,
                                                    Table.class.getName(), DatasetProperties.EMPTY);
      AppMetadataStore appMetadataStore = new AppMetadataStore(table, cConf, upgradeComplete);
      // If upgrade was not complete, check if it is and update boolean
      if (!upgradeComplete.get()) {
        boolean isUpgradeComplete = appMetadataStore.isUpgradeComplete(APP_VERSION_UPGRADE_KEY);
        if (isUpgradeComplete) {
          upgradeComplete.set(true);
        }
      }
      return appMetadataStore;
    } catch (DatasetManagementException | IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
