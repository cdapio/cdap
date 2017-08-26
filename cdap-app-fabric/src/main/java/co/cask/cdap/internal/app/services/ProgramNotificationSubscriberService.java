/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.ApplicationMeta;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Service that receives program status notifications and persists to the store
 */
public class ProgramNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));

  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  // These attributes are used to fetch the AppMetadataStore
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final byte[] APP_VERSION_UPGRADE_KEY = Bytes.toBytes("version.default.store");

  private final CConfiguration cConf;
  private final AtomicBoolean upgradeComplete;
  private final DatasetFramework datasetFramework;

  private ExecutorService taskExecutorService;

  @Inject
  ProgramNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                       DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                       MetricsCollectionService metricsCollectionService) {
    super(messagingService, cConf, datasetFramework, txClient, metricsCollectionService);
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.upgradeComplete = new AtomicBoolean(false);
  }

  @Override
  protected void startUp() {
    LOG.info("Starting {}", getClass().getSimpleName());
    taskExecutorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                              .setNameFormat("program-status-subscriber-task")
                                                              .build());
    taskExecutorService.submit(new ProgramStatusSubscriberRunnable(
      cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)));
  }

  @Override
  protected void shutDown() {
    super.shutDown();
    try {
      // Shutdown the executor, which will issue an interrupt to the running thread.
      taskExecutorService.shutdownNow();
      taskExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // Ignore it.
    } finally {
      if (!taskExecutorService.isTerminated()) {
        taskExecutorService.shutdownNow();
      }
    }
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Thread that receives TMS notifications and persists the program status notification to the store
   */
  private class ProgramStatusSubscriberRunnable extends AbstractSubscriberRunnable {
    private final String topic;

    ProgramStatusSubscriberRunnable(String topic) {
      // Fetching of program status events are non-transactional
      super("program.status", topic, cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS),
            cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE), false);
      this.topic = topic;
    }

    @Nullable
    @Override
    protected String initialize(DatasetContext context) throws RetryableException {
      return getAppMetadataStore(context).retrieveSubscriberState(topic);
    }

    @Override
    public void persistMessageId(DatasetContext context, String lastFetchedMessageId) {
      getAppMetadataStore(context).persistSubscriberState(topic, lastFetchedMessageId);
    }

    @Override
    protected void processNotifications(DatasetContext context,
                                        AbstractNotificationSubscriberService.NotificationIterator notifications) {
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);
      while (notifications.hasNext()) {
        processNotification(appMetadataStore, notifications.next(), notifications.getLastMessageId().getBytes());
      }
    }

    private void processNotification(AppMetadataStore appMetadataStore, Notification notification,
                                     byte[] messageIdBytes) {
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
      ProgramId programId = programRunId.getParent();

      ApplicationMeta meta = appMetadataStore.getApplication(programRunId.getNamespace(),
                                                             programRunId.getApplication(),
                                                             programRunId.getVersion());
      // Check if the application exists
      if (meta == null) {
        LOG.warn("Ignore notification for application {} that doesn't exist, {}", programRunId, notification);
        return;
      }
      // Check if the program exists
      if (getProgramSpecFromApp(meta.getSpec(), programRunId) == null) {
        LOG.warn("Ignore notification for program {} that doesn't exist, {}", programRunId, notification);
        return;
      }

      String runId = programRunId.getRun();
      String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
      long endTimeSecs = getTimeSeconds(notification.getProperties(), ProgramOptionConstants.END_TIME);

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
          Map<String, String> userArguments = GSON.fromJson(userArgumentsString, STRING_STRING_MAP);
          Map<String, String> systemArguments = GSON.fromJson(systemArgumentsString, STRING_STRING_MAP);
          appMetadataStore.recordProgramStart(programId, runId, startTimeSecs, twillRunId,
                                              userArguments, systemArguments, messageIdBytes);
          return;
        case RUNNING:
          long logicalStartTimeSecs = getTimeSeconds(notification.getProperties(),
                                                     ProgramOptionConstants.LOGICAL_START_TIME);
          if (logicalStartTimeSecs == -1) {
            LOG.warn("Ignore program running notification for program {} without {} specified, {}",
                     programRunId, ProgramOptionConstants.LOGICAL_START_TIME, notification);
            return;
          }
          appMetadataStore.recordProgramRunning(programId, runId, logicalStartTimeSecs, twillRunId, messageIdBytes);
          return;
        case SUSPENDED:
          appMetadataStore.recordProgramSuspend(programId, runId, messageIdBytes);
          return;
        case RESUMING:
          appMetadataStore.recordProgramResumed(programId, runId, messageIdBytes);
          return;
        case COMPLETED:
        case KILLED:
          if (endTimeSecs == -1) {
            LOG.warn("Ignore program killed notification for program {} without end time specified, {}",
                     programRunId, notification);
            return;
          }
          appMetadataStore.recordProgramStop(programId, runId, endTimeSecs, programRunStatus, null, messageIdBytes);
          return;
        case FAILED:
          if (endTimeSecs == -1) {
            LOG.warn("Ignore program failed notification for program {} without end time specified, {}",
                     programRunId, notification);
            return;
          }
          BasicThrowable cause = decodeBasicThrowable(properties.get(ProgramOptionConstants.PROGRAM_ERROR));
          appMetadataStore.recordProgramStop(programId, runId, endTimeSecs, programRunStatus, cause, messageIdBytes);
          return;
        default:
          // This should not happen
          LOG.error("Unsupported program status {} for program {}, {}", programRunStatus, programRunId, notification);
      }
    }

    @Nullable
    private ProgramSpecification getProgramSpecFromApp(ApplicationSpecification appSpec, ProgramRunId programRunId) {
      String programName = programRunId.getProgram();
      ProgramType type = programRunId.getType();
      if (type == ProgramType.FLOW && appSpec.getFlows().containsKey(programName)) {
        return appSpec.getFlows().get(programName);
      }
      if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(programName)) {
        return appSpec.getMapReduce().get(programName);
      }
      if (type == ProgramType.SPARK && appSpec.getSpark().containsKey(programName)) {
        return appSpec.getSpark().get(programName);
      }
      if (type == ProgramType.WORKFLOW && appSpec.getWorkflows().containsKey(programName)) {
        return appSpec.getWorkflows().get(programName);
      }
      if (type == ProgramType.SERVICE && appSpec.getServices().containsKey(programName)) {
        return appSpec.getServices().get(programName);
      }
      if (type == ProgramType.WORKER && appSpec.getWorkers().containsKey(programName)) {
        return appSpec.getWorkers().get(programName);
      }
      return null;
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
      // TODO Find a way to access the appMetadataStore without copying code from the DefaultStore
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
}
