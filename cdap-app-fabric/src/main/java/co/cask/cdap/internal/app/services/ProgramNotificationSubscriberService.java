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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.provision.ProvisionRequest;
import co.cask.cdap.internal.provision.ProvisionerNotifier;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Service that receives program status notifications and persists to the store
 */
public class ProgramNotificationSubscriberService extends AbstractNotificationSubscriberService {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private final String recordedProgramStatusPublishTopic;
  private final ProvisionerNotifier provisionerNotifier;
  private final ProgramLifecycleService programLifecycleService;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final Queue<Runnable> tasks;
  private ExecutorService taskExecutor;

  @Inject
  ProgramNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                       DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                       MetricsCollectionService metricsCollectionService,
                                       ProvisionerNotifier provisionerNotifier,
                                       ProgramLifecycleService programLifecycleService,
                                       ProvisioningService provisioningService,
                                       ProgramStateWriter programStateWriter) {
    super("program.status", cConf, cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC), false,
          cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS),
          messagingService, datasetFramework, txClient, metricsCollectionService);
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.recordedProgramStatusPublishTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    this.provisionerNotifier = provisionerNotifier;
    this.programLifecycleService = programLifecycleService;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.tasks = new LinkedList<>();
  }

  @Override
  protected void doStartUp() {
    taskExecutor = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("program-execution-%d"));
  }

  @Override
  protected void doShutdown() {
    taskExecutor.shutdownNow();
    try {
      // Wait for it to shutdown for short while. It's ok if the task executor is not completely shutdown as
      // tasks should have their own states maintained in persisted store. Also, threads created are daemon threads
      // hence won't block the process exit.
      taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignore interrupt.
    }
  }

  @Nullable
  @Override
  protected String loadMessageId(DatasetContext datasetContext) throws Exception {
    return getAppMetadataStore(datasetContext).retrieveSubscriberState(getTopicId().getTopic(), "");
  }

  @Override
  protected void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception {
    getAppMetadataStore(datasetContext).persistSubscriberState(getTopicId().getTopic(), "", messageId);
  }

  @Override
  protected void processMessages(DatasetContext datasetContext,
                                 Iterator<ImmutablePair<String, Notification>> messages) throws Exception {
    AppMetadataStore appMetadataStore = getAppMetadataStore(datasetContext);
    List<Runnable> tasks = new LinkedList<>();
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      Optional<Runnable> task = processNotification(datasetContext, appMetadataStore,
                                                    messagePair.getFirst().getBytes(StandardCharsets.UTF_8),
                                                    messagePair.getSecond());
      task.ifPresent(tasks::add);
    }

    // Only add post processing tasks if all messages are processed. If there is exception in the processNotifiation,
    // messages will be replayed.
    this.tasks.addAll(tasks);
  }

  @Override
  protected void postProcess() {
    Runnable task = tasks.poll();
    while (task != null) {
      taskExecutor.execute(task);
      task = tasks.poll();
    }
  }

  /**
   * Process a {@link Notification} received from TMS.
   *
   * @param datasetContext the {@link DatasetContext} for getting access to dataset instances
   * @param appMetadataStore the {@link AppMetadataStore} for updating app metadata
   * @param messageIdBytes the raw message id in the TMS for the notification
   * @param notification the {@link Notification} to process
   * @return an {@link Optional} {@link Runnable} task to run after the transactional processing of the whole
   *         messages batch is completed
   * @throws Exception if failed to process the given notification
   */
  private Optional<Runnable> processNotification(DatasetContext datasetContext, AppMetadataStore appMetadataStore,
                                                 byte[] messageIdBytes, Notification notification) throws Exception {
    Map<String, String> properties = notification.getProperties();
    // Required parameters
    String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
    String programStatusStr = properties.get(ProgramOptionConstants.PROGRAM_STATUS);
    String clusterStatusStr = properties.get(ProgramOptionConstants.CLUSTER_STATUS);

    // Ignore notifications which specify an invalid ProgramRunId, which shouldn't happen
    if (programRun == null) {
      LOG.warn("Ignore notification that misses program run state information, {}", notification);
      return Optional.empty();
    }
    ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);

    ProgramRunStatus programRunStatus = null;
    if (programStatusStr != null) {
      try {
        programRunStatus = ProgramRunStatus.valueOf(programStatusStr);
      } catch (IllegalArgumentException e) {
        LOG.warn("Ignore notification with invalid program run status {} for program {}, {}",
                 programStatusStr, programRun, notification);
        return Optional.empty();
      }
    }

    ProgramRunClusterStatus clusterStatus = null;
    if (clusterStatusStr != null) {
      try {
        clusterStatus = ProgramRunClusterStatus.valueOf(clusterStatusStr);
      } catch (IllegalArgumentException e) {
        LOG.warn("Ignore notification with invalid program run cluster status {} for program {}",
                 clusterStatusStr, programRun);
        return Optional.empty();
      }
    }

    if (programRunStatus != null) {
      handleProgramEvent(programRunId, programRunStatus, notification, messageIdBytes,
                         appMetadataStore);
    }

    if (clusterStatus == null) {
      return Optional.empty();
    }

    return handleClusterEvent(programRunId, clusterStatus, notification,
                              messageIdBytes, datasetContext, appMetadataStore);
  }

  private void handleProgramEvent(ProgramRunId programRunId, ProgramRunStatus programRunStatus,
                                  Notification notification, byte[] messageIdBytes,
                                  AppMetadataStore appMetadataStore) throws Exception {
    LOG.trace("Processing program status notification: {}", notification);
    Map<String, String> properties = notification.getProperties();
    String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
    long endTimeSecs = getTimeSeconds(notification.getProperties(), ProgramOptionConstants.END_TIME);

    RunRecordMeta recordedRunRecord;
    switch (programRunStatus) {
      case STARTING:
        String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
        Map<String, String> systemArguments = systemArgumentsString == null ?
          Collections.emptyMap() : GSON.fromJson(systemArgumentsString, STRING_STRING_MAP);
        boolean isInWorkflow = systemArguments.containsKey(ProgramOptionConstants.WORKFLOW_NAME);
        boolean skipProvisioning = Boolean.parseBoolean(systemArguments.get(ProgramOptionConstants.SKIP_PROVISIONING));
        // if this is a preview run or a program within a workflow, we don't actually need to provision a cluster
        // instead, we skip forward past the provisioning and provisioned states and go straight to starting.
        if (isInWorkflow || skipProvisioning) {
          ProgramOptions programOptions = createProgramOptions(programRunId.getParent(), properties);
          ProgramDescriptor programDescriptor =
            GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);
          appMetadataStore.recordProgramProvisioning(programRunId, programOptions.getUserArguments().asMap(),
                                                     programOptions.getArguments().asMap(), messageIdBytes,
                                                     programDescriptor.getArtifactId().toApiArtifactId());
          appMetadataStore.recordProgramProvisioned(programRunId, 0, messageIdBytes);
        }
        recordedRunRecord = appMetadataStore.recordProgramStart(programRunId, twillRunId,
                                                                systemArguments, messageIdBytes);
        break;
      case RUNNING:
        long logicalStartTimeSecs = getTimeSeconds(notification.getProperties(),
                                                   ProgramOptionConstants.LOGICAL_START_TIME);
        if (logicalStartTimeSecs == -1) {
          LOG.warn("Ignore program running notification for program {} without {} specified, {}",
                   programRunId, ProgramOptionConstants.LOGICAL_START_TIME, notification);
          return;
        }
        recordedRunRecord =
          appMetadataStore.recordProgramRunning(programRunId, logicalStartTimeSecs, twillRunId, messageIdBytes);
        break;
      case SUSPENDED:
        recordedRunRecord = appMetadataStore.recordProgramSuspend(programRunId, messageIdBytes);
        break;
      case RESUMING:
        recordedRunRecord = appMetadataStore.recordProgramResumed(programRunId, messageIdBytes);
        break;
      case COMPLETED:
      case KILLED:
        if (endTimeSecs == -1) {
          LOG.warn("Ignore program killed notification for program {} without end time specified, {}",
                   programRunId, notification);
          return;
        }
        recordedRunRecord =
          appMetadataStore.recordProgramStop(programRunId, endTimeSecs, programRunStatus, null, messageIdBytes);
        break;
      case FAILED:
        if (endTimeSecs == -1) {
          LOG.warn("Ignore program failed notification for program {} without end time specified, {}",
                   programRunId, notification);
          return;
        }
        BasicThrowable cause = decodeBasicThrowable(properties.get(ProgramOptionConstants.PROGRAM_ERROR));
        recordedRunRecord =
          appMetadataStore.recordProgramStop(programRunId, endTimeSecs, programRunStatus, cause, messageIdBytes);
        break;
      default:
        // This should not happen
        LOG.error("Unsupported program status {} for program {}, {}", programRunStatus, programRunId, notification);
        return;
    }
    if (recordedRunRecord != null) {
      publishRecordedStatus(notification, programRunId, recordedRunRecord.getStatus());
      if (programRunStatus.isEndState()) {
        // if this is a preview run or a program within a workflow, we don't actually need to de-provision the cluster.
        // instead, we just record the state as deprovisioned without notifying the provisioner
        boolean isInWorkflow = recordedRunRecord.getSystemArgs().containsKey(ProgramOptionConstants.WORKFLOW_NAME);
        boolean skipProvisioning =
          Boolean.parseBoolean(recordedRunRecord.getSystemArgs().get(ProgramOptionConstants.SKIP_PROVISIONING));
        if (isInWorkflow || skipProvisioning) {
          appMetadataStore.recordProgramDeprovisioning(programRunId, messageIdBytes);
          appMetadataStore.recordProgramDeprovisioned(programRunId, messageIdBytes);
        } else {
          // TODO: CDAP-13295 remove once runtime monitor emits this message
          provisionerNotifier.deprovisioning(programRunId);
        }
      }
    }
  }

  private Optional<Runnable> handleClusterEvent(ProgramRunId programRunId, ProgramRunClusterStatus clusterStatus,
                                                Notification notification, byte[] messageIdBytes,
                                                DatasetContext datasetContext, AppMetadataStore appMetadataStore) {
    Map<String, String> properties = notification.getProperties();

    ProgramOptions programOptions = createProgramOptions(programRunId.getParent(), properties);
    String userId = properties.get(ProgramOptionConstants.USER_ID);

    ProgramDescriptor programDescriptor =
      GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);
    switch (clusterStatus) {
      case PROVISIONING:
        appMetadataStore.recordProgramProvisioning(programRunId, programOptions.getUserArguments().asMap(),
                                                   programOptions.getArguments().asMap(), messageIdBytes,
                                                   programDescriptor.getArtifactId().toApiArtifactId());

        ProvisionRequest provisionRequest = new ProvisionRequest(programRunId, programOptions, programDescriptor,
                                                                 userId);
        return Optional.of(provisioningService.provision(provisionRequest, datasetContext));
      case PROVISIONED:
        Cluster cluster = GSON.fromJson(properties.get(ProgramOptionConstants.CLUSTER), Cluster.class);
        appMetadataStore.recordProgramProvisioned(programRunId, cluster.getNodes().size(), messageIdBytes);

        // Update the ProgramOptions system arguments to include the cluster information
        Map<String, String> systemArgs = new HashMap<>(programOptions.getArguments().asMap());
        systemArgs.put(ProgramOptionConstants.CLUSTER, properties.get(ProgramOptionConstants.CLUSTER));
        if (properties.containsKey(ProgramOptionConstants.CLUSTER_KEY_INFO)) {
          systemArgs.put(ProgramOptionConstants.CLUSTER_KEY_INFO,
                         properties.get(ProgramOptionConstants.CLUSTER_KEY_INFO));
        }
        ProgramOptions newProgramOptions = new SimpleProgramOptions(programOptions.getProgramId(),
                                                                    new BasicArguments(systemArgs),
                                                                    programOptions.getUserArguments());

        // Publish the program STARTING state before starting the program
        programStateWriter.start(programRunId, newProgramOptions, null, programDescriptor);

        // start the program run
        return Optional.of(() -> {
          String oldUser = SecurityRequestContext.getUserId();
          try {
            SecurityRequestContext.setUserId(userId);
            try {
              programLifecycleService.startInternal(programDescriptor, newProgramOptions, programRunId);
            } catch (Exception e) {
              programStateWriter.error(programRunId, e);
            }
          } finally {
            SecurityRequestContext.setUserId(oldUser);
          }
        });
      case DEPROVISIONING:
        appMetadataStore.recordProgramDeprovisioning(programRunId, messageIdBytes);
        return Optional.of(provisioningService.deprovision(programRunId, datasetContext));
      case DEPROVISIONED:
        appMetadataStore.recordProgramDeprovisioned(programRunId, messageIdBytes);
        break;
    }

    return Optional.empty();
  }

  private ProgramOptions createProgramOptions(ProgramId programId, Map<String, String> properties) {
    String userArgumentsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
    String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
    String debugString = properties.get(ProgramOptionConstants.DEBUG_ENABLED);

    Boolean debug = Boolean.valueOf(debugString);
    Map<String, String> userArguments = userArgumentsString == null ?
      Collections.emptyMap() : GSON.fromJson(userArgumentsString, STRING_STRING_MAP);
    Map<String, String> systemArguments = systemArgumentsString == null ?
      Collections.emptyMap() : GSON.fromJson(systemArgumentsString, STRING_STRING_MAP);

    return new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                    new BasicArguments(userArguments), debug);
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
    return AppMetadataStore.create(cConf, context, datasetFramework);
  }
}
