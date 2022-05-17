/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.app.store.RunRecordDetailWithExistingStatus;
import io.cdap.cdap.internal.provision.ProvisionRequest;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.reporting.ProgramHeartbeatTable;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Service that receives program status notifications and persists to the store.
 * No transactions should be started in any of the overrided methods since they are already wrapped in a transaction.
 */
public class ProgramNotificationSubscriberService extends AbstractNotificationSubscriberService {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final String CDAP_VERSION = "cdap.version";
  private static final Map<ProgramRunStatus, String> STATUS_METRICS_NAME = ImmutableMap.of(
    ProgramRunStatus.COMPLETED, Constants.Metrics.Program.PROGRAM_COMPLETED_RUNS,
    ProgramRunStatus.KILLED, Constants.Metrics.Program.PROGRAM_KILLED_RUNS,
    ProgramRunStatus.FAILED, Constants.Metrics.Program.PROGRAM_FAILED_RUNS,
    ProgramRunStatus.REJECTED, Constants.Metrics.Program.PROGRAM_REJECTED_RUNS
  );
  private static final Map<SchedulableProgramType, ProgramType> WORKFLOW_INNER_PROGRAM_TYPES = ImmutableMap.of(
    SchedulableProgramType.MAPREDUCE, ProgramType.MAPREDUCE,
    SchedulableProgramType.SPARK, ProgramType.SPARK
  );

  private final String recordedProgramStatusPublishTopic;
  private final ProvisionerNotifier provisionerNotifier;
  private final ProgramLifecycleService programLifecycleService;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final Queue<Runnable> tasks;
  private final MetricsCollectionService metricsCollectionService;
  private Set<ProgramCompletionNotifier> programCompletionNotifiers;
  private final CConfiguration cConf;
  private final Store store;
  private final RunRecordMonitorService runRecordMonitorService;

  @Inject
  ProgramNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                       MetricsCollectionService metricsCollectionService,
                                       ProvisionerNotifier provisionerNotifier,
                                       ProgramLifecycleService programLifecycleService,
                                       ProvisioningService provisioningService,
                                       ProgramStateWriter programStateWriter, TransactionRunner transactionRunner,
                                       Store store,
                                       RunRecordMonitorService runRecordMonitorService) {
    super("program.status", cConf, cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC),
          cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS),
          messagingService, metricsCollectionService, transactionRunner);
    this.recordedProgramStatusPublishTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    this.provisionerNotifier = provisionerNotifier;
    this.programLifecycleService = programLifecycleService;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.tasks = new LinkedList<>();
    this.metricsCollectionService = metricsCollectionService;
    this.programCompletionNotifiers = Collections.emptySet();
    this.runRecordMonitorService = runRecordMonitorService;
    this.cConf = cConf;
    this.store = store;
  }

  @Override
  protected void doStartUp() throws Exception {
    super.doStartUp();

    int batchSize = cConf.getInt(Constants.RuntimeMonitor.INIT_BATCH_SIZE);
    RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf,
                                                                    Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX);
    long startTs = System.currentTimeMillis();

    Retries.runWithRetries(() -> store.scanActiveRuns(batchSize, (runRecordDetail) -> {
      if (runRecordDetail.getStartTs() > startTs) {
        return;
      }
      try {
        if (runRecordDetail.getStatus() == ProgramRunStatus.PENDING) {
          runRecordMonitorService.addRequest(runRecordDetail.getProgramRunId());
        } else if (runRecordDetail.getStatus() == ProgramRunStatus.STARTING) {
          runRecordMonitorService.addRequest(runRecordDetail.getProgramRunId());
          // It is unknown what is the state of program runs in STARTING state.
          // A STARTING message is published again to retry STARTING logic.
          ProgramOptions programOptions =
            new SimpleProgramOptions(runRecordDetail.getProgramRunId().getParent(),
                                     new BasicArguments(runRecordDetail.getSystemArgs()),
                                     new BasicArguments(runRecordDetail.getUserArgs()));
          LOG.debug("Retrying to start run {}.", runRecordDetail.getProgramRunId());
          programStateWriter.start(runRecordDetail.getProgramRunId(),
                                   programOptions,
                                   null,
                                   this.store.loadProgram(runRecordDetail.getProgramRunId().getParent()));
        }
      } catch (Exception e) {
        ProgramRunId programRunId = runRecordDetail.getProgramRunId();
        LOG.warn("Retrying to start run {} failed. Marking it as failed.", programRunId, e);
        programStateWriter.error(programRunId, e);
      }
    }), retryStrategy, e -> true);
  }

  @Inject(optional = true)
  void setProgramCompletionNotifiers(Set<ProgramCompletionNotifier> notifiers) {
    this.programCompletionNotifiers = notifiers;
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context) throws IOException, TableNotFoundException {
    return getAppMetadataStore(context).retrieveSubscriberState(getTopicId().getTopic(), "");
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId)
    throws IOException, TableNotFoundException {
    getAppMetadataStore(context).persistSubscriberState(getTopicId().getTopic(), "", messageId);
  }

  @Override
  protected void processMessages(StructuredTableContext structuredTableContext,
                                 Iterator<ImmutablePair<String, Notification>> messages) throws Exception {
    ProgramHeartbeatTable heartbeatDataset = new ProgramHeartbeatTable(structuredTableContext);
    List<Runnable> tasks = new LinkedList<>();
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      List<Runnable> runnables = processNotification(heartbeatDataset,
                                                     messagePair.getFirst().getBytes(StandardCharsets.UTF_8),
                                                     messagePair.getSecond(), structuredTableContext);
      tasks.addAll(runnables);
    }

    // Only add post processing tasks if all messages are processed. If there is exception in the processNotifiation,
    // messages will be replayed.
    this.tasks.addAll(tasks);
  }

  @Override
  protected void postProcess() {
    Runnable task = tasks.poll();
    while (task != null) {
      task.run();
      task = tasks.poll();
    }
  }

  /**
   * Process a {@link Notification} received from TMS.
   *
   * @param programHeartbeatTable the {@link ProgramHeartbeatTable} for writing heart beats and program status
   * @param messageIdBytes the raw message id in the TMS for the notification
   * @param notification the {@link Notification} to process
   * @param context context to get the table for operations
   * @return a {@link List} of {@link Runnable} tasks to run after the transactional processing of the whole
   *         messages batch is completed
   * @throws Exception if failed to process the given notification
   */
  private List<Runnable> processNotification(ProgramHeartbeatTable programHeartbeatTable,
                                             byte[] messageIdBytes, Notification notification,
                                             StructuredTableContext context) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    Map<String, String> properties = notification.getProperties();
    // Required parameters
    String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
    String programStatusStr = properties.get(ProgramOptionConstants.PROGRAM_STATUS);
    String clusterStatusStr = properties.get(ProgramOptionConstants.CLUSTER_STATUS);

    // Ignore notifications which specify an invalid ProgramRunId, which shouldn't happen
    if (programRun == null) {
      LOG.warn("Ignore notification that misses program run state information, {}", notification);
      return Collections.emptyList();
    }
    ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);

    ProgramRunStatus programRunStatus = null;
    if (programStatusStr != null) {
      try {
        programRunStatus = ProgramRunStatus.valueOf(programStatusStr);
      } catch (IllegalArgumentException e) {
        LOG.warn("Ignore notification with invalid program run status {} for program {}, {}",
                 programStatusStr, programRun, notification);
        return Collections.emptyList();
      }
    }

    ProgramRunClusterStatus clusterStatus = null;
    if (clusterStatusStr != null) {
      try {
        clusterStatus = ProgramRunClusterStatus.valueOf(clusterStatusStr);
      } catch (IllegalArgumentException e) {
        LOG.warn("Ignore notification with invalid program run cluster status {} for program {}",
                 clusterStatusStr, programRun);
        return Collections.emptyList();
      }
    }
    if (notification.getNotificationType().equals(Notification.Type.PROGRAM_HEART_BEAT)) {
      RunRecordDetail runRecordMeta = appMetadataStore.getRun(programRunId);
      long heartBeatTimeInSeconds =
        TimeUnit.MILLISECONDS.toSeconds(Long.parseLong(properties.get(ProgramOptionConstants.HEART_BEAT_TIME)));
      writeToHeartBeatTable(runRecordMeta, heartBeatTimeInSeconds, programHeartbeatTable);
      // we can return after writing to heart beat table
      return Collections.emptyList();
    }
    List<Runnable> result = new ArrayList<>();
    if (programRunStatus != null) {
      handleProgramEvent(programRunId, programRunStatus, notification, messageIdBytes,
                         appMetadataStore, programHeartbeatTable, result);
    }
    if (clusterStatus == null) {
      return result;
    }
    LOG.info("---Cluster status to be handled - {}---", clusterStatus);
    handleClusterEvent(programRunId, clusterStatus, notification,
                       messageIdBytes, appMetadataStore, context).ifPresent(result::add);
    return result;
  }

  private void handleProgramEvent(ProgramRunId programRunId, ProgramRunStatus programRunStatus,
                                  Notification notification, byte[] messageIdBytes,
                                  AppMetadataStore appMetadataStore,
                                  ProgramHeartbeatTable programHeartbeatTable,
                                  List<Runnable> runnables) throws Exception {
    LOG.trace("Processing program status notification: {}", notification);
    Map<String, String> properties = notification.getProperties();
    String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);

    RunRecordDetail recordedRunRecord;
    switch (programRunStatus) {
      case STARTING:
        try {
          RunRecordDetail runRecordDetail = appMetadataStore.getRun(programRunId);
          if (runRecordDetail != null
            && runRecordDetail.getStatus() != ProgramRunStatus.PENDING
            && runRecordDetail.getStatus() != ProgramRunStatus.STARTING) {
            //This is an invalid state transition happening. Valid state transitions are:
            // PENDING => STARTING : normal state transition
            // STARTING => STARTING : state transition after app-fabric restart
            LOG.debug("Ignoring unexpected request to transition program run {} from {} state to program " +
                        "STARTING state.", programRunId, runRecordDetail.getStatus());
            return;
          }
        } catch (IllegalStateException ex) {
          LOG.error("Request to transition program run {} from non-existent state to program STARTING state " +
                      "but multiple run IDs exist.", programRunId);
        }

        String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
        Map<String, String> systemArguments = systemArgumentsString == null ?
          Collections.emptyMap() : GSON.fromJson(systemArgumentsString, STRING_STRING_MAP);
        boolean isInWorkflow = systemArguments.containsKey(ProgramOptionConstants.WORKFLOW_NAME);
        boolean skipProvisioning = Boolean.parseBoolean(systemArguments.get(ProgramOptionConstants.SKIP_PROVISIONING));

        ProgramOptions prgOptions = ProgramOptions.fromNotification(notification, GSON);
        ProgramDescriptor prgDescriptor =
          GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);

        // if this is a preview run or a program within a workflow, we don't actually need to provision a cluster
        // instead, we skip forward past the provisioning and provisioned states and go straight to starting.
        // if this is NOT a preview run or a program within a workflow (i.e., else case), program is started and its
        // state changes into Starting.
        if (isInWorkflow || skipProvisioning) {
          appMetadataStore.recordProgramProvisioning(programRunId, prgOptions.getUserArguments().asMap(),
                                                     prgOptions.getArguments().asMap(), messageIdBytes,
                                                     prgDescriptor.getArtifactId().toApiArtifactId());
          appMetadataStore.recordProgramProvisioned(programRunId, 0, messageIdBytes);
        } else {
          runnables.add(() -> {
            String oldUser = SecurityRequestContext.getUserId();
            try {
              SecurityRequestContext.setUserId(prgOptions.getArguments().getOption(ProgramOptionConstants.USER_ID));
              try {
                programLifecycleService.startInternal(prgDescriptor, prgOptions, programRunId);
              } catch (Exception e) {
                LOG.error("Failed to start program {}", programRunId, e);
                programStateWriter.error(programRunId, e);
              }
            } finally {
              SecurityRequestContext.setUserId(oldUser);
            }
          });
        }

        recordedRunRecord = appMetadataStore.recordProgramStart(programRunId, twillRunId,
                                                                systemArguments, messageIdBytes);
        writeToHeartBeatTable(recordedRunRecord,
                              RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS),
                              programHeartbeatTable);

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
        writeToHeartBeatTable(recordedRunRecord, logicalStartTimeSecs, programHeartbeatTable);
        runRecordMonitorService.removeRequest(programRunId, true);
        long startDelayTime = logicalStartTimeSecs - RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
        emitStartingTimeMetric(programRunId, startDelayTime, recordedRunRecord);
        break;
      case SUSPENDED:
        long suspendTime = getTimeSeconds(notification.getProperties(),
                                          ProgramOptionConstants.SUSPEND_TIME);
        // since we are adding suspend time recently, there might be old suspended notifications for which time
        // can be -1.
        recordedRunRecord = appMetadataStore.recordProgramSuspend(programRunId, messageIdBytes, suspendTime);
        writeToHeartBeatTable(recordedRunRecord, suspendTime, programHeartbeatTable);
        break;
      case RESUMING:
        long resumeTime = getTimeSeconds(notification.getProperties(),
                                         ProgramOptionConstants.RESUME_TIME);
        // since we are adding suspend time recently, there might be old suspended notifications for which time
        // can be -1.
        recordedRunRecord = appMetadataStore.recordProgramResumed(programRunId, messageIdBytes, resumeTime);
        writeToHeartBeatTable(recordedRunRecord, resumeTime, programHeartbeatTable);
        break;
      case STOPPING:
        Map<String, String> notificationProperties = notification.getProperties();
        long stoppingTsSecs = getTimeSeconds(notificationProperties, ProgramOptionConstants.STOPPING_TIME);
        if (stoppingTsSecs == -1L) {
          LOG.warn("Ignore program stopping notification for program {} without {} specified, {}",
                   programRunId, ProgramOptionConstants.STOPPING_TIME, notification);
          return;
        }
        long terminateTsSecs = getTimeSeconds(notificationProperties, ProgramOptionConstants.TERMINATE_TIME);

        recordedRunRecord = appMetadataStore.recordProgramStopping(programRunId, messageIdBytes, stoppingTsSecs,
                                                                   terminateTsSecs);
        LOG.info("---Recorded run record - '{}'---", recordedRunRecord);
        writeToHeartBeatTable(recordedRunRecord, stoppingTsSecs, programHeartbeatTable);
        break;
      case COMPLETED:
      case KILLED:
      case FAILED:
        recordedRunRecord = handleProgramCompletion(appMetadataStore, programHeartbeatTable,
                                                    programRunId, programRunStatus, notification,
                                                    messageIdBytes, runnables);
        break;
      case REJECTED:
        ProgramOptions programOptions = ProgramOptions.fromNotification(notification, GSON);
        ProgramDescriptor programDescriptor =
          GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);
        recordedRunRecord = appMetadataStore.recordProgramRejected(
          programRunId, programOptions.getUserArguments().asMap(),
          programOptions.getArguments().asMap(), messageIdBytes, programDescriptor.getArtifactId().toApiArtifactId());
        writeToHeartBeatTable(recordedRunRecord,
                              RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS),
                              programHeartbeatTable);
        getEmitMetricsRunnable(programRunId, recordedRunRecord, Constants.Metrics.Program.PROGRAM_REJECTED_RUNS,
                               null).ifPresent(runnables::add);
        runRecordMonitorService.removeRequest(programRunId, true);
        break;
      default:
        // This should not happen
        LOG.error("Unsupported program status {} for program {}, {}", programRunStatus, programRunId, notification);
        return;
    }

    if (recordedRunRecord != null) {
      // We need to publish the message so that the trigger subscriber can pick it up and start the trigger if
      // necessary
      publishRecordedStatus(notification, programRunId, recordedRunRecord.getStatus());
      // for any status that represents completion of a job that was actually started (excludes rejected jobs)
      // publish the deprovisioning event(s).
      if (programRunStatus.isEndState() && programRunStatus != ProgramRunStatus.REJECTED) {
        // if this is a preview run or a program within a workflow, we don't actually need to de-provision the cluster.
        // instead, we just record the state as deprovisioned without notifying the provisioner
        // and we will emit the program status metrics for it
        boolean isInWorkflow = recordedRunRecord.getSystemArgs().containsKey(ProgramOptionConstants.WORKFLOW_NAME);
        boolean skipProvisioning =
          Boolean.parseBoolean(recordedRunRecord.getSystemArgs().get(ProgramOptionConstants.SKIP_PROVISIONING));

        if (isInWorkflow || skipProvisioning) {
          appMetadataStore.recordProgramDeprovisioning(programRunId, messageIdBytes);
          appMetadataStore.recordProgramDeprovisioned(programRunId, null, messageIdBytes);
        } else {
          provisionerNotifier.deprovisioning(programRunId);
        }
      }
    }
  }

  /**
   * Handles a program completion notification.
   *
   * @param appMetadataStore the {@link AppMetadataStore} to write the status to
   * @param programHeartbeatTable the {@link ProgramHeartbeatTable} to write the status to
   * @param programRunId the program run of the completed program
   * @param programRunStatus the status of the completion
   * @param notification the {@link Notification} that carries information about the program completion
   * @param sourceId the source message id of the notification
   * @param runnables a {@link List} adding {@link Runnable} to be executed after event handling is completed
   * @return a {@link RunRecordDetail} that carries the result of updates to {@link AppMetadataStore}. If there
   *         is no update, {@code null} will be returned
   * @throws Exception if failed to update program status
   */
  @Nullable
  private RunRecordDetail handleProgramCompletion(AppMetadataStore appMetadataStore,
                                                  ProgramHeartbeatTable programHeartbeatTable,
                                                  ProgramRunId programRunId,
                                                  ProgramRunStatus programRunStatus,
                                                  Notification notification,
                                                  byte[] sourceId,
                                                  List<Runnable> runnables) throws Exception {
    Map<String, String> properties = notification.getProperties();

    long endTimeSecs = getTimeSeconds(properties, ProgramOptionConstants.END_TIME);
    if (endTimeSecs == -1) {
      LOG.warn("Ignore program {} notification for program {} without end time specified, {}",
               programRunStatus.name().toLowerCase(), programRunId, notification);
      return null;
    }

    BasicThrowable failureCause = decodeBasicThrowable(properties.get(ProgramOptionConstants.PROGRAM_ERROR));

    // If it is a workflow, process the inner program states first
    // We expect all inner program states has been received already before receiving the workflow state.
    // If there is any states missing, it will be handled here.
    if (programRunId.getType() == ProgramType.WORKFLOW) {
      processWorkflowOnStop(appMetadataStore, programHeartbeatTable, programRunId,
                            programRunStatus, notification, sourceId, runnables);
    }

    RunRecordDetailWithExistingStatus recordedRunRecord = appMetadataStore.recordProgramStop(programRunId, endTimeSecs,
                                                                                             programRunStatus,
                                                                                             failureCause, sourceId);
    if (recordedRunRecord != null) {
      writeToHeartBeatTable(recordedRunRecord, endTimeSecs, programHeartbeatTable);
      getEmitMetricsRunnable(programRunId, recordedRunRecord, STATUS_METRICS_NAME.get(programRunStatus),
                             recordedRunRecord.getExistingStatus()).ifPresent(runnables::add);

      // emit program run time metric.
      long runTime = endTimeSecs - RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
      SystemArguments
        .getProfileIdFromArgs(programRunId.getNamespaceId(), recordedRunRecord.getSystemArgs())
        .ifPresent(profileId -> {
          emitRunTimeMetric(programRunId, programRunStatus, runTime, recordedRunRecord);
          emitStoppingTimeMetric(programRunId, profileId, recordedRunRecord);
        });

      runnables.add(() -> {
        programCompletionNotifiers.forEach(notifier -> notifier.onProgramCompleted(programRunId,
                                                                                   recordedRunRecord.getStatus()));
        runRecordMonitorService.removeRequest(programRunId, true);
      });
    }
    return recordedRunRecord;
  }

  /**
   * On workflow program stop, inspects inner program states and adjust them if they are not in end state already.
   *
   * @param appMetadataStore the {@link AppMetadataStore} to write the status to
   * @param programHeartbeatTable the {@link ProgramHeartbeatTable} to write the status to
   * @param programRunId the program run of the completed program
   * @param programRunStatus the status of the completion
   * @param notification the {@link Notification} that carries information about the workflow completion
   * @param sourceId the source message id of the notification
   * @param runnables a {@link List} adding {@link Runnable} to be executed after event handling is completed
   * @throws Exception if failed to update program status
   */
  private void processWorkflowOnStop(AppMetadataStore appMetadataStore,
                                     ProgramHeartbeatTable programHeartbeatTable,
                                     ProgramRunId programRunId,
                                     ProgramRunStatus programRunStatus,
                                     Notification notification,
                                     byte[] sourceId, List<Runnable> runnables) throws Exception {
    ApplicationId appId = programRunId.getParent().getParent();
    WorkflowSpecification workflowSpec = Optional.ofNullable(appMetadataStore.getApplication(appId))
      .map(appMeta -> appMeta.getSpec().getWorkflows().get(programRunId.getProgram()))
      .orElse(null);

    // If cannot find the workflow spec (e.g. app deleted), then there is nothing we can do.
    if (workflowSpec == null) {
      return;
    }

    // For all MR and Spark nodes, we need to update the inner program run status if they are not in end state yet.
    for (WorkflowNode workflowNode : workflowSpec.getNodeIdMap().values()) {
      if (!(workflowNode instanceof WorkflowActionNode)) {
        continue;
      }

      ScheduleProgramInfo programInfo = ((WorkflowActionNode) workflowNode).getProgram();
      if (!WORKFLOW_INNER_PROGRAM_TYPES.containsKey(programInfo.getProgramType())) {
        continue;
      }

      // Get all active runs of the inner program. If the parent workflow runId is the same as this one,
      // set a terminal state for the inner program run.
      ProgramId innerProgramId = appId.program(WORKFLOW_INNER_PROGRAM_TYPES.get(programInfo.getProgramType()),
                                               programInfo.getProgramName());

      Map<ProgramRunId, Notification> innerProgramNotifications = new LinkedHashMap<>();

      appMetadataStore.scanActiveRuns(innerProgramId, runRecord -> {
        Map<String, String> systemArgs = runRecord.getSystemArgs();
        String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
        String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);

        if (workflowName == null || workflowRun == null) {
          return;
        }

        ProgramRunId workflowRunId = appId.program(ProgramType.WORKFLOW, workflowName).run(workflowRun);
        if (!programRunId.equals(workflowRunId)) {
          return;
        }

        Map<String, String> notificationProps = new HashMap<>(notification.getProperties());
        notificationProps.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runRecord.getProgramRunId()));

        innerProgramNotifications.put(runRecord.getProgramRunId(),
                                      new Notification(Notification.Type.PROGRAM_STATUS, notificationProps));
      });

      for (Map.Entry<ProgramRunId, Notification> entry : innerProgramNotifications.entrySet()) {
        handleProgramEvent(entry.getKey(), programRunStatus, entry.getValue(),
                           sourceId, appMetadataStore, programHeartbeatTable, runnables);
      }
    }
  }

  /**
   * write to heart beat table if the recordedRunRecord is not null
   */
  private void writeToHeartBeatTable(@Nullable RunRecordDetail recordedRunRecord,
                                     long timestampInSeconds,
                                     ProgramHeartbeatTable programHeartbeatTable) throws IOException {
    if (recordedRunRecord != null) {
      programHeartbeatTable.writeRunRecordMeta(recordedRunRecord, timestampInSeconds);
    }
  }

  /**
   * Handles a notification related to cluster operations.
   *
   * @param programRunId program run id from the event
   * @param clusterStatus cluster status from the event
   * @param notification the notification to process
   * @param messageIdBytes the unique ID for the notification message
   * @param appMetadataStore the data table to use
   * @param context the table context for performing table operations
   * @return an {@link Optional} of {@link Runnable} to carry a task to execute after handling of this event completed.
   *         See {@link #postProcess()} for details.
   * @throws IOException if failed to read/write to the app metadata store.
   */
  private Optional<Runnable> handleClusterEvent(ProgramRunId programRunId, ProgramRunClusterStatus clusterStatus,
                                                Notification notification, byte[] messageIdBytes,
                                                AppMetadataStore appMetadataStore, StructuredTableContext context)
                                                throws IOException, InterruptedException {
    Map<String, String> properties = notification.getProperties();

    ProgramOptions programOptions = ProgramOptions.fromNotification(notification, GSON);
    String userId = properties.get(ProgramOptionConstants.USER_ID);

    long endTs = getTimeSeconds(properties, ProgramOptionConstants.CLUSTER_END_TIME);
    ProgramDescriptor programDescriptor =
      GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);
    switch (clusterStatus) {
      case PROVISIONING:
        appMetadataStore.recordProgramProvisioning(programRunId, programOptions.getUserArguments().asMap(),
                                                   programOptions.getArguments().asMap(), messageIdBytes,
                                                   programDescriptor.getArtifactId().toApiArtifactId());

        ProvisionRequest provisionRequest = new ProvisionRequest(programRunId, programOptions, programDescriptor,
                                                                 userId);
        return Optional.of(provisioningService.provision(provisionRequest, context));
      case PROVISIONED:
        Cluster cluster = GSON.fromJson(properties.get(ProgramOptionConstants.CLUSTER), Cluster.class);
        RunRecordDetail runRecord =
          appMetadataStore.recordProgramProvisioned(programRunId, cluster.getNodes().size(), messageIdBytes);
        // if the run was stopped right before provisioning completes, it is possible for the state
        // to transition to stopping or killed, but not in time to cancel the provisioning.
        // In that case, we end up with a provisioned message, but we don't want to start the program.
        if (runRecord == null || runRecord.getStatus() == ProgramRunStatus.STOPPING
          || runRecord.getStatus() == ProgramRunStatus.KILLED) {
          break;
        }

        // Update the ProgramOptions system arguments to include information needed for program execution
        Map<String, String> systemArgs = new HashMap<>(programOptions.getArguments().asMap());
        systemArgs.put(ProgramOptionConstants.USER_ID, properties.get(ProgramOptionConstants.USER_ID));
        systemArgs.put(ProgramOptionConstants.CLUSTER, properties.get(ProgramOptionConstants.CLUSTER));
        systemArgs.put(ProgramOptionConstants.SECURE_KEYS_DIR, properties.get(ProgramOptionConstants.SECURE_KEYS_DIR));

        ProgramOptions newProgramOptions = new SimpleProgramOptions(programOptions.getProgramId(),
                                                                    new BasicArguments(systemArgs),
                                                                    programOptions.getUserArguments());

        // Publish the program STARTING state before starting the program
        programStateWriter.start(programRunId, newProgramOptions, null, programDescriptor);

        // emit provisioning time metric
        long provisioningTime = System.currentTimeMillis() / 1000 -
          RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
        SystemArguments
          .getProfileIdFromArgs(programRunId.getNamespaceId(), systemArgs)
          .ifPresent(profileId -> emitProvisioningTimeMetric(programRunId, profileId,
                                                             programOptions, provisioningTime));

        break;
      case DEPROVISIONING:
        RunRecordDetail recordedMeta = appMetadataStore.recordProgramDeprovisioning(programRunId, messageIdBytes);
        // If we skipped recording the run status, that means this was a duplicate message,
        // or an invalid state transition. In both cases, we should not try to deprovision the cluster.
        if (recordedMeta != null) {
          LOG.info("---Processing deprovisioning---");
          return Optional.of(provisioningService.deprovision(programRunId, context));
        }
        break;
      case DEPROVISIONED:
        appMetadataStore.recordProgramDeprovisioned(programRunId, endTs, messageIdBytes);
        break;
      case ORPHANED:
        appMetadataStore.recordProgramOrphaned(programRunId, endTs, messageIdBytes);
        break;
    }

    return Optional.empty();
  }

  private Optional<Runnable> getEmitMetricsRunnable(ProgramRunId programRunId,
                                                    @Nullable RunRecordDetail recordedRunRecord, String metricName,
                                                    @Nullable ProgramRunStatus existingStatus) {
    if (recordedRunRecord == null) {
      return Optional.empty();
    }
    Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(),
                                                                       recordedRunRecord.getSystemArgs());
    Map<String, String> additionalTags = getAdditionalTagsForProgramMetrics(recordedRunRecord,
                                                                            existingStatus);
    return profile.map(profileId -> () -> emitProfileMetrics(programRunId, profileId, metricName, additionalTags));
  }

  private void publishRecordedStatus(Notification notification,
                                     ProgramRunId programRunId, ProgramRunStatus status) throws Exception {
    Map<String, String> notificationProperties = new HashMap<>(notification.getProperties());
    notificationProperties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
    notificationProperties.put(ProgramOptionConstants.PROGRAM_STATUS, status.name());
    notificationProperties.put(CDAP_VERSION, ProjectInfo.getVersion().toString());
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
    return (timeString == null) ? -1 : TimeUnit.MILLISECONDS.toSeconds(Long.parseLong(timeString));
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
   * Emit the metrics context for the program, the tags are constructed with the program run id and
   * the profile id
   */
  private void emitProfileMetrics(ProgramRunId programRunId, ProfileId profileId, String metricName,
                                  Map<String, String> additionalTags) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun())
      .putAll(additionalTags)
      .build();

    metricsCollectionService.getContext(tags).increment(metricName, 1L);
  }

  /**
   * Emit the provisioning time metric. The tags are constructed with the program run id,
   * profile id and program options.
   */
  private void emitProvisioningTimeMetric(ProgramRunId programRunId, ProfileId profileId,
                                          ProgramOptions programOptions, long provisioningTime) {
    Map<String, String> args = programOptions.getArguments().asMap();
    String provisioner = SystemArguments.getProfileProvisioner(args);
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.PROVISIONER, provisioner)
      .put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun())
      .build();
    metricsCollectionService.getContext(tags).gauge(Constants.Metrics.Program.PROGRAM_PROVISIONING_DELAY_SECONDS,
                                                    provisioningTime);
  }

  /**
   * Emit the program run time metric. The tags are constructed with the program run id and program run status.
   */
  private void emitRunTimeMetric(ProgramRunId programRunId, ProgramRunStatus programRunStatus,
                                 long runTime, RunRecordDetail runRecord) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.STATUS, programRunStatus.name())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .putAll(getAdditionalTagsForProfileMetrics(runRecord, programRunId))
      .putAll(getAdditionalTagsForProgramMetrics(runRecord, null))
      .build();
    MetricsContext metricsContext = ProgramRunners.createProgramMetricsContext(programRunId, tags,
                                                                               metricsCollectionService);
    metricsContext.gauge(Constants.Metrics.Program.RUN_TIME_SECONDS, runTime);
  }

  /**
   * Emit stopping latency metric.
   */
  private void emitStoppingTimeMetric(ProgramRunId programRunId, ProfileId profileId, RunRecordDetail runRecord) {
    if (runRecord.getStopTs() == null || runRecord.getStoppingTs() == null) {
      return;
    }
    Map<String, String> additionalTags = new HashMap<>();
    // don't want to add the tag if it is not present otherwise it will result in NPE
    additionalTags.computeIfAbsent(Constants.Metrics.Tag.PROVISIONER,
                                   provisioner -> SystemArguments.getProfileProvisioner(
                                     runRecord.getSystemArgs()));
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .putAll(additionalTags)
      .build();
    MetricsContext metricsContext = ProgramRunners.createProgramMetricsContext(programRunId, tags,
                                                                               metricsCollectionService);

    metricsContext.gauge(Constants.Metrics.Program.PROGRAM_STOPPING_DELAY_SECONDS,
                         runRecord.getStopTs() - runRecord.getStoppingTs());
  }

  private void emitStartingTimeMetric(ProgramRunId programRunId, long startDelayTime,
                                      @Nullable RunRecordDetail runRecord) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .putAll(getAdditionalTagsForProfileMetrics(runRecord, programRunId))
      .putAll(getAdditionalTagsForProgramMetrics(runRecord, null))
      .build();
    MetricsContext metricsContext = ProgramRunners.createProgramMetricsContext(programRunId, tags,
                                                                               metricsCollectionService);
    metricsContext.gauge(Constants.Metrics.Program.PROGRAM_STARTING_DELAY_SECONDS, startDelayTime);
  }

  private Map<String, String> getAdditionalTagsForProfileMetrics(@Nullable RunRecordDetail runRecord,
                                                                 ProgramRunId programRunId) {
    Map<String, String> tags = new HashMap<>(Collections.emptyMap());
    if (runRecord == null) {
      return tags;
    }
    SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(), runRecord.getSystemArgs())
      .ifPresent(profileId -> {
        tags.putIfAbsent(Constants.Metrics.Tag.PROFILE, profileId.getProfile());
        tags.putIfAbsent(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name());
      });
    return tags;
  }

  /**
   * Returns an instance of {@link AppMetadataStore}.
   */
  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }

  private Map<String, String> getAdditionalTagsForProgramMetrics(@Nullable RunRecordDetail runRecord,
                                                                 @Nullable ProgramRunStatus existingStatus) {
    Map<String, String> additionalTags = new HashMap<>();
    if (runRecord == null) {
      return additionalTags;
    }
    // don't want to add the tag if it is not present otherwise it will result in NPE
    additionalTags.computeIfAbsent(Constants.Metrics.Tag.PROVISIONER,
                                   provisioner -> SystemArguments.getProfileProvisioner(runRecord.getSystemArgs()));
    additionalTags.computeIfAbsent(Constants.Metrics.Tag.CLUSTER_STATUS,
                                   clusterStatus -> runRecord.getCluster().getStatus().name());
    additionalTags.computeIfAbsent(Constants.Metrics.Tag.EXISTING_STATUS,
                                   existingProgramStatus -> existingStatus == null ? null : existingStatus.name());

    return additionalTags;
  }
}
