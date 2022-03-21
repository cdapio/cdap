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

package io.cdap.cdap.internal.provision;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.TooManyRequestsException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
import io.cdap.cdap.internal.app.services.RunRecordMonitorService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.reporting.ProgramHeartbeatTable;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Service that receives program status notifications and persists to the store.
 * No transactions should be started in any of the overrided methods since they are already wrapped in a transaction.
 */
public class ProvisionNotificationSubscriberService extends AbstractNotificationSubscriberService {

  private static final Logger LOG = LoggerFactory.getLogger(ProvisionNotificationSubscriberService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final Queue<Runnable> tasks;
  private final MetricsCollectionService metricsCollectionService;
  private final CConfiguration cConf;
  private final RunRecordMonitorService runRecordMonitorService;

  @Inject
  ProvisionNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                         MetricsCollectionService metricsCollectionService,
                                         ProvisioningService provisioningService,
                                         ProgramStateWriter programStateWriter, TransactionRunner transactionRunner,
                                         RunRecordMonitorService runRecordMonitorService) {
    super("provision", cConf, cConf.get(Constants.AppFabric.PROVISION_EVENT_TOPIC),
          cConf.getInt(Constants.AppFabric.PROVISION_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.PROVISION_EVENT_POLL_DELAY_MILLIS),
          messagingService, metricsCollectionService, transactionRunner);
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.tasks = new LinkedList<>();
    this.metricsCollectionService = metricsCollectionService;
    this.cConf = cConf;
    this.runRecordMonitorService = runRecordMonitorService;
  }

  @Override
  protected void doStartUp() throws Exception {
    super.doStartUp();
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
    List<Runnable> tasks = new LinkedList<>();
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      LOG.error("wyzhang: ProvsionNotifciationSubscriber notification process start");
      List<Runnable> runnables = processNotification(messagePair.getFirst().getBytes(StandardCharsets.UTF_8),
                                                     messagePair.getSecond(), structuredTableContext);
      LOG.error("wyzhang: ProvsionNotifciationSubscriber notification process end");
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
   * messages batch is completed
   * @throws Exception if failed to process the given notification
   */
  private List<Runnable> processNotification(byte[] messageIdBytes, Notification notification,
                                             StructuredTableContext context) throws Exception {
    AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
    Map<String, String> properties = notification.getProperties();

    // Extract and validate ProgramRunId. Ignore notification if ProgramRunId is missing, which should never happen.
    String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
    if (programRun == null) {
      LOG.warn("Ignore notification that misses program run state information, {}", notification);
      return Collections.emptyList();
    }
    ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);

    // Extract and validate ProgramRunClusterStatus.
    ProgramRunClusterStatus clusterStatus = null;
    String clusterStatusStr = properties.get(ProgramOptionConstants.CLUSTER_STATUS);
    if (clusterStatusStr != null) {
      try {
        clusterStatus = ProgramRunClusterStatus.valueOf(clusterStatusStr);
      } catch (IllegalArgumentException e) {
        LOG.warn("Ignore notification with invalid program run cluster status {} for program {}",
                 clusterStatusStr, programRun);
        return Collections.emptyList();
      }
    }

    List<Runnable> result = new ArrayList<>();

    if (clusterStatus != null) {
      handleClusterEvent(programRunId, clusterStatus, notification,
                         messageIdBytes, appMetadataStore, context).ifPresent(result::add);
    }

    return result;
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
   * See {@link #postProcess()} for details.
   * @throws IOException if failed to read/write to the app metadata store.
   */
  private Optional<Runnable> handleClusterEvent(ProgramRunId programRunId, ProgramRunClusterStatus clusterStatus,
                                                Notification notification, byte[] messageIdBytes,
                                                AppMetadataStore appMetadataStore,
                                                StructuredTableContext context) throws Exception {
    Map<String, String> properties = notification.getProperties();

    ProgramOptions programOptions = ProgramOptions.fromNotification(notification, GSON);
    String userId = properties.get(ProgramOptionConstants.USER_ID);

    long endTs = getTimeSeconds(properties, ProgramOptionConstants.CLUSTER_END_TIME);
    ProgramDescriptor programDescriptor =
      GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);
    switch (clusterStatus) {
      case PROVISIONING:
        RunRecordMonitorService.Counter counter = runRecordMonitorService.addRequestAndGetCount(programRunId);
        int maxConcurrentRuns = cConf.getInt(Constants.AppFabric.MAX_CONCURRENT_RUNS);
        LOG.error("wyzhang: ProvisionNotifcationSubsriberService: check if can provision: launching={},running={}",
                  counter.getLaunchingCount(),
                  counter.getRunningCount());
        if (maxConcurrentRuns >= 0 && maxConcurrentRuns < counter.getLaunchingCount() + counter.getRunningCount()) {
          String msg = String.format("Program runid %s cannot start provisioning " +
                                       "because the maximum of %d outstanding runs is allowed",
                                     programRunId, maxConcurrentRuns);
          LOG.info(msg);

          LOG.error("wyzhang: ProvisionNotifcationSubsriberService: reject provision");
          TooManyRequestsException e = new TooManyRequestsException(msg);
          throw e;
        }
        LOG.error("wyzhang: ProvisionNotifcationSubsriberService: do provision");

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
   * Returns an instance of {@link AppMetadataStore}.
   */
  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }
}
