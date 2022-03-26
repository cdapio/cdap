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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.TooManyRequestsException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
import io.cdap.cdap.internal.app.services.RunRecordMonitorService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Service that receives program start notification and launches provisioning when not hitting flow control limits.
 */
public class ProgramStartSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStartSubscriberService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private final int maxConcurrentRuns;
  private final int maxConcurrentLaunching;
  private final RunRecordMonitorService runRecordMonitorService;
  private final ProvisionerNotifier provisionerNotifier;

  @Inject
  ProgramStartSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                MetricsCollectionService metricsCollectionService,
                                TransactionRunner transactionRunner,
                                RunRecordMonitorService runRecordMonitorService,
                                ProvisionerNotifier provisionerNotifier) {
    super("provision.pending", cConf,
          cConf.get(Constants.AppFabric.PROGRAM_START_EVENT_TOPIC),
          cConf.getInt(Constants.AppFabric.PROGRAM_START_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.PROGRAM_START_EVENT_POLL_DELAY_MILLIS),
          messagingService, metricsCollectionService, transactionRunner);
    this.maxConcurrentRuns = cConf.getInt(Constants.AppFabric.MAX_CONCURRENT_RUNS);
    this.maxConcurrentLaunching = cConf.getInt(Constants.AppFabric.MAX_CONCURRENT_LAUNCHING);
    this.runRecordMonitorService = runRecordMonitorService;
    this.provisionerNotifier = provisionerNotifier;
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
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      processNotification(messagePair.getSecond());
    }
  }
  
  /**
   * Process a {@link Notification} received from TMS.
   */
  private void processNotification(Notification notification)
    throws IllegalArgumentException, TooManyRequestsException {
    // Validate notification type
    if (!notification.getNotificationType().equals(Notification.Type.PROGRAM_STATUS)) {
      LOG.warn("Unexpected notification type {}. Should be {}",
               notification.getNotificationType().toString(),
               Notification.Type.PROGRAM_STATUS);
      return;
    }

    Map<String, String> properties = notification.getProperties();

    // Extract and validate ProgramRunId.
    String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
    if (programRun == null) {
      LOG.warn("Unexpected notification: missing program run ID: {}", notification);
      return;
    }
    ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);

    // Extract and validate ProgramRunClusterStatus is ENQUEUED
    String clusterStatusStr = properties.get(ProgramOptionConstants.CLUSTER_STATUS);
    if (clusterStatusStr == null) {
      LOG.warn("Unexpected notification: cluster status is missing: {}", notification);
      return;
    }
    ProgramRunClusterStatus clusterStatus = null;
    clusterStatus = ProgramRunClusterStatus.valueOf(clusterStatusStr);
    if (clusterStatus != ProgramRunClusterStatus.ENQUEUED) {
      LOG.error("Unexpected notification: cluster status is {}, expecting {}"
                clusterStatus, ProgramRunClusterStatus.ENQUEUED););
      return;
    }

    // Start provisioning
    ProgramOptions programOptions = ProgramOptions.fromNotification(notification, GSON);
    ProgramDescriptor programDescriptor = GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR),
                                                        ProgramDescriptor.class);
    String userId = properties.get(ProgramOptionConstants.USER_ID);
    addLaunchingRequest(programRunId);
    provisionerNotifier.provisioning(programRunId, programOptions, programDescriptor, userId);
  }

  private void addLaunchingRequest(ProgramRunId programRunId) throws TooManyRequestsException {
    RunRecordMonitorService.Counter count;
    try {
      count = runRecordMonitorService.addRequestAndGetCount(programRunId);
    } catch (IllegalArgumentException e) {
      LOG.warn("Start program without flow control");
      return;
    }

    try {
      if (maxConcurrentRuns >= 0 &&
        count.getReservedLaunchingCount() + count.getLaunchingCount() + count.getRunningCount() > maxConcurrentRuns) {
        throw new TooManyRequestsException(
          String.format("Cannot start program because of %d reserved + %d launching + %d running > %d",
                        count.getReservedLaunchingCount(), count.getLaunchingCount(),
                        count.getRunningCount(), maxConcurrentRuns));
      }

      if (maxConcurrentLaunching >= 0 &&
        count.getReservedLaunchingCount() + count.getLaunchingCount() > maxConcurrentLaunching) {
        throw new TooManyRequestsException(
          String.format("Cannot start program because of %d reserved + %d launching > %d",
                        count.getReservedLaunchingCount(), count.getLaunchingCount(), maxConcurrentLaunching));
      }
    } catch (TooManyRequestsException e) {
      runRecordMonitorService.removeRequest(programRunId);
      throw e;
    }
  }

  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }
}
