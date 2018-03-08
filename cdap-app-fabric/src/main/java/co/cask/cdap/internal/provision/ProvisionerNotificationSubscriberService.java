/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.services.AbstractNotificationSubscriberService;
import co.cask.cdap.internal.app.services.ProgramNotificationSubscriberService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Subscribes to TMS for program run status changes, calling the ProvisioningService when necessary.
 */
public class ProvisionerNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final String programStatusTopic;
  private final int fetchSize;
  private final long pollDelayMillis;
  private final ExecutorService taskExecutorService;
  private final ProvisioningService provisioningService;
  private final ProvisionerNotifier provisionerNotifier;
  private final DatasetFramework datasetFramework;

  @Inject
  private ProvisionerNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                                   DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                                   MetricsCollectionService metricsCollectionService,
                                                   ProvisioningService provisioningService,
                                                   ProvisionerNotifier provisionerNotifier) {
    super(messagingService, cConf, datasetFramework, txClient, metricsCollectionService);
    this.programStatusTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    this.pollDelayMillis = cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS);
    this.fetchSize = cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE);
    this.taskExecutorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("provisioner-status-subscriber").build());
    this.provisioningService = provisioningService;
    this.provisionerNotifier = provisionerNotifier;
    this.datasetFramework = datasetFramework;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());
    taskExecutorService.submit(new ProvisioningSubscriberRunnable(programStatusTopic, pollDelayMillis, fetchSize));
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
   * Thread that receives TMS notifications on program status and provisions/deprovisions clusters.
   */
  private class ProvisioningSubscriberRunnable extends AbstractSubscriberRunnable {
    private static final String NAME = "provisioner.status.subscriber";

    ProvisioningSubscriberRunnable(String topic, long pollDelayMillis, int fetchSize) {
      // Fetching of program status events are non-transactional
      super(NAME, topic, pollDelayMillis, fetchSize, false);
    }

    @Nullable
    @Override
    protected String initialize(DatasetContext context) {
      try {
        ProvisionerStore.createIfNotExists(datasetFramework);
        ProvisionerStore store = ProvisionerStore.get(context);
        return store.getSubscriberState(NAME);
      } catch (Exception e) {
        throw new RetryableException(e);
      }
    }

    @Override
    public void persistMessageId(DatasetContext context, String lastFetchedMessageId) throws Exception {
      ProvisionerStore store = ProvisionerStore.get(context);
      store.persistSubscriberState(NAME, lastFetchedMessageId);
    }

    @Override
    protected void processNotifications(DatasetContext context,
                                        AbstractNotificationSubscriberService.NotificationIterator notifications)
      throws Exception {
      while (notifications.hasNext()) {
        processNotification(notifications.next());
      }
    }

    private void processNotification(Notification notification) throws Exception {
      Map<String, String> properties = notification.getProperties();

      // Required parameters
      String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String clusterStatusStr = properties.get(ProgramOptionConstants.CLUSTER_STATUS);

      // Ignore notifications about other state transitions
      if (programRun == null || clusterStatusStr == null) {
        return;
      }

      ProgramRunClusterStatus clusterStatus = ProgramRunClusterStatus.valueOf(clusterStatusStr);
      ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);
      ProgramId programId = programRunId.getParent();

      switch (clusterStatus) {
        case PROVISIONING:
          ProgramOptions programOptions = getProgramOptions(programId, properties);
          ProgramDescriptor programDescriptor =
            GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR), ProgramDescriptor.class);
          Cluster cluster = provisioningService.provision(programRunId, getProgramOptions(programId, properties));
          String userId = properties.get(ProgramOptionConstants.USER_ID);
          provisionerNotifier.provisioned(programRunId, programOptions, programDescriptor, userId, cluster);
          break;
        case DEPROVISIONING:
          provisioningService.deprovision(programRunId);
          provisionerNotifier.deprovisioned(programRunId);
          break;
      }
    }

    private ProgramOptions getProgramOptions(ProgramId programId, Map<String, String> properties) {
      String userArgumentsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
      String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      String debugString = properties.get(ProgramOptionConstants.DEBUG_ENABLED);

      Boolean debug = Boolean.valueOf(debugString);
      Map<String, String> userArguments = GSON.fromJson(userArgumentsString, MAP_TYPE);
      Map<String, String> systemArguments = GSON.fromJson(systemArgumentsString, MAP_TYPE);

      return new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                      new BasicArguments(userArguments), debug);
    }
  }
}
