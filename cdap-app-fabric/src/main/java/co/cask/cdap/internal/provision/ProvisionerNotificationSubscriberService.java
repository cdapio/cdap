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
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ImmutablePair;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * Subscribes to TMS for program run status changes, calling the ProvisioningService when necessary.
 */
public class ProvisionerNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String NAME = "provisioner.status.subscriber";
  private final String programStatusTopic;
  private final int fetchSize;
  private final long pollDelayMillis;
  private final ExecutorService taskExecutorService;
  private final ProvisioningService provisioningService;
  private final DatasetFramework datasetFramework;

  @Inject
  private ProvisionerNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                                   DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                                   MetricsCollectionService metricsCollectionService,
                                                   ProvisioningService provisioningService) {
    super(NAME, cConf, cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC),
          false, cConf.getInt(Constants.Scheduler.PROGRAM_STATUS_EVENT_FETCH_SIZE),
          cConf.getInt(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS),
          messagingService, datasetFramework, txClient, metricsCollectionService);
    this.programStatusTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    this.pollDelayMillis = cConf.getLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS);
    this.fetchSize = cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE);
    this.taskExecutorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("provisioner-status-subscriber").build());
    this.provisioningService = provisioningService;
    this.datasetFramework = datasetFramework;
  }

  @Nullable
  @Override
  protected String loadMessageId(DatasetContext datasetContext) throws Exception {
    ProvisionerDataset.createIfNotExists(datasetFramework);
    ProvisionerDataset store = ProvisionerDataset.get(datasetContext);
    return store.getSubscriberState(NAME);
  }

  @Override
  protected void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception {
    ProvisionerDataset store = ProvisionerDataset.get(datasetContext);
    store.persistSubscriberState(NAME, messageId);
  }

  @Override
  protected void processMessages(DatasetContext datasetContext,
                                 Iterator<ImmutablePair<String, Notification>> messages) throws Exception {
    while (messages.hasNext()) {
      Notification notification = messages.next().getSecond();
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
          String userId = properties.get(ProgramOptionConstants.USER_ID);
          ProvisionRequest provisionRequest = new ProvisionRequest(programRunId, programOptions, programDescriptor,
                                                                   userId);
          provisioningService.provision(provisionRequest);
          break;
        case DEPROVISIONING:
          provisioningService.deprovision(programRunId);
          break;
      }
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
