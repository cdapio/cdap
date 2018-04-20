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

package co.cask.cdap.app.runtime.monitor;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorServer;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Runtime Monitor Test
 */
public class RuntimeMonitorTest {

  private static CConfiguration cConf;
  private static MessagingService messagingService;
  private RuntimeMonitorServer runtimeServer;
  private MessagingContext messagingContext;
  private Injector injector;
  private TransactionManager txManager;
  private TransactionSystemClient transactionSystemClient;
  private DatasetService datasetService;
  private DatasetFramework datasetFramework;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Before
  public void init() throws IOException, UnsupportedTypeException, TopicAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, "programStatus");

    // clear the host to make sure it binds to loopback address
    cConf.unset(Constants.RuntimeMonitor.SERVER_HOST);
    cConf.set(Constants.RuntimeMonitor.SERVER_PORT, "0");
    cConf.set(Constants.RuntimeMonitor.BATCH_LIMIT, "2");
    cConf.set(Constants.RuntimeMonitor.POLL_TIME_MS, "200");
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    this.injector = Guice.createInjector(new AppFabricTestModule(cConf));
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    messagingService.createTopic(new TopicMetadata(new TopicId("system", "cdap-programStatus")));
    messagingContext = new MultiThreadMessagingContext(messagingService);

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    transactionSystemClient = injector.getInstance(TransactionSystemClient.class);

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    datasetFramework = injector.getInstance(DatasetFramework.class);

    runtimeServer = injector.getInstance(RuntimeMonitorServer.class);
    runtimeServer.startAndWait();
  }

  @After
  public void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }

    txManager.stopAndWait();
    datasetService.stopAndWait();
    runtimeServer.stopAndWait();
  }

  @Test
  public void testRunTimeMonitor() throws Exception {
    String programRunId = UUID.randomUUID().toString();
    publishProgramStatus(programRunId);
    verifyPublishedMessages(2, cConf, 2);

    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(runtimeServer.getBindAddress().getAddress().getHostAddress())
      .setPort(runtimeServer.getBindAddress().getPort())
      .setSSLEnabled(true)
      .build();
    ClientConfig.Builder clientConfigBuilder = ClientConfig.builder()
      .setDefaultReadTimeout(60000)
      .setApiVersion("v1")
      .setVerifySSLCert(false)
      .setConnectionConfig(connectionConfig);

    CConfiguration cConfCopy = CConfiguration.copy(cConf);
    // change topic name because cdap config is different than runtime config
    cConfCopy.set(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, "cdap-programStatus");

    RuntimeMonitor runtimeMonitor = new RuntimeMonitor(new ProgramRunId("default", "app1", ProgramType.WORKFLOW,
                                                                        "myworkflow", programRunId), cConfCopy,
                                                       datasetFramework, transactionSystemClient,
                                                       messagingContext.getMessagePublisher(),
                                                       clientConfigBuilder.build());
    runtimeMonitor.startAndWait();
    // use different configuration for verification
    verifyPublishedMessages(2, cConfCopy, 2);
    runtimeMonitor.stopAndWait();

    // publish some more messages to test offset manager
    publishProgramStatus(programRunId);
    verifyPublishedMessages(2, cConf, 4);

    runtimeMonitor = new RuntimeMonitor(new ProgramRunId("default", "app1", ProgramType.WORKFLOW,
                                                         "myworkflow", programRunId), cConfCopy,
                                                       datasetFramework, transactionSystemClient,
                                                       messagingContext.getMessagePublisher(),
                                                       clientConfigBuilder.build());
    runtimeMonitor.startAndWait();
    // use different configuration for verification
    verifyPublishedMessages(2, cConfCopy, 4);

    // publish completed status to trigger offset clean up
    publishCompletedStatus(programRunId);

    // use different configuration for verification
    verifyPublishedMessages(2, cConfCopy, 5);

    // wait for runtime server to stop automatically
    Tasks.waitFor(true, () -> !runtimeServer.isRunning(), 5, TimeUnit.MINUTES);

    runtimeMonitor.stopAndWait();
  }

  private void verifyPublishedMessages(int limit, CConfiguration cConfig, int expectedCount) throws Exception {
    MessageFetcher fetcher = messagingContext.getMessageFetcher();
    final String[] messageId = {null};
    Tasks.waitFor(true, new Callable<Boolean>() {
      int count = 0;

      @Override
      public Boolean call() throws Exception {
        try (CloseableIterator<Message> iter =
               fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                             cConfig.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC), limit, messageId[0])) {
          while (iter.hasNext()) {
            Message message = iter.next();
            messageId[0] = message.getId();
            count++;
          }
        }

        return count == expectedCount;
      }
    }, 5, TimeUnit.MINUTES);
  }

  private void publishProgramStatus(String programRunUuid) {
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(cConf, messagingService);

    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "app1");
    ProgramRunId programRunId = new ProgramRunId(appId, ProgramType.WORKFLOW, "myworkflow",
                                                 programRunUuid);
    programStateWriter.start(programRunId, new SimpleProgramOptions(new ProgramId(appId, ProgramType.WORKFLOW,
                                                                                  "myworkflow")), null, null);
    programStateWriter.running(programRunId, null);
  }

  private void publishCompletedStatus(String programRunUuid) {
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(cConf, messagingService);
    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "app1");
    ProgramRunId programRunId = new ProgramRunId(appId, ProgramType.WORKFLOW, "myworkflow", programRunUuid);
    programStateWriter.completed(programRunId);
  }
}
