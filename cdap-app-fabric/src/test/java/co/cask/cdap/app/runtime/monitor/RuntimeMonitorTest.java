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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorServer;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Runtime Monitor Test
 */
public class RuntimeMonitorTest {

  private static CConfiguration cConf;
  private static MessagingService messagingService;
  private RuntimeMonitorServer runtimeServer;
  private MultiThreadMessagingContext messagingContext;
  private TransactionManager txManager;
  private TransactionSystemClient transactionSystemClient;
  private DatasetService datasetService;
  private DatasetFramework datasetFramework;
  private Transactional transactional;

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
    cConf.set(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, "1000");
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    messagingService.createTopic(new TopicMetadata(new TopicId("system", "cdap-programStatus")));
    messagingContext = new MultiThreadMessagingContext(messagingService);

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    transactionSystemClient = injector.getInstance(TransactionSystemClient.class);

    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(transactionSystemClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    datasetFramework = injector.getInstance(DatasetFramework.class);

    runtimeServer = injector.getInstance(RuntimeMonitorServer.class);
    runtimeServer.startAndWait();
  }

  @After
  public void stop() throws Exception {
    runtimeServer.stopAndWait();
    datasetService.stopAndWait();
    txManager.stopAndWait();

    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testRunTimeMonitor() throws Exception {
    RunId runId = RunIds.generate();
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app1").workflow("myworkflow").run(runId);
    publishProgramStatus(programRunId);
    verifyPublishedMessages(2, cConf, 2, null);

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

    RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId,
                                                       cConfCopy, messagingService, clientConfigBuilder.build(),
                                                       datasetFramework, transactionSystemClient);

    runtimeMonitor.startAndWait();
    // use different configuration for verification
    String lastProcessed = verifyPublishedMessages(2, cConfCopy, 2, null);
    runtimeMonitor.stopAndWait();

    // publish some more messages to test offset manager
    publishProgramStatus(programRunId);
    verifyPublishedMessages(2, cConf, 2, lastProcessed);

    runtimeMonitor = new RuntimeMonitor(programRunId, cConfCopy, messagingService, clientConfigBuilder.build(),
                                        datasetFramework, transactionSystemClient);
    runtimeMonitor.startAndWait();
    // use different configuration for verification
    lastProcessed = verifyPublishedMessages(2, cConfCopy, 2, lastProcessed);

    // publish completed status to trigger offset clean up
    publishCompletedStatus(programRunId);

    // use different configuration for verification
    verifyPublishedMessages(2, cConfCopy, 1, lastProcessed);

    // wait for runtime server to stop automatically
    Tasks.waitFor(true, () -> !runtimeServer.isRunning(), 5, TimeUnit.MINUTES);

    runtimeMonitor.stopAndWait();
  }

  private String verifyPublishedMessages(int limit, CConfiguration cConfig,
                                         int expectedCount, final String messageId) throws Exception {
    final String[] lastProcessed = {null};

    Tasks.waitFor(true, new Callable<Boolean>() {
      int count = 0;

      @Override
      public Boolean call() throws Exception {
        transactional.execute(context -> {
          MessageFetcher fetcher = messagingContext.getMessageFetcher();
          try (CloseableIterator<Message> iter =
                 fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                               cConfig.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC), limit, messageId)) {
            while (iter.hasNext()) {
              Message message = iter.next();
              lastProcessed[0] = message.getId();
              count++;
            }
          }
        });

        return count == expectedCount;
      }
    }, 5, TimeUnit.MINUTES);

    return lastProcessed[0];
  }

  private void publishProgramStatus(ProgramRunId programRunId) {
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(cConf, messagingService);
    programStateWriter.start(programRunId, new SimpleProgramOptions(programRunId.getParent()), null, null);
    programStateWriter.running(programRunId, null);
  }

  private void publishCompletedStatus(ProgramRunId programRunId) {
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(cConf, messagingService);
    programStateWriter.completed(programRunId);
  }
}
