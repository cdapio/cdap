/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.monitor;

import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.app.guice.RemoteExecutionDiscoveryModule;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.MultiThreadDatasetCache;
import io.cdap.cdap.data2.transaction.TransactionSystemClientAdapter;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteProcessController;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorClient;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorServer;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorServerInfo;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.tools.KeyStores;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Runtime Monitor Test
 */
public class RuntimeMonitorTest {

  private static CConfiguration cConf;
  private static MessagingService messagingService;

  private final AtomicReference<ProgramRunId> publishProgramKilled = new AtomicReference<>();

  private RuntimeMonitorServer runtimeServer;
  private MultiThreadMessagingContext messagingContext;
  private TransactionManager txManager;
  private DatasetOpExecutorService dsOpExecService;
  private DatasetService datasetService;
  private Transactional transactional;
  private TransactionRunner transactionRunner;

  private KeyStore serverKeyStore;
  private KeyStore clientKeyStore;


  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Before
  public void init() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    cConf.set(Constants.RuntimeMonitor.SERVER_INFO_FILE, new File(TMP_FOLDER.newFolder(), "info").getAbsolutePath());
    cConf.set(Constants.RuntimeMonitor.BATCH_SIZE, "2");
    cConf.set(Constants.RuntimeMonitor.POLL_TIME_MS, "200");
    cConf.set(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, "1000");

    serverKeyStore = KeyStores.generatedCertKeyStore(1, "");
    clientKeyStore = KeyStores.generatedCertKeyStore(1, "");

    Injector injector = Guice.createInjector(new PrivateModule() {
      @Override
      protected void configure() {
        bind(KeyStore.class).annotatedWith(Constants.AppFabric.KeyStore.class).toInstance(serverKeyStore);
        bind(KeyStore.class).annotatedWith(Constants.AppFabric.TrustStore.class).toInstance(clientKeyStore);
        bind(RuntimeMonitorServer.class);
        expose(RuntimeMonitorServer.class);

        // Bind a no-op Cancellable for the RuntimeMonitorService. The cancellable is for killing a program
        bind(Cancellable.class).toInstance(() -> {
          ProgramRunId programRunId = publishProgramKilled.get();
          if (programRunId != null && publishProgramKilled.compareAndSet(programRunId, null)) {
            publishProgramStatus(programRunId, ProgramStatus.KILLED);
          }
        });
      }
    }, Modules.override(new AppFabricTestModule(cConf)).with(new RemoteExecutionDiscoveryModule()));

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    messagingContext = new MultiThreadMessagingContext(messagingService);

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);
    TransactionSystemClient transactionSystemClient = injector.getInstance(TransactionSystemClient.class);

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);

    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(transactionSystemClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
    this.transactionRunner = injector.getInstance(TransactionRunner.class);

    dsOpExecService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpExecService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    runtimeServer = injector.getInstance(RuntimeMonitorServer.class);
    runtimeServer.startAndWait();

    verifyServerPortWritten();
  }

  @After
  public void stop() {
    runtimeServer.stopAndWait();
    datasetService.stopAndWait();
    dsOpExecService.stopAndWait();
    txManager.stopAndWait();

    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  /**
   * Verifies the {@link RuntimeMonitorServer} writes out its host and port to a file as specified by the
   * {@link Constants.RuntimeMonitor#SERVER_INFO_FILE} config.
   */
  private void verifyServerPortWritten() throws IOException {
    String file = cConf.get(Constants.RuntimeMonitor.SERVER_INFO_FILE);
    try (Reader reader = Files.newBufferedReader(Paths.get(file), StandardCharsets.UTF_8)) {
      RuntimeMonitorServerInfo info = new Gson().fromJson(reader, RuntimeMonitorServerInfo.class);
      Assert.assertEquals(runtimeServer.getBindAddress().getAddress(), info.getHostAddress());
      Assert.assertEquals(runtimeServer.getBindAddress().getPort(), info.getPort());
    }
  }

  @Test
  public void testRunTimeMonitor() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5, Threads.createDaemonThreadFactory("test"));

    verifyServerPortWritten();

    RunId runId = RunIds.generate();
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app1").workflow("myworkflow").run(runId);
    publishProgramStatus(programRunId, ProgramStatus.INITIALIZING);
    publishProgramStatus(programRunId, ProgramStatus.RUNNING);
    verifyPublishedMessages(cConf, 2, null);

    // change topic name because cdap config is different than runtime config
    CConfiguration monitorCConf = CConfiguration.copy(cConf);
    monitorCConf.set(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, "cdap-programStatus");
    messagingService.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic("cdap-programStatus")));

    RuntimeMonitorClient monitorClient = new RuntimeMonitorClient(HttpRequestConfig.DEFAULT,
                                                                  clientKeyStore, serverKeyStore,
                                                                  runtimeServer::getBindAddress, Proxy.NO_PROXY);

    RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, monitorCConf, monitorClient,
                                                       messagingContext, scheduler,
                                                       monitorMessage -> { }, new MockRemoteProcessController(),
                                                       new NoOpProgramStateWriter(), transactionRunner);

    runtimeMonitor.startAndWait();
    // use different configuration for verification
    String lastProcessed = verifyPublishedMessages(monitorCConf, 2, null);
    runtimeMonitor.stopAndWait();

    // publish some more messages to test offset manager
    publishProgramStatus(programRunId, ProgramStatus.RUNNING);
    publishProgramStatus(programRunId, ProgramStatus.RUNNING);
    verifyPublishedMessages(cConf, 2, lastProcessed);

    runtimeMonitor = new RuntimeMonitor(programRunId, monitorCConf, monitorClient,
                                        messagingContext, scheduler, monitorMessage -> { },
                                        new MockRemoteProcessController(), new NoOpProgramStateWriter(),
                                        transactionRunner);
    runtimeMonitor.startAndWait();
    // use different configuration for verification
    lastProcessed = verifyPublishedMessages(monitorCConf, 2, lastProcessed);

    // publish completed status to trigger offset clean up
    publishProgramStatus(programRunId, ProgramStatus.COMPLETED);

    // use different configuration for verification
    verifyPublishedMessages(monitorCConf, 1, lastProcessed);

    // The RuntimeServer should be stopped upon the RuntimeMonitor received the program completed state
    Tasks.waitFor(Service.State.TERMINATED, runtimeServer::state, 10, TimeUnit.SECONDS);
    // The RuntimeMonitor should stop itself upon receiving the program completed state
    Tasks.waitFor(Service.State.TERMINATED, runtimeMonitor::state, 10, TimeUnit.SECONDS);
  }

  @Test
  public void testTopicExpansion() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5, Threads.createDaemonThreadFactory("test"));

    RunId runId = RunIds.generate();
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app1").workflow("testTopicExpansion").run(runId);

    // change topic name because cdap config is different than runtime config
    CConfiguration monitorCConf = CConfiguration.copy(cConf);

    monitorCConf.set(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, "cdap-programStatus");
    messagingService.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic("cdap-programStatus")));

    String metricsPrefix = cConf.get(Constants.Metrics.TOPIC_PREFIX);
    int topicNum = monitorCConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM);
    String newMetricsPrefix = "cdap-" + metricsPrefix;
    monitorCConf.set(Constants.Metrics.TOPIC_PREFIX, newMetricsPrefix);

    // Create the metrics topics used by the runtime monitor
    for (int i = 0; i < topicNum; i++) {
      messagingService.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic(newMetricsPrefix + i)));
    }

    // Publish something to the metrics topic used by the runtime monitor server.
    for (int i = 0; i < topicNum; i++) {
      messagingContext.getMessagePublisher().publish(NamespaceId.SYSTEM.getNamespace(), metricsPrefix + i, "test" + i);
    }

    RuntimeMonitorClient monitorClient = new RuntimeMonitorClient(HttpRequestConfig.DEFAULT,
                                                                  clientKeyStore, serverKeyStore,
                                                                  runtimeServer::getBindAddress, Proxy.NO_PROXY);

    RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, monitorCConf, monitorClient,
                                                       messagingContext, scheduler, monitorMessage -> { },
                                                       new MockRemoteProcessController(), new NoOpProgramStateWriter(),
                                                       transactionRunner);
    runtimeMonitor.startAndWait();

    // Wait and verify messages as being republished by the runtime monitor to the "local" metrics topics
    Tasks.waitFor(true, () -> {
      // Fetch from each metrics topic and there should be a message inside
      for (int i = 0; i < topicNum; i++) {
        try (CloseableIterator<Message> iterator =
          messagingContext.getMessageFetcher()
            .fetch(NamespaceId.SYSTEM.getNamespace(), newMetricsPrefix + i, 10, null)) {
          Optional<String> message = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .map(Message::getPayloadAsString)
            .findFirst();
          if (!("test" + i).equals(message.orElse(null))) {
            return false;
          }
        }
      }

      return true;
    }, 1000, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // publish completed status to shutdown the runtime server and the runtime monitor
    publishProgramStatus(programRunId, ProgramStatus.COMPLETED);

    // The RuntimeServer should be stopped upon the RuntimeMonitor received the program completed state
    Tasks.waitFor(Service.State.TERMINATED, runtimeServer::state, 10, TimeUnit.SECONDS);
    // The RuntimeMonitor should stop itself upon receiving the program completed state
    Tasks.waitFor(Service.State.TERMINATED, runtimeMonitor::state, 10, TimeUnit.SECONDS);
  }

  @Test
  public void testStopProgram() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5, Threads.createDaemonThreadFactory("test"));

    RunId runId = RunIds.generate();
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app1").workflow("testkill").run(runId);
    publishProgramStatus(programRunId, ProgramStatus.INITIALIZING);
    publishProgramStatus(programRunId, ProgramStatus.RUNNING);

    // change topic name because cdap config is different than runtime config
    CConfiguration monitorCConf = CConfiguration.copy(cConf);
    monitorCConf.set(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, "cdap-programStatus");
    messagingService.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic("cdap-programStatus")));

    RuntimeMonitorClient monitorClient = new RuntimeMonitorClient(HttpRequestConfig.DEFAULT,
                                                                  clientKeyStore, serverKeyStore,
                                                                  runtimeServer::getBindAddress, Proxy.NO_PROXY);

    RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, monitorCConf, monitorClient,
                                                       messagingContext, scheduler, monitorMessage -> { },
                                                       new MockRemoteProcessController(), new NoOpProgramStateWriter(),
                                                       transactionRunner);

    runtimeMonitor.startAndWait();
    verifyPublishedMessages(monitorCConf, 2, null);

    // Set the program run id to have KILLED state published
    publishProgramKilled.set(programRunId);

    // Kill the running program via RuntimeMonitor.
    runtimeMonitor.requestStop();

    // The RuntimeServer should be stopped upon the RuntimeMonitor received the program completed state
    Tasks.waitFor(Service.State.TERMINATED, runtimeServer::state, 10, TimeUnit.SECONDS);
    // The RuntimeMonitor should stop itself upon receiving the program completed state
    Tasks.waitFor(Service.State.TERMINATED, runtimeMonitor::state, 10, TimeUnit.SECONDS);
  }


  private String verifyPublishedMessages(CConfiguration cConfig,
                                         int expectedCount, @Nullable final String messageId) throws Exception {
    final String[] lastProcessed = {null};

    Tasks.waitFor(true, new Callable<Boolean>() {
      int count = 0;

      @Override
      public Boolean call() throws Exception {
        transactional.execute(context -> {
          MessageFetcher fetcher = messagingContext.getMessageFetcher();
          try (CloseableIterator<Message> iter =
            fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                          cConfig.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC), 100, messageId)) {
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

  private void publishProgramStatus(ProgramRunId programRunId, ProgramStatus status) {
    ProgramStateWriter stateWriter = new MessagingProgramStateWriter(cConf, messagingService);

    switch (status) {
      case INITIALIZING:
        stateWriter.start(programRunId, new SimpleProgramOptions(programRunId.getParent()), null, null);
        break;
      case RUNNING:
        stateWriter.running(programRunId, null);
        break;
      case COMPLETED:
        stateWriter.completed(programRunId);
        break;
      case FAILED:
        stateWriter.error(programRunId, new Exception("Program run failed"));
        break;
      case KILLED:
        stateWriter.killed(programRunId);
        break;
      default:
        throw new IllegalArgumentException("Unsupported program status " + status);
    }
  }

  private static final class MockRemoteProcessController implements RemoteProcessController {

    @Override
    public boolean isRunning() {
      return false;
    }

    @Override
    public void kill() {

    }
  }
}
