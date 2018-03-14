/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.MockResponder;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for test cases that need to run MapReduce programs.
 */
@Category(XSlowTests.class)
public class MapReduceRunnerTestBase {

  private static final Gson GSON = new Gson();

  private static Injector injector;
  private static TransactionManager txService;
  private static StreamHandler streamHandler;

  protected static TransactionExecutorFactory txExecutorFactory;
  protected static DatasetFramework dsFramework;
  protected static DynamicDatasetCache datasetCache;
  protected static MetricStore metricStore;


  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  protected static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {
    @Override
    public File get() {
      try {
        return TEMP_FOLDER.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration conf = CConfiguration.create();
    // allow subclasses to override the following two parameters
    Integer txTimeout = Integer.getInteger(TxConstants.Manager.CFG_TX_TIMEOUT);
    if (txTimeout != null) {
      conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, txTimeout);
    }
    Integer txCleanupInterval = Integer.getInteger(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL);
    if (txCleanupInterval != null) {
      conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, txCleanupInterval);
    }
    injector = AppFabricTestHelper.getInjector(conf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class);
      }
    });
    txService = injector.getInstance(TransactionManager.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = injector.getInstance(DatasetFramework.class);
    datasetCache = new SingleThreadDatasetCache(
      new SystemDatasetInstantiator(dsFramework, MapReduceRunnerTestBase.class.getClassLoader(), null),
      injector.getInstance(TransactionSystemClient.class),
      NamespaceId.DEFAULT, DatasetDefinition.NO_ARGUMENTS, null, null);

    metricStore = injector.getInstance(MetricStore.class);
    txService.startAndWait();
    streamHandler = injector.getInstance(StreamHandler.class);

    // Always create the default namespace
    injector.getInstance(NamespaceAdmin.class).create(NamespaceMeta.DEFAULT);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    txService.stopAndWait();
  }

  @After
  public void after() throws Exception {
    // cleanup user data (only user datasets)
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(DefaultId.NAMESPACE)) {
      dsFramework.deleteInstance(DefaultId.NAMESPACE.dataset(spec.getName()));
    }
  }

  protected void writeToStream(String streamName, String body) throws IOException {
    writeToStream(Id.Stream.from(Id.Namespace.DEFAULT, streamName), body);
  }

  protected void writeToStream(Id.Stream streamId, String body) throws IOException {
    String path = String.format("/v3/namespaces/%s/streams/%s", streamId.getNamespaceId(), streamId.getId());
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path);

    request.content().writeCharSequence(body, StandardCharsets.UTF_8);
    HttpUtil.setContentLength(request, request.content().readableBytes());

    MockResponder responder = new MockResponder();
    try {
      streamHandler.enqueue(request, responder, streamId.getNamespaceId(), streamId.getId());
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
    if (responder.getStatus() != HttpResponseStatus.OK) {
      throw new IOException("Failed to write to stream. Status = " + responder.getStatus());
    }
  }

  protected ApplicationWithPrograms deployApp(Class<?> appClass) throws Exception {
    return AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);
  }

  protected ApplicationWithPrograms deployApp(Class<?> appClass, Config config) throws Exception {
    return AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER, config);
  }

  protected ApplicationWithPrograms deployApp(Id.Namespace namespace, Class<?> appClass) throws Exception {
    return AppFabricTestHelper.deployApplicationWithManager(namespace, appClass, TEMP_FOLDER_SUPPLIER);
  }

  // returns true if the program ran successfully
  protected boolean runProgram(ApplicationWithPrograms app, Class<?> programClass, Arguments args) throws Exception {
    return waitForCompletion(AppFabricTestHelper.submit(app, programClass.getName(), args, TEMP_FOLDER_SUPPLIER));
  }

  /**
   * Returns a list of {@link Notification} object fetched from the data event topic in TMS that was published
   * starting from the given time.
   */
  protected List<Notification> getDataNotifications(long startTime) throws Exception {
    // Get data notifications from TMS
    List<Notification> notifications = new ArrayList<>();
    MessagingContext messagingContext = new MultiThreadMessagingContext(injector.getInstance(MessagingService.class));

    try (CloseableIterator<Message> messages = messagingContext.getMessageFetcher()
      .fetch(NamespaceId.SYSTEM.getNamespace(),
             injector.getInstance(CConfiguration.class).get(Constants.Dataset.DATA_EVENT_TOPIC), 10, startTime)) {

      while (messages.hasNext()) {
        notifications.add(GSON.fromJson(new String(messages.next().getPayload(), StandardCharsets.UTF_8),
                                        Notification.class));
      }
    }

    return notifications;
  }


  private boolean waitForCompletion(ProgramController controller) throws InterruptedException {
    final AtomicBoolean success = new AtomicBoolean(false);
    final CountDownLatch completion = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void completed() {
        success.set(true);
        completion.countDown();
      }

      @Override
      public void error(Throwable cause) {
        completion.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // MR tests can run for long time.
    completion.await(10, TimeUnit.MINUTES);
    return success.get();
  }
}
