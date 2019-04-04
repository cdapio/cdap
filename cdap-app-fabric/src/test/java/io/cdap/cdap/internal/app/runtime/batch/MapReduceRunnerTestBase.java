/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Injector;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DynamicDatasetCache;
import io.cdap.cdap.data2.dataset2.SingleThreadDatasetCache;
import io.cdap.cdap.data2.transaction.TransactionExecutorFactory;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.DefaultId;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.XSlowTests;
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
    injector = AppFabricTestHelper.getInjector(conf);
    txService = injector.getInstance(TransactionManager.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = injector.getInstance(DatasetFramework.class);
    datasetCache = new SingleThreadDatasetCache(
      new SystemDatasetInstantiator(dsFramework, MapReduceRunnerTestBase.class.getClassLoader(), null),
      injector.getInstance(TransactionSystemClient.class),
      NamespaceId.DEFAULT, DatasetDefinition.NO_ARGUMENTS, null, null);

    metricStore = injector.getInstance(MetricStore.class);
    txService.startAndWait();

    // Always create the default namespace
    injector.getInstance(NamespaceAdmin.class).create(NamespaceMeta.DEFAULT);
  }

  @AfterClass
  public static void afterClass() {
    txService.stopAndWait();
    AppFabricTestHelper.shutdown();
  }

  @After
  public void after() throws Exception {
    // cleanup user data (only user datasets)
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(DefaultId.NAMESPACE)) {
      dsFramework.deleteInstance(DefaultId.NAMESPACE.dataset(spec.getName()));
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
