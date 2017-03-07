/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.framework.local.LocalLogAppender;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.ReadRange;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.NoOpOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.Services;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LocalLogAppenderResilientTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testResilientLogging() throws Exception {
    Configuration hConf = new Configuration();
    CConfiguration cConf = CConfiguration.create();

    File datasetDir = new File(tmpFolder.newFolder(), "datasetUser");
    //noinspection ResultOfMethodCallIgnored
    datasetDir.mkdirs();

    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, "localhost");

    cConf.set(Constants.Dataset.Executor.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.Executor.PORT, Networks.getRandomPort());

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule().getModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new TransactionMetricsModule(),
      new ExploreClientModule(),
      new LoggingModules().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        }
      });

    TransactionManager txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    DatasetOpExecutorService opExecutorService = injector.getInstance(DatasetOpExecutorService.class);
    opExecutorService.startAndWait();

    // Start the logging before starting the service.
    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TRL_ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                       "RUN", "INSTANCE"));
    String logBaseDir = "trl-log/log_files_" + new Random(System.currentTimeMillis()).nextLong();

    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);
    cConf.setInt(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024);
    final LogAppender appender = injector.getInstance(LocalLogAppender.class);
    new LogAppenderInitializer(appender).initialize("TestResilientLogging");

    int failureMsgCount = 3;
    final CountDownLatch failureLatch = new CountDownLatch(failureMsgCount);
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    loggerContext.getStatusManager().add(new StatusListener() {
      @Override
      public void addStatusEvent(Status status) {
        if (status.getLevel() != Status.ERROR || status.getOrigin() != appender) {
          return;
        }
        Throwable cause = status.getThrowable();
        if (cause != null) {
          Throwable rootCause = Throwables.getRootCause(cause);
          if (rootCause instanceof ServiceUnavailableException) {
            String serviceName = ((ServiceUnavailableException) rootCause).getServiceName();
            if (Constants.Service.DATASET_MANAGER.equals(serviceName)) {
              failureLatch.countDown();
            }
          }
        }
      }
    });

    Logger logger = LoggerFactory.getLogger("TestResilientLogging");
    for (int i = 0; i < failureMsgCount; ++i) {
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    // Wait for the three failure to append to happen
    // The wait time has to be > 3 seconds because DatasetServiceClient has 1 second timeout on discovery
    failureLatch.await(5, TimeUnit.SECONDS);

    // Start dataset service, wait for it to be discoverable
    DatasetService dsService = injector.getInstance(DatasetService.class);
    dsService.startAndWait();

    final CountDownLatch startLatch = new CountDownLatch(1);
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    discoveryClient.discover(Constants.Service.DATASET_MANAGER).watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        if (!Iterables.isEmpty(serviceDiscovered)) {
          startLatch.countDown();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    startLatch.await(5, TimeUnit.SECONDS);

    // Do some more logging after the service is started.
    for (int i = 5; i < 10; ++i) {
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    appender.stop();

    // Verify - we should have at least 5 events.
    LoggingContext loggingContext = new FlowletLoggingContext("TRL_ACCT_1", "APP_1", "FLOW_1", "", "RUN", "INSTANCE");
    FileLogReader logTail = injector.getInstance(FileLogReader.class);
    LoggingTester.LogCallback logCallback1 = new LoggingTester.LogCallback();
    logTail.getLogPrev(loggingContext, ReadRange.LATEST, 10, Filter.EMPTY_FILTER,
                       logCallback1);
    List<LogEvent> allEvents = logCallback1.getEvents();
    Assert.assertTrue(allEvents.toString(), allEvents.size() >= 5);

    // Finally - stop all services
    Services.chainStop(dsService, opExecutorService, txManager);
  }
}
