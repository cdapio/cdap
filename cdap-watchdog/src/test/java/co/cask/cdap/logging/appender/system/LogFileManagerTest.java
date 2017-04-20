/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.system;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.DefaultDatasetManager;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.meta.FileMetaDataWriter;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class LogFileManagerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" + CDAPLogAppender.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);
    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NonCustomLocationUnitTestModule().getModule(),
      new TransactionModules().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testLogFileManager() throws Exception {
    int syncInterval = 1024 * 1024;
    long maxLifeTimeMs = 50;
    long maxFileSizeInBytes = 104857600;
    DatasetManager datasetManager = new DefaultDatasetManager(injector.getInstance(DatasetFramework.class),
                                                              NamespaceId.SYSTEM,
                                                              co.cask.cdap.common.service.RetryStrategies.noRetry());
    Transactional transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(injector.getInstance(DatasetFramework.class)),
        injector.getInstance(TransactionSystemClient.class),
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(datasetManager, transactional);
    LogFileManager logFileManager = new LogFileManager("700", "600", maxLifeTimeMs, maxFileSizeInBytes, syncInterval,
                                                       fileMetaDataWriter,
                                                       injector.getInstance(LocationFactory.class));
    LogPathIdentifier logPathIdentifier = new LogPathIdentifier("test", "testApp", "testFlow");
    long timestamp = System.currentTimeMillis();
    LogFileOutputStream outputStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
    LoggingEvent event1 =
      getLoggingEvent("co.cask.Test1",
                      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                      Level.ERROR , "test message 1");
    outputStream.append(event1);
    // we are doing this, instead of calling getLogFileOutputStream to avoid race, if test machine can be slow.
    Assert.assertNotNull((logFileManager.getActiveOutputStream(logPathIdentifier)));
    TimeUnit.MILLISECONDS.sleep(60);
    logFileManager.flush();
    // should be closed on flush, should return null
    Assert.assertNull((logFileManager.getActiveOutputStream(logPathIdentifier)));
    LogFileOutputStream newLogOutStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
    // make sure the new location we got is different
    Assert.assertNotEquals(outputStream.getLocation(), newLogOutStream.getLocation());
  }

  private LoggingEvent getLoggingEvent(String fqcn, Logger logger, Level level, String message) {
    return new LoggingEvent(fqcn, logger, level, message, null, null);
  }

}
