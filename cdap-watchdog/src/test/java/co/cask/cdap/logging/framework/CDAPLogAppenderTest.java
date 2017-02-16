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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.meta.FileMetaDataReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CDAPLogAppenderTest {
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
  public void testCDAPLogAppender() throws Exception {
    int syncInterval = 1024 * 1024;
    CDAPLogAppender cdapLogAppender = new CDAPLogAppender();

    cdapLogAppender.setSyncIntervalBytes(syncInterval);
    cdapLogAppender.setMaxFileLifetimeMs(TimeUnit.DAYS.toMillis(1));
    cdapLogAppender.setMaxFileSizeInBytes(104857600);
    cdapLogAppender.setDirPermissions("700");
    cdapLogAppender.setFilePermissions("600");
    cdapLogAppender.setFileRetentionDurationDays(1);
    cdapLogAppender.setLogCleanupIntervalMins(10);
    cdapLogAppender.setFileCleanupTransactionTimeout(30);
    AppenderContext context = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                       injector.getInstance(TransactionSystemClient.class),
                                                       injector.getInstance(LocationFactory.class),
                                                       new NoOpMetricsCollectionService());
    context.start();
    cdapLogAppender.setContext(context);
    cdapLogAppender.start();

    FileMetaDataReader fileMetaDataReader = injector.getInstance(FileMetaDataReader.class);
    LoggingEvent event =
      new LoggingEvent("co.cask.Test",
                       (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                       Level.ERROR , "test message", null, null);
    Map<String, String> properties = new HashMap<>();
    properties.put(NamespaceLoggingContext.TAG_NAMESPACE_ID, "default");
    properties.put(ApplicationLoggingContext.TAG_APPLICATION_ID, "testApp");
    properties.put(FlowletLoggingContext.TAG_FLOW_ID, "testFlow");
    properties.put(FlowletLoggingContext.TAG_FLOWLET_ID, "testFlowlet");

    event.setMDCPropertyMap(properties);

    cdapLogAppender.doAppend(event);
    cdapLogAppender.stop();
    context.stop();

    try {
      List<LogLocation> files = fileMetaDataReader.listFiles(cdapLogAppender.getLoggingPath(properties),
                                                             0, Long.MAX_VALUE);
      Assert.assertEquals(1, files.size());
      LogLocation logLocation = files.get(0);
      Assert.assertEquals(LogLocation.VERSION_1, logLocation.getFrameworkVersion());
      Assert.assertTrue(logLocation.getLocation().exists());
      CloseableIterator<LogEvent> logEventCloseableIterator =
        logLocation.readLog(Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
      int logCount = 0;
      while (logEventCloseableIterator.hasNext()) {
        logCount++;
        LogEvent logEvent = logEventCloseableIterator.next();
        Assert.assertEquals(event.getMessage(), logEvent.getLoggingEvent().getMessage());
      }
      logEventCloseableIterator.close();
      Assert.assertEquals(1, logCount);
      // checking permission
      String expectedPermissions = "rw-------";
      for (LogLocation file : files) {
        Location location = file.getLocation();
        Assert.assertEquals(expectedPermissions, location.getPermissions());
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testCDAPLogAppenderRotation() throws Exception {
    int syncInterval = 1024 * 1024;
    FileMetaDataReader fileMetaDataReader = injector.getInstance(FileMetaDataReader.class);
    CDAPLogAppender cdapLogAppender = new CDAPLogAppender();
    AppenderContext context = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                       injector.getInstance(TransactionSystemClient.class),
                                                       injector.getInstance(LocationFactory.class),
                                                       new NoOpMetricsCollectionService());
    context.start();

    cdapLogAppender.setSyncIntervalBytes(syncInterval);
    cdapLogAppender.setMaxFileLifetimeMs(500);
    cdapLogAppender.setMaxFileSizeInBytes(104857600);
    cdapLogAppender.setDirPermissions("750");
    cdapLogAppender.setFilePermissions("640");
    cdapLogAppender.setFileRetentionDurationDays(1);
    cdapLogAppender.setLogCleanupIntervalMins(10);
    cdapLogAppender.setFileCleanupTransactionTimeout(30);
    cdapLogAppender.setContext(context);
    cdapLogAppender.start();

    Map<String, String> properties = new HashMap<>();
    properties.put(NamespaceLoggingContext.TAG_NAMESPACE_ID, "testTimeRotation");
    properties.put(ApplicationLoggingContext.TAG_APPLICATION_ID, "testApp");
    properties.put(FlowletLoggingContext.TAG_FLOW_ID, "testFlow");
    properties.put(FlowletLoggingContext.TAG_FLOWLET_ID, "testFlowlet");

    long currentTimeMillisEvent1 = System.currentTimeMillis();

    LoggingEvent event1 =
      getLoggingEvent("co.cask.Test1",
                      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                      Level.ERROR , "test message 1", properties);

    event1.setTimeStamp(currentTimeMillisEvent1);
    cdapLogAppender.doAppend(event1);

    // Pause pass the max file lifetime ms
    TimeUnit.MILLISECONDS.sleep(500);

    long currentTimeMillisEvent2 = System.currentTimeMillis();

    LoggingEvent event2 = getLoggingEvent("co.cask.Test2",
                                          (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
                                            Logger.ROOT_LOGGER_NAME), Level.ERROR , "test message 2", properties);
    event2.setTimeStamp(currentTimeMillisEvent1 + 1000);
    cdapLogAppender.doAppend(event2);
    cdapLogAppender.stop();
    context.stop();

    try {
      List<LogLocation> files = fileMetaDataReader.listFiles(cdapLogAppender.getLoggingPath(properties),
                                                             0, Long.MAX_VALUE);
      Assert.assertEquals(2, files.size());
      assertLogEventDetails(event1, files.get(0));
      assertLogEventDetails(event2, files.get(1));
      Assert.assertEquals(currentTimeMillisEvent1, files.get(0).getEventTimeMs());
      Assert.assertEquals(currentTimeMillisEvent1 + 1000, files.get(1).getEventTimeMs());
      Assert.assertTrue(files.get(0).getFileCreationTimeMs() >= currentTimeMillisEvent1);
      Assert.assertTrue(files.get(1).getFileCreationTimeMs() >= currentTimeMillisEvent2);

      // checking permission
      String expectedPermissions = "rw-r-----";
      for (LogLocation file : files) {
        Location location = file.getLocation();
        Assert.assertEquals(expectedPermissions, location.getPermissions());
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testCDAPLogAppenderSizeBasedRotation() throws Exception {
    int syncInterval = 1024 * 1024;
    FileMetaDataReader fileMetaDataReader = injector.getInstance(FileMetaDataReader.class);
    CDAPLogAppender cdapLogAppender = new CDAPLogAppender();
    AppenderContext context = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                       injector.getInstance(TransactionSystemClient.class),
                                                       injector.getInstance(LocationFactory.class),
                                                       new NoOpMetricsCollectionService());
    context.start();

    cdapLogAppender.setSyncIntervalBytes(syncInterval);
    cdapLogAppender.setMaxFileLifetimeMs(TimeUnit.DAYS.toMillis(1));
    cdapLogAppender.setMaxFileSizeInBytes(500);
    cdapLogAppender.setDirPermissions("750");
    cdapLogAppender.setFilePermissions("640");
    cdapLogAppender.setFileRetentionDurationDays(1);
    cdapLogAppender.setLogCleanupIntervalMins(10);
    cdapLogAppender.setFileCleanupTransactionTimeout(30);
    cdapLogAppender.setContext(context);
    cdapLogAppender.start();

    Map<String, String> properties = new HashMap<>();
    properties.put(NamespaceLoggingContext.TAG_NAMESPACE_ID, "testSizeRotation");
    properties.put(ApplicationLoggingContext.TAG_APPLICATION_ID, "testApp");
    properties.put(FlowletLoggingContext.TAG_FLOW_ID, "testFlow");
    properties.put(FlowletLoggingContext.TAG_FLOWLET_ID, "testFlowlet");

    long currentTimeMillisEvent1 = System.currentTimeMillis();

    LoggingEvent event1 =
      getLoggingEvent("co.cask.Test1",
                      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                      Level.ERROR , "test message 1", properties);

    event1.setTimeStamp(currentTimeMillisEvent1);
    cdapLogAppender.doAppend(event1);
    // sync updates the file size
    cdapLogAppender.sync();

    long currentTimeMillisEvent2 = System.currentTimeMillis();
    LoggingEvent event2 = getLoggingEvent("co.cask.Test2",
                                          (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
                                            Logger.ROOT_LOGGER_NAME), Level.ERROR , "test message 2", properties);
    event2.setTimeStamp(currentTimeMillisEvent2);
    // one new append, we will rotate to new file as the file size limit is very low and last append exceeded that.
    cdapLogAppender.doAppend(event2);
    cdapLogAppender.stop();
    context.stop();

    try {
      List<LogLocation> files = fileMetaDataReader.listFiles(cdapLogAppender.getLoggingPath(properties),
                                                             0, Long.MAX_VALUE);
      Assert.assertEquals(2, files.size());
      assertLogEventDetails(event1, files.get(0));
      assertLogEventDetails(event2, files.get(1));
      Assert.assertEquals(currentTimeMillisEvent1, files.get(0).getEventTimeMs());
      Assert.assertEquals(currentTimeMillisEvent2, files.get(1).getEventTimeMs());
      Assert.assertTrue(files.get(0).getFileCreationTimeMs() >= currentTimeMillisEvent1);
      Assert.assertTrue(files.get(1).getFileCreationTimeMs() >= currentTimeMillisEvent2);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private void assertLogEventDetails(LoggingEvent expectedLoggingEvent, LogLocation logLocation) throws IOException {
    Assert.assertEquals(LogLocation.VERSION_1, logLocation.getFrameworkVersion());
    Assert.assertTrue(logLocation.getLocation().exists());
    CloseableIterator<LogEvent> logEventCloseableIterator =
      logLocation.readLog(Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    int logCount = 0;
    while (logEventCloseableIterator.hasNext()) {
      logCount++;
      LogEvent logEvent = logEventCloseableIterator.next();
      Assert.assertEquals(expectedLoggingEvent.getMessage(), logEvent.getLoggingEvent().getMessage());
    }
    logEventCloseableIterator.close();
    Assert.assertEquals(1, logCount);
  }

  private LoggingEvent getLoggingEvent(String fqcn, Logger logger, Level level, String message,
                                       Map<String, String> mdcMap) {
    LoggingEvent event =
      new LoggingEvent(fqcn, logger, level , message, null, null);
    event.setMDCPropertyMap(mdcMap);
    return event;
  }
}
