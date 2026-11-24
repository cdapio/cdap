/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.system;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.logging.LoggingConfiguration;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.meta.FileMetaDataWriter;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

public class LogFileManagerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = new Configuration();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" + CDAPLogAppender.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);
    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NonCustomLocationUnitTestModule(),
      new TransactionModules().getInMemoryModules(),
      new LocalLogAppenderModule(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new StorageModule(),
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
    txManager.startAsync().awaitRunning();

    StoreDefinition.LogFileMetaStore.create(injector.getInstance(StructuredTableAdmin.class));
  }

  @AfterClass
  public static void cleanUp() {
    txManager.stopAsync().awaitTerminated();
  }

  @Test
  public void testLogFileManager() throws Exception {
    int syncInterval = 1024 * 1024;
    long maxLifeTimeMs = 50;
    long maxFileSizeInBytes = 104857600;
    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(injector.getInstance(TransactionRunner.class));
    LogFileManager logFileManager = new LogFileManager("700", "600", maxLifeTimeMs, maxFileSizeInBytes, syncInterval,
                                                       fileMetaDataWriter,
                                                       injector.getInstance(LocationFactory.class));
    LogPathIdentifier logPathIdentifier = new LogPathIdentifier("test", "testApp", "testFlow");
    long timestamp = System.currentTimeMillis();
    LogFileOutputStream outputStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
    LoggingEvent event1 =
      getLoggingEvent("io.cdap.Test1",
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

  @Test
  public void testMetadataWriteFailureClosesQuietlyAndDeletesFile() throws Exception {
    FileMetaDataWriter failingMetaWriter = Mockito.mock(FileMetaDataWriter.class);
    IOException metaError = new IOException("metadata write failed");
    Mockito.doThrow(metaError).when(failingMetaWriter)
        .writeMetaData(Mockito.any(LogPathIdentifier.class), Mockito.anyLong(),
            Mockito.anyLong(), Mockito.any(Location.class));

    LocalLocationFactory localLocationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    LogFileManager manager = new LogFileManager(
        "", "", 60000L, 1048576L, 1024, failingMetaWriter, localLocationFactory);
    LogPathIdentifier identifier = new LogPathIdentifier("ns1", "app1", "worker1");
    long now = System.currentTimeMillis();

    try {
      manager.getLogFileOutputStream(identifier, now);
      Assert.fail("Expected IOException when FileMetaDataWriter throws");
    } catch (IOException e) {
      Assert.assertSame(metaError, e.getCause());
    }

    Assert.assertNull(manager.getActiveOutputStream(identifier));
    Location contextDir = manager.getLogsDirectoryLocation()
        .append("ns1")
        .append(LogFileManager.formatLogDirectoryName(now))
        .append("app1")
        .append("worker1");
    if (contextDir.exists()) {
      Assert.assertTrue(contextDir.list().isEmpty());
    }
  }

  @Test
  public void testCloseQuietlySwallowsStreamCloseExceptionAndClosesRemaining() throws Exception {
    final AtomicBoolean firstClosed = new AtomicBoolean(false);
    final AtomicBoolean secondClosed = new AtomicBoolean(false);
    final LocalLocationFactory delegateFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    FileMetaDataWriter noOpMetaWriter = Mockito.mock(FileMetaDataWriter.class);
    LogFileManager manager = new LogFileManager(
        "", "", 60000L, 1048576L, 1024, noOpMetaWriter, delegateFactory);

    LogPathIdentifier id1 = new LogPathIdentifier("ns1", "app1", "prog1");
    LogPathIdentifier id2 = new LogPathIdentifier("ns1", "app2", "prog2");

    // Open two real streams, then wrap the first location's underlying stream via a failing closeable
    LogFileOutputStream stream1 = manager.getLogFileOutputStream(id1, System.currentTimeMillis());
    LogFileOutputStream stream2 = manager.getLogFileOutputStream(id2, System.currentTimeMillis());
    Assert.assertNotNull(stream1);
    Assert.assertNotNull(stream2);

    // Replace active map entries via close behavior: verify close() clears all active streams quietly
    // even if a stream was already closed or throws on close.
    OutputStream rawOut = new FilterOutputStream(delegateFactory.create("failing.avro").getOutputStream()) {
      @Override
      public void close() throws IOException {
        firstClosed.set(true);
        super.close();
        throw new IOException("simulated close failure");
      }
    };
    Location failingLoc = Mockito.mock(Location.class);
    Mockito.when(failingLoc.getOutputStream()).thenReturn(rawOut);
    LogFileOutputStream failingLogStream = new LogFileOutputStream(
        failingLoc, "", 1024, System.currentTimeMillis(), () -> secondClosed.set(true));

    java.lang.reflect.Field mapField = LogFileManager.class.getDeclaredField("outputStreamMap");
    mapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<LogPathIdentifier, LogFileOutputStream> streamMap =
        (java.util.Map<LogPathIdentifier, LogFileOutputStream>) mapField.get(manager);
    streamMap.put(new LogPathIdentifier("ns1", "failingApp", "failingProg"), failingLogStream);

    // Must not throw IOException even though failingLogStream.close() throws IOException
    manager.close();

    Assert.assertTrue(firstClosed.get());
    Assert.assertTrue(secondClosed.get());
    Assert.assertNull(manager.getActiveOutputStream(id1));
    Assert.assertNull(manager.getActiveOutputStream(id2));
  }

  private LoggingEvent getLoggingEvent(String fqcn, Logger logger, Level level, String message) {
    return new LoggingEvent(fqcn, logger, level, message, null, null);
  }

}
