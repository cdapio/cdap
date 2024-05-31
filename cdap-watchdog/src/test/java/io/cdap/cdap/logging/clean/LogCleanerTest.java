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

package io.cdap.cdap.logging.clean;

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
import io.cdap.cdap.logging.appender.system.CDAPLogAppender;
import io.cdap.cdap.logging.appender.system.LogFileManager;
import io.cdap.cdap.logging.appender.system.LogPathIdentifier;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.meta.FileMetaDataReader;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class LogCleanerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

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
        new NonCustomLocationUnitTestModule(),
        new TransactionModules().getInMemoryModules(),
        new LocalLogAppenderModule(),
        new DataSetsModules().getInMemoryModules(),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new AuthorizationTestModule(),
        new AuthorizationEnforcementModule().getInMemoryModules(),
        new AuthenticationContextModules().getNoOpModule(),
        new StorageModule(),
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
    StoreDefinition.LogFileMetaStore.create(injector.getInstance(StructuredTableAdmin.class));
  }

  @AfterClass
  public static void cleanUp() {
    txManager.stopAndWait();
  }

  @Test
  @Parameters({
      "Delete all expired files, -5000, -4980, 1, 100, 50, 0, 0" ,
      "Delete all expired files less than batch size, -50000, -49800, 10, 100, 10, 11, 1",
      "No expired files to delete, 50000, 52000, 500, 100, 50, 0, 5",
      "Delete expired files leave remaining, -50000, 50000, 20000, 100, 50, 0, 3"
  })
  public void testLogFileCleanup(String description, int startTimeOffset, int endTimeOffset,
      int timeStep, int retentionDurationMs, int fileCleanupBatchSize,
      int expiredFileCount, int unexpiredFileCount) throws Exception {
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(transactionRunner);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    long currentTime = System.currentTimeMillis();
    LogPathIdentifier logPathIdentifier = new LogPathIdentifier("testNs",
        "testApp",
        UUID.randomUUID().toString());
    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(transactionRunner);
    Location dirLocation = locationFactory.create("logs");
    dirLocation.mkdirs();

    // Create logs files for the timestamps.
    for (int i = startTimeOffset; i <= endTimeOffset; i += timeStep) {
      long time = currentTime + i;
      Location location = getLocation(dirLocation, logPathIdentifier);
      location.mkdirs();
      location = location.append("test" + i + ".avro");
      location.createNew();
      fileMetaDataWriter.writeMetaData(logPathIdentifier, time, time, location);
    }

    // Run log cleaner.
    LogCleaner logCleaner = new LogCleaner(fileMetadataCleaner,
        locationFactory,
        dirLocation,
        retentionDurationMs,
        10,
        fileCleanupBatchSize);
    logCleaner.run();
    FileMetaDataReader fileMetaDataReader = injector.getInstance(FileMetaDataReader.class);

    // Assert on the count of expired and unexpired files remaining.
    Assert.assertEquals(expiredFileCount,
        fileMetaDataReader.listFiles(logPathIdentifier, 0, System.currentTimeMillis()).size());
    Assert.assertEquals(unexpiredFileCount,
        fileMetaDataReader.listFiles(logPathIdentifier, System.currentTimeMillis(), Long.MAX_VALUE).size());

    // Assert for log folder deletion.
    // If no files are expected to remain, then folder should be deleted.
    boolean folderExists = (expiredFileCount + unexpiredFileCount != 0);
    Location location = getLocation(dirLocation, logPathIdentifier);
    Assert.assertEquals(location.exists(), folderExists);
  }

  @Test
  @Parameters({
      "2, 3600000, 2",
      "10, 3600000, 10",
      "100, -1, 10"
  })
  public void testLogFolderCleanup(int folderCleanupBatchSize,
      long expectedDelayInMillis,
      int expectedDeleteCount) throws IOException, InterruptedException {
    // Create log folders and files
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(transactionRunner);
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location logFolder = locationFactory.create("logs");
    logFolder.mkdirs();

    List<String> children = Arrays.asList("/A/A", "/A/B", "/A/C", "/B/A", "/B/B", "/B/C", "/C/A", "/C/B", "/C/C");
    for (String child : children) {
      Location location = logFolder.append(child);
      location.mkdirs();
    }

    Thread.sleep(1000);
    logFolder.append("/B/B/test.log").createNew();

    // Run log folder cleaner
    LogCleaner logCleaner = new LogCleaner(fileMetadataCleaner,
        locationFactory,
        logFolder,
        500,
        folderCleanupBatchSize,
        10);

    // Assert for the returned response for delay in next schedule
    Assert.assertEquals("location size: " + logFolder.list().size(), expectedDelayInMillis, logCleaner.run());

    // Assert if folders exists
    children = Arrays.asList("/A", "/A/A", "/A/B", "/A/C", "/B/A", "/B/B", "/B/C", "/C", "/C/A", "/C/B", "/C/C");
    int deletedCount = 0;
    for (String child : children) {
      Location location = logFolder.append(child);
      if (child.equals("/B/B")) {
        Assert.assertTrue(location.append("test.log").exists());
      } else {
        if (!location.exists()) {
          deletedCount++;
        }
      }
    }

    // Assert for the deleted folder count
    Assert.assertEquals(expectedDeleteCount, deletedCount);
  }

  @Test
  public void testLogFolderCleanupExclusion() throws IOException, InterruptedException {
    // Create log folders and files
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(transactionRunner);
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location logFolder = locationFactory.create("logs" + System.currentTimeMillis());
    logFolder.mkdirs();

    for (int dayOffset = 5; dayOffset >= 3; dayOffset--) {
      createFolderWithDateFormat(logFolder, dayOffset);
    }

    Thread.sleep(5000);
    for (int dayOffset = 2; dayOffset >= 0; dayOffset--) {
      createFolderWithDateFormat(logFolder, dayOffset);
    }

    // Run log folder cleaner
    LogCleaner logCleaner = new LogCleaner(fileMetadataCleaner,
        locationFactory,
        logFolder,
        2000,
        10,
        10);
    logCleaner.run();

    // Assert for the remaining folder count
    Assert.assertEquals(3, logFolder.list().size());
  }

  private void createFolderWithDateFormat(Location logFolder, int dayOffset) throws IOException {
    long time = System.currentTimeMillis();
    String date = LogFileManager.formatLogDirectoryName(time - dayOffset * MILLIS_IN_DAY);
    Location location = logFolder.append(date);
    location.mkdirs();
    location = location.append("test_child_folder");
    location.mkdirs();
  }

  private Location getLocation(Location logsDirectoryLocation, LogPathIdentifier logPathIdentifier) throws IOException {
    long currentTime = System.currentTimeMillis();
    String date = LogFileManager.formatLogDirectoryName(currentTime);
    Location contextLocation =
        logsDirectoryLocation.append(logPathIdentifier.getNamespaceId())
            .append(date)
            .append(logPathIdentifier.getPathId1())
            .append(logPathIdentifier.getPathId2());
    return contextLocation;
  }
}
