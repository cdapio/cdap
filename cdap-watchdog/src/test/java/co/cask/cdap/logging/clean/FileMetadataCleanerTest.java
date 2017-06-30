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

package co.cask.cdap.logging.clean;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.logging.LoggingContext;
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
import co.cask.cdap.logging.appender.system.CDAPLogAppender;
import co.cask.cdap.logging.appender.system.LogPathIdentifier;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.meta.FileMetaDataReader;
import co.cask.cdap.logging.meta.FileMetaDataWriter;
import co.cask.cdap.logging.meta.LoggingStoreTableUtil;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FileMetadataCleanerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final int CUTOFF_TIME_TRANSACTION = 50;
  private static final int TRANSACTION_TIMEOUT = 60;

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
  public void testScanAndDeleteOldMetadata() throws Exception {
    // use file meta data manager to write meta data in old format
    // use file meta writer to write meta data in new format
    // scan for old files and make sure we only get the old meta data entries.
    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    DatasetManager datasetManager = new DefaultDatasetManager(datasetFramework, NamespaceId.SYSTEM,
                                                              co.cask.cdap.common.service.RetryStrategies.noRetry(),
                                                              null);
    Transactional transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), injector.getInstance(TransactionSystemClient.class),
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(datasetManager, transactional);
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    LoggingContext flowContext =
      LoggingContextHelper.getLoggingContext("testNs", "testApp", "testFlow", ProgramType.FLOW);
    long eventTimestamp = System.currentTimeMillis();
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Location testLocation = locationFactory.create("testFile");
    try {
      // write 50 entries in old format
      for (int i = 0; i < 50; i++) {
        fileMetaDataManager.writeMetaData(flowContext, eventTimestamp + i, testLocation);
      }

      LoggingContext wflowContext =
        LoggingContextHelper.getLoggingContext("testNs", "testApp", "testWflow", ProgramType.WORKFLOW);
      fileMetaDataManager.writeMetaData(wflowContext, eventTimestamp, testLocation);

      LoggingContext mrContext =
        LoggingContextHelper.getLoggingContext("testNs", "testApp", "testMR", ProgramType.MAPREDUCE);
      fileMetaDataManager.writeMetaData(mrContext, eventTimestamp, testLocation);

      LoggingContext sparkContext =
        LoggingContextHelper.getLoggingContext("testNs", "testApp", "testSpark", ProgramType.SPARK);
      fileMetaDataManager.writeMetaData(sparkContext, eventTimestamp, testLocation);

      // write 50 entries in new format
      long newEventTime = eventTimestamp + 1000;
      long currentTime = newEventTime + 1000;
      LogPathIdentifier logPathIdentifier = new LogPathIdentifier("testNs", "testApp", "testFlow");

      for (int i = 50; i < 100; i++) {
        fileMetaDataWriter.writeMetaData(logPathIdentifier, newEventTime + i, currentTime + i, testLocation);
      }

      FileMetaDataReader fileMetaDataReader = injector.getInstance(FileMetaDataReader.class);
      Assert.assertEquals(50, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(flowContext),
                                                          eventTimestamp - 1, eventTimestamp + 100).size());
      Assert.assertEquals(1, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(wflowContext),
                                                           eventTimestamp - 1, eventTimestamp + 100).size());
      Assert.assertEquals(1, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(mrContext),
                                                           eventTimestamp - 1, eventTimestamp + 100).size());
      Assert.assertEquals(1, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(sparkContext),
                                                          eventTimestamp - 1, eventTimestamp + 100).size());
      FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(datasetManager, transactional);
        fileMetadataCleaner.scanAndDeleteOldMetaData(TRANSACTION_TIMEOUT, CUTOFF_TIME_TRANSACTION);
      // deleted all old metadata
      Assert.assertEquals(0, fileMetaDataReader.listFiles(logPathIdentifier,
                                                           eventTimestamp - 1, eventTimestamp + 100).size());
      Assert.assertEquals(0, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(wflowContext),
                                                          eventTimestamp - 1, eventTimestamp + 100).size());
      Assert.assertEquals(0, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(mrContext),
                                                          eventTimestamp - 1, eventTimestamp + 100).size());
      Assert.assertEquals(0, fileMetaDataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(sparkContext),
                                                          eventTimestamp - 1, eventTimestamp + 100).size());

    } finally {
      // cleanup meta
      cleanupMetadata(transactional, datasetManager);
    }
  }

  boolean deleteAllMetaEntries(Transactional transactional, int timeout, final DatasetManager datasetManager,
                               final byte[] startKey, final byte[] stopKey) {
    final List<byte[]> deletedEntries = new ArrayList<>();
    try {
      transactional.execute(timeout, new TxRunnable() {
        public void run(DatasetContext context) throws Exception {
          Stopwatch stopwatch = new Stopwatch();
          stopwatch.start();
          Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
          // create range with tillTime as endColumn
          Scan scan = new Scan(startKey, stopKey);
          try (Scanner scanner = table.scan(scan)) {
            Row row;
            while ((row = scanner.next()) != null) {
              if (stopwatch.elapsedTime(TimeUnit.SECONDS) > CUTOFF_TIME_TRANSACTION) {
                break;
              }
              byte[] rowKey = row.getRow();
              // delete all columns for this row
              table.delete(rowKey);
              deletedEntries.add(rowKey);
            }
          }
        }
      });
    } catch (TransactionFailureException e) {
      return false;
    }
    return true;
  }

  private void cleanupMetadata(Transactional transactional, DatasetManager datasetManager) {
    // cleanup meta
    deleteAllMetaEntries(transactional, 30, datasetManager, LoggingStoreTableUtil.OLD_FILE_META_ROW_KEY_PREFIX,
                         Bytes.stopKeyForPrefix(LoggingStoreTableUtil.OLD_FILE_META_ROW_KEY_PREFIX));
    deleteAllMetaEntries(transactional, 30, datasetManager, LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX,
                         Bytes.stopKeyForPrefix(LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX));
  }

  @Test
  public void testScanAndDeleteNewMetadata() throws Exception {
    // use file meta data manager to write meta data in old format
    // use file meta writer to write meta data in new format
    // scan for old files and make sure we only get the old meta data entries.
    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    DatasetManager datasetManager = new DefaultDatasetManager(datasetFramework, NamespaceId.SYSTEM,
                                                              co.cask.cdap.common.service.RetryStrategies.noRetry(),
                                                              null);
    Transactional transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), injector.getInstance(TransactionSystemClient.class),
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(datasetManager, transactional);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(datasetManager, transactional);
    try {
      long currentTime = System.currentTimeMillis();
      long eventTimestamp = currentTime - 100;
      LogPathIdentifier logPathIdentifier = new LogPathIdentifier("testNs2", "testApp", "testFlow");
      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      List<String> expected = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        Location location = locationFactory.create("testFlowFile" + i);
        // values : event time is 100ms behind current timestamp
        fileMetaDataWriter.writeMetaData(logPathIdentifier, eventTimestamp + i, currentTime + i, location);
        expected.add(location.toURI().getPath());
      }

      long tillTime = currentTime + 50;
      List<FileMetadataCleaner.DeletedEntry> deletedEntries =
        fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, TRANSACTION_TIMEOUT);
      // we should have deleted 51 rows, till time is inclusive
      Assert.assertEquals(51, deletedEntries.size());
      int count = 0;
      for (FileMetadataCleaner.DeletedEntry deletedEntry : deletedEntries) {
        Assert.assertEquals(expected.get(count), deletedEntry.getPath());
        count += 1;
      }
      // now add 10 entries for spark
      logPathIdentifier = new LogPathIdentifier("testNs2", "testApp", "testSpark");
      expected = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
        Location location = locationFactory.create("testSparkFile" + i);
        // values : event time is 100ms behind current timestamp
        fileMetaDataWriter.writeMetaData(logPathIdentifier, eventTimestamp + i, currentTime + i, location);
        expected.add(location.toURI().getPath());
      }

      // lets keep the same till time - this should only delete the spark entries now
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, TRANSACTION_TIMEOUT);
      // we should have deleted 51 rows, till time is inclusive
      Assert.assertEquals(10, deletedEntries.size());
      count = 0;
      for (FileMetadataCleaner.DeletedEntry deletedEntry : deletedEntries) {
        Assert.assertEquals(expected.get(count), deletedEntry.getPath());
        count += 1;
      }

      // now add 10 entries in mr context in time range 60-70
      logPathIdentifier = new LogPathIdentifier("testNs2", "testApp", "testMr");
      expected = new ArrayList<>();

      // flow should come up at the beginning in the expected list
      for (int i = 51; i <= 70; i++) {
        expected.add(locationFactory.create("testFlowFile" + i).toURI().getPath());
      }

      for (int i = 0; i < 10; i++) {
        Location location = locationFactory.create("testMrFile" + i);
        // values : event time is 100ms behind current timestamp
        fileMetaDataWriter.writeMetaData(logPathIdentifier, eventTimestamp + i, currentTime + i, location);
        expected.add(location.toURI().getPath());
      }

      List<String> nextExpected = new ArrayList<>();
      logPathIdentifier = new LogPathIdentifier("testNs2", "testApp", "testCustomAction");
      for (int i = 90; i < 100; i++) {
        Location location = locationFactory.create("testActionFile" + i);
        // values : event time is 100ms behind current timestamp
        fileMetaDataWriter.writeMetaData(logPathIdentifier, eventTimestamp + i, currentTime + i, location);
        nextExpected.add(location.toURI().getPath());
      }

      tillTime = currentTime + 70;
      // lets delete till 70.
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, TRANSACTION_TIMEOUT);
      // we should have deleted 51-70 files of flow and 0-9 files of spark files in that order and 0 files of action.
      Assert.assertEquals(30, deletedEntries.size());
      count = 0;
      for (FileMetadataCleaner.DeletedEntry deletedEntry : deletedEntries) {
        Assert.assertEquals(expected.get(count), deletedEntry.getPath());
        count += 1;
      }

      // now delete till currentTime + 100, this should delete all remaining entries.
      // custom action should come first and then flow entries

      tillTime = currentTime + 100;
      // lets delete till 100.
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, TRANSACTION_TIMEOUT);
      // we should have deleted 90-99 of custom action(10) 71-99 (29) files of flow.
      for (int i = 71; i < 100; i++) {
        nextExpected.add(locationFactory.create("testFlowFile" + i).toURI().getPath());
      }
      Assert.assertEquals(39, deletedEntries.size());
      count = 0;
      for (FileMetadataCleaner.DeletedEntry deletedEntry : deletedEntries) {
        Assert.assertEquals(nextExpected.get(count), deletedEntry.getPath());
        count += 1;
      }

      // now lets do a delete with till time  = currentTime + 1000, this should return empty result
      tillTime = currentTime + 1000;
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, TRANSACTION_TIMEOUT);
      Assert.assertEquals(0, deletedEntries.size());
    } finally {
      // cleanup meta
      cleanupMetadata(transactional, datasetManager);
    }
  }


  @Test
  public void testFileMetadataWithCommonContextPrefix() throws Exception {
    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    DatasetManager datasetManager = new DefaultDatasetManager(datasetFramework, NamespaceId.SYSTEM,
                                                              co.cask.cdap.common.service.RetryStrategies.noRetry(),
                                                              null);
    Transactional transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), injector.getInstance(TransactionSystemClient.class),
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(datasetManager, transactional);
    FileMetaDataReader fileMetadataReader = injector.getInstance(FileMetaDataReader.class);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(datasetManager, transactional);
    try {
      List<LogPathIdentifier> logPathIdentifiers = new ArrayList<>();
      // we write entries where program id is of format testFlow{1..20},
      // this should be able to scan and delete common prefix programs like testFlow1, testFlow10 during clenaup.
      for (int i = 1; i <= 20; i++) {
        logPathIdentifiers.add(new LogPathIdentifier(NamespaceId.DEFAULT.getNamespace(),
                                                     "testApp", String.format("testFlow%s", i)));
      }

      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      Location location = locationFactory.create(TMP_FOLDER.newFolder().getPath()).append("/logs");
      long currentTime = System.currentTimeMillis();
      long newCurrentTime = currentTime + 100;

      for (int i = 1; i <= 20; i++) {
        LogPathIdentifier identifier = logPathIdentifiers.get(i - 1);
        for (int j = 0; j < 10; j++) {
          fileMetaDataWriter.writeMetaData(identifier, newCurrentTime + j, newCurrentTime + j,
                                           location.append("testFileNew" + Integer.toString(j)));
        }
      }

      List<LogLocation> locations;
      for (int i = 1; i <= 20; i++) {
        locations = fileMetadataReader.listFiles(logPathIdentifiers.get(i - 1),
                                                 newCurrentTime, newCurrentTime + 10);
        // should include files from currentTime (0..9)
        Assert.assertEquals(10, locations.size());
      }

      long tillTime = newCurrentTime + 4;
      List<FileMetadataCleaner.DeletedEntry> deleteEntries =
        fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, TRANSACTION_TIMEOUT);
      // 20 context, 5 entries each
      Assert.assertEquals(100, deleteEntries.size());
      for (int i = 1; i <= 20; i++) {
        locations = fileMetadataReader.listFiles(logPathIdentifiers.get(i - 1),
                                                 newCurrentTime, newCurrentTime + 10);
        // should include files from time (5..9)
        Assert.assertEquals(5, locations.size());
        int startIndex = 5;
        for (LogLocation logLocation : locations) {
          Assert.assertEquals(String.format("testFileNew%s", startIndex), logLocation.getLocation().getName());
          startIndex++;
        }
      }
    } finally {
      // cleanup meta
      cleanupMetadata(transactional, datasetManager);
    }
  }
}
