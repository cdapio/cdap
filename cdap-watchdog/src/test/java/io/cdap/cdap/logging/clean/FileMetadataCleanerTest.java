/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
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
import io.cdap.cdap.logging.appender.system.LogPathIdentifier;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.meta.FileMetaDataReader;
import io.cdap.cdap.logging.meta.FileMetaDataWriter;
import io.cdap.cdap.logging.write.LogLocation;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileMetadataCleanerTest {
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
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.LogFileMetaStore.createTables(injector.getInstance(StructuredTableAdmin.class), false);
  }

  @AfterClass
  public static void cleanUp() {
    txManager.stopAndWait();
  }

  @Test
  @Ignore
  // TODO CDAP-14953 ignoring this test until this jira fixed
  public void testScanAndDeleteNewMetadata() throws Exception {
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(transactionRunner);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(transactionRunner);
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
        fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 100);
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
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 100);
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
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 100);
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
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 100);
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
      deletedEntries = fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 100);
      Assert.assertEquals(0, deletedEntries.size());
    } finally {
      // cleanup meta
      deleteAllMetaEntries(transactionRunner);
    }
  }


  @Test
  public void testFileMetadataWithCommonContextPrefix() throws Exception {
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(transactionRunner);
    FileMetaDataReader fileMetadataReader = injector.getInstance(FileMetaDataReader.class);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(transactionRunner);
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
        fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 100);
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
      deleteAllMetaEntries(transactionRunner);
    }
  }

  @Test
  public void testWithBatchSizeLargerThanNumOfFiles() throws Exception {
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);

    FileMetaDataWriter fileMetaDataWriter = new FileMetaDataWriter(transactionRunner);
    FileMetaDataReader fileMetadataReader = injector.getInstance(FileMetaDataReader.class);
    FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(transactionRunner);
    try {
      LogPathIdentifier identifier = new LogPathIdentifier(NamespaceId.DEFAULT.getNamespace(),
                                                           "testApp", String.format("testFlow%s", 0));

      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      Location location = locationFactory.create(TMP_FOLDER.newFolder().getPath()).append("/logs");
      long currentTime = System.currentTimeMillis();
      long newCurrentTime = currentTime + 100;

      for (int j = 0; j < 10; j++) {
        fileMetaDataWriter.writeMetaData(identifier, newCurrentTime + j, newCurrentTime + j,
                                         location.append("testFileNew" + Integer.toString(j)));
      }

      List<LogLocation> locations;
      locations = fileMetadataReader.listFiles(identifier, newCurrentTime, newCurrentTime + 10);
      // should include files from currentTime (0..9)
      Assert.assertEquals(10, locations.size());

      long tillTime = newCurrentTime + 4;
      List<FileMetadataCleaner.DeletedEntry> deleteEntries =
        fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, 1000);
      Assert.assertEquals(5, deleteEntries.size());
    } finally {
      // cleanup meta
      deleteAllMetaEntries(transactionRunner);
    }
  }

  private boolean deleteAllMetaEntries(TransactionRunner transactionRunner) {
    final List<DeletedEntry> deletedEntries = new ArrayList<>();
    try {
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(StoreDefinition.LogFileMetaStore.LOG_FILE_META);
        try (CloseableIterator<StructuredRow> iter = table.scan(Range.all(), Integer.MAX_VALUE)) {
          while (iter.hasNext()) {
            StructuredRow row = iter.next();
            String loggingContext = row.getString(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD);
            long eventTime = row.getLong(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD);
            long creationTime = row.getLong(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD);
            table.delete(Arrays.asList(Fields.stringField(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD,
                                                          loggingContext),
                                       Fields.longField(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD,
                                                        eventTime),
                                       Fields.longField(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD,
                                                        creationTime)));
            deletedEntries.add(new DeletedEntry(loggingContext, eventTime, creationTime));
          }
        }
      }, IOException.class);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  private static final class DeletedEntry {
    private String identifier;
    private long eventTime;
    private long creationTime;

    private DeletedEntry(String identifier, long eventTime, long creationTime) {
      this.identifier = identifier;
      this.eventTime = eventTime;
      this.creationTime = creationTime;
    }
  }
}
