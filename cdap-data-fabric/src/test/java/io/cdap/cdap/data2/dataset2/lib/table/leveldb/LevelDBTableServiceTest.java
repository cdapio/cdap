/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.leveldb;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.test.TestRunner;
import io.cdap.cdap.data.runtime.DataFabricLevelDBModule;
import io.cdap.cdap.data.runtime.TransactionMetricsModule;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.SnapshotImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

/**
 *
 */
@RunWith(TestRunner.class)
public class LevelDBTableServiceTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  static LevelDBTableService service;
  static Injector injector;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
        new ConfigModule(conf),
        new NonCustomLocationUnitTestModule(),
        new InMemoryDiscoveryModule(),
        new DataFabricLevelDBModule(),
        new TransactionMetricsModule(),
        new AuthorizationTestModule(),
        new AuthorizationEnforcementModule().getStandaloneModules(),
        new AuthenticationContextModules().getMasterModule());
    service = injector.getInstance(LevelDBTableService.class);
  }

  @Test
  public void testGetTableStats() throws Exception {
    String table1 = "cdap_default.table1";
    String table2 = "cdap_default.table2";
    TableId tableId1 = TableId.from("default", "table1");
    TableId tableId2 = TableId.from("default", "table2");
    Assert.assertNull(service.getTableStats().get(tableId1));
    Assert.assertNull(service.getTableStats().get(tableId2));

    service.ensureTableExists(table1);
    service.ensureTableExists(table2);
    // We sleep to allow ops flush out to disk
    TimeUnit.SECONDS.sleep(1);

    // NOTE: empty table may take non-zero disk space: it stores some meta files as well
    Assert.assertNotNull(service.getTableStats().get(tableId1));
    long table1Size = service.getTableStats().get(tableId1).getDiskSizeBytes();
    Assert.assertNotNull(service.getTableStats().get(tableId2));
    long table2Size = service.getTableStats().get(tableId2).getDiskSizeBytes();

    writeSome(service, table1, 4096, 1024, true);
    TimeUnit.SECONDS.sleep(1);

    long table1SizeUpdated = service.getTableStats().get(tableId1).getDiskSizeBytes();
    Assert.assertTrue(table1SizeUpdated > table1Size);
    table1Size = table1SizeUpdated;
    Assert.assertEquals(table2Size, service.getTableStats().get(tableId2).getDiskSizeBytes());

    writeSome(service, table1, 4096, 1024, true);
    writeSome(service, table2, 4096, 1024, true);
    TimeUnit.SECONDS.sleep(1);

    Assert.assertTrue(service.getTableStats().get(tableId1).getDiskSizeBytes() > table1Size);
    long table2SizeUpdated = service.getTableStats().get(tableId2).getDiskSizeBytes();
    Assert.assertTrue(table2SizeUpdated > table2Size);
    table2Size = table2SizeUpdated;

    service.dropTable(table1);
    TimeUnit.SECONDS.sleep(1);

    Assert.assertNull(service.getTableStats().get(tableId1));
    Assert.assertEquals(table2Size, service.getTableStats().get(tableId2).getDiskSizeBytes());
  }

  @Test
  public void testCompression() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    // Write compressible data to table with compression disabled, then record on-disk size to be used later to
    // compare with that with compression enabled.
    LevelDBTableService compressedTableService = LevelDBTableService.getInstance();
    cConf.setBoolean(Constants.CFG_DATA_LEVELDB_COMPRESSION_ENABLED, false);
    compressedTableService.setConfiguration(cConf);
    String tableUncompressed = "cdap_default.tableUncompressed";
    TableId tableUncompressedID = TableId.from("default", "tableUncompressed");
    compressedTableService.ensureTableExists(tableUncompressed);
    // Write large enough number of rows to ensure some data are flushed to disk (e.g. > 4MB)
    writeSome(compressedTableService, tableUncompressed, 32768, 1024, true);
    long uncompressedDiskSizeBytes = compressedTableService.getTableStats().get(tableUncompressedID)
        .getDiskSizeBytes();

    // Write compressible data to table that enables compression, then record on-disk size, which should be
    // smaller than that with compressed disabled.
    LevelDBTableService uncompressedTableService = LevelDBTableService.getInstance();
    cConf.setBoolean(Constants.CFG_DATA_LEVELDB_COMPRESSION_ENABLED, true);
    uncompressedTableService.setConfiguration(cConf);
    String tableCompressed = "cdap_default.tableCompressed";
    TableId tableCompressedID = TableId.from("default", "tableCompressed");
    uncompressedTableService.ensureTableExists(tableCompressed);
    writeSome(uncompressedTableService, tableCompressed, 32768, 1024, true);
    long compressedDiskSizeBytes = uncompressedTableService.getTableStats().get(tableCompressedID)
        .getDiskSizeBytes();

    // Ensure on-disk file size is smaller when compression is enabled.
    Assert.assertTrue(uncompressedDiskSizeBytes > compressedDiskSizeBytes);
  }

  @Test
  public void testCompactTables() throws Exception {
    String tableName = "cdap_default.testCompactTables";
    TableId tableId = TableId.from("default", "testCompactTables");
    Assert.assertNull(service.getTableStats().get(tableId));

    // Create an empty table and record its disk size.
    service.ensureTableExists(tableName);
    long emptyTableDiskSize = service.getTableStats().get(tableId).getDiskSizeBytes();

    // Write some data and compact to disk.
    DB table = service.getTable(tableName);
    DbImpl tableImpl = (DbImpl) table;
    writeSome(service, tableName, 8 * 1024, 1024, false);
    tableImpl.flushMemTable();
    service.compact(tableName);
    LevelDBTableService.TableStats tableStats = service.getTableStats().get(tableId);
    long tableDiskSize = tableStats.getDiskSizeBytes();
    Assert.assertTrue(tableDiskSize > emptyTableDiskSize);

    // Wipe the table by deleting all rows, followed by a compaction which should make the table empty again.
    try (DBIterator iterator = table.iterator()) {
      iterator.seekToFirst();
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.peekNext();
        table.delete(entry.getKey());
        iterator.next();
      }
    }
    tableImpl.flushMemTable();
    service.compact(tableName);
    long currentTableDiskSize = service.getTableStats().get(tableId).getDiskSizeBytes();

    // Verify that the table on-disk size is the same as initial size when it is empty.
    Assert.assertEquals(emptyTableDiskSize, currentTableDiskSize);
  }

  @Test
  public void testFileMetaDataSize() throws IOException {
    String tableName = "cdap_default.testMetaSize";
    TableId tableId = TableId.from("default", "testMetaSize");

    // Create a table and write some data and compact to get big array refs in metadata (without fix)
    service.ensureTableExists(tableName);
    DB table = service.getTable(tableName);
    DbImpl tableImpl = (DbImpl) table;
    writeSome(service, tableName, 8 * 1024, 1024, false);
    tableImpl.flushMemTable();
    service.compact(tableName);
    // Check MetaData keys array sizes
    SnapshotImpl snapshot = (SnapshotImpl) tableImpl.getSnapshot();
    Collection<FileMetaData> fileMetaDatas = snapshot.getVersion().getFiles().values();
    Assert.assertFalse(fileMetaDatas.isEmpty());
    for (FileMetaData fileMetaData : fileMetaDatas) {
      Assert.assertEquals(fileMetaData.getLargest().getUserKey().length(),
          fileMetaData.getLargest().getUserKey().getRawArray().length);
      Assert.assertEquals(fileMetaData.getSmallest().getUserKey().length(),
          fileMetaData.getSmallest().getUserKey().getRawArray().length);
    }
  }

  private void writeSome(LevelDBTableService service, String tableName,
      long numRows, int valNumBytes, boolean compressible) throws IOException {
    LevelDBTableCore table = new LevelDBTableCore(tableName, service);
    Random r = new Random();
    int keyNumBytes = 64;
    byte[] key = new byte[keyNumBytes];
    byte[] value = new byte[valNumBytes];
    for (long i = 0; i < numRows; i++) {
      if (compressible) {
        key = BigInteger.valueOf(i).toByteArray();
        Arrays.fill(value, (byte) 0x8);
      } else {
        r.nextBytes(key);
        r.nextBytes(value);
      }
      table.put(key, Bytes.toBytes("column" + i), value, 0L);
    }
  }
}
