/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.data.runtime.DataFabricLevelDBModule;
import io.cdap.cdap.data.runtime.TransactionMetricsModule;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;


public class LevelDBTableCoreTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  static LevelDBTableService service;
  static Injector injector = null;

  private final String rowNamePrefix = "row-";
  private final String colName = "colName";

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
  public void testGetAndPut() throws Exception {
    String tableName = "testGetTable";
    TableId tableId = TableId.from("default", tableName);

    {
      Assert.assertNull(service.getTableStats().get(tableId));
      service.ensureTableExists(tableName);
      LevelDBTableCore table = new LevelDBTableCore(tableName, service);

      String row = "row";
      String col = "col";
      String val = "val";
      int numVersion = 10;

      // Write different values at the same target row and col as different versions.
      for (int i = 0; i < numVersion; i++) {
        writeRowCol(table, row, col, String.format("%s-%d", val, i), i);
      }
      // Write another value with default version (i.e. max version) that hides all values above.
      writeRowColDefaultVersion(table, row, col, val);

      // Read value at default version
      Assert.assertTrue(readRowColDefaultVersion(table, row, col).equals(val));

      // Read a specific version.
      for (int i = 0; i < numVersion; i++) {
        Assert.assertTrue(readRowCol(table, row, col, i).equals(String.format("%s-%d", val, i)));
      }

      // Reading highest-versioned value should return the value with max version.
      Assert.assertTrue(readRowColLatest(table, row, col).equals(val));

      // After deleting the value at default version (i.e. max version),
      // reading highest-versioned value should return the value at a proper version.
      deleteRowColDefaultVersion(table, row, col);
      Assert.assertTrue(readRowColLatest(table, row, col).equals(String.format("%s-%d", val, numVersion - 1)));

      // Reading value at default version (i.e. max version) should return null as it has been deleted above.
      Assert.assertEquals(null, readRowColDefaultVersion(table, row, col));

      service.dropTable(tableName);
    }
  }

  @Test
  public void testScan() throws Exception {
    String tableName = "testScanTable";
    TableId tableId = TableId.from("default", tableName);

    {
      Assert.assertNull(service.getTableStats().get(tableId));
      service.ensureTableExists(tableName);
      LevelDBTableCore table = new LevelDBTableCore(tableName, service);

      int numRows = 8;
      int numVersion = 8;

      // Write data to multiple rows and multiple versions per row and col.
      writeData(table, rowNamePrefix, numRows, colName, 1024, numVersion);

      // Scan only the first row and make sure no data from other rows are returned.
      try (Scanner scanner = table.scan(getRowName(rowNamePrefix, 0).getBytes(StandardCharsets.UTF_8),
                                        getRowName(rowNamePrefix, 1).getBytes(StandardCharsets.UTF_8),
                                        null, null, null)) {
        Row row;
        while ((row = scanner.next()) != null) {
          String rowName = new String(row.getRow(), StandardCharsets.UTF_8);
          Assert.assertTrue(rowName.equals(getRowName(rowNamePrefix, 0)));
        }
      }

      // Test a corner case by writing to row i + 1 at default version (i.e. max version) and scan row i,
      // because scan uses the max version at row i + 1 as scan end key (excluded) and we want to make sure
      // nothing from row i + 1 gets returned in such case.
      writeRowColDefaultVersion(table, getRowName(rowNamePrefix, 1), colName, "dummy-value");
      try (Scanner scanner = table.scan(getRowName(rowNamePrefix, 0).getBytes(StandardCharsets.UTF_8),
                                        getRowName(rowNamePrefix, 1).getBytes(StandardCharsets.UTF_8),
                                        null, null, null)) {
        Row row;
        while ((row = scanner.next()) != null) {
          String rowName = new String(row.getRow(), StandardCharsets.UTF_8);
          Assert.assertTrue(rowName.equals(getRowName(rowNamePrefix, 0)));
        }
      }

      service.dropTable(tableName);
    }
  }

  @Test
  public void testDeleteRows() throws Exception {
    String tableName = "testDeleteRowsTable";
    TableId tableId = TableId.from("default", tableName);

    // Single value table
    {
      Assert.assertNull(service.getTableStats().get(tableId));
      service.ensureTableExists(tableName);
      LevelDBTableCore table = new LevelDBTableCore(tableName, service);
      writeData(table, rowNamePrefix, 32, colName, 1024, 1);
      List<byte[]> rowsToDelete = new ArrayList<byte[]>();
      rowsToDelete.add(getRowName(rowNamePrefix, 1).getBytes(StandardCharsets.UTF_8));
      rowsToDelete.add(getRowName(rowNamePrefix, 5).getBytes(StandardCharsets.UTF_8));
      rowsToDelete.add(getRowName(rowNamePrefix, 7).getBytes(StandardCharsets.UTF_8));
      table.deleteRows(rowsToDelete);
      try (Scanner scanner = table.scan(Bytes.toBytes(1L), null, null, null, null)) {
        Row row;
        while ((row = scanner.next()) != null) {
          String rowName = new String(row.getRow(), StandardCharsets.UTF_8);
          Assert.assertFalse(rowsToDelete.contains(rowName.getBytes(StandardCharsets.UTF_8)));
        }
      }
      service.dropTable(tableName);
    }

    // Multi-version value
    {
      Assert.assertNull(service.getTableStats().get(tableId));
      service.ensureTableExists(tableName);
      LevelDBTableCore table = new LevelDBTableCore(tableName, service);
      writeData(table, rowNamePrefix, 32, colName, 1024, 8);
      List<byte[]> rowsToDelete = new ArrayList<byte[]>();
      rowsToDelete.add(getRowName(rowNamePrefix, 1).getBytes(StandardCharsets.UTF_8));
      rowsToDelete.add(getRowName(rowNamePrefix, 5).getBytes(StandardCharsets.UTF_8));
      rowsToDelete.add(getRowName(rowNamePrefix, 7).getBytes(StandardCharsets.UTF_8));
      table.deleteRows(rowsToDelete);
      try (Scanner scanner = table.scan(null, null, null, null, null)) {
        Row row;
        while ((row = scanner.next()) != null) {
          String rowName = new String(row.getRow(), StandardCharsets.UTF_8);
          Assert.assertFalse(rowsToDelete.contains(rowName.getBytes(StandardCharsets.UTF_8)));
        }
      }
      service.dropTable(tableName);
    }
  }

  @Test
  public void testDelete() throws Exception {
    String tableName = "testDeleteTable";
    TableId tableId = TableId.from("default", tableName);

    // Test put and delete without specifying version (i.e. uisng latest version)
    {
      Assert.assertNull(service.getTableStats().get(tableId));
      service.ensureTableExists(tableName);
      LevelDBTableCore table = new LevelDBTableCore(tableName, service);
      String row = "row-0";
      String col = "col-0";
      String val = "val-0";
      String valRead;

      writeRowColDefaultVersion(table, row, col, val);
      valRead = readRowColDefaultVersion(table, row, col);
      Assert.assertTrue(valRead.equals(val));

      deleteRowColDefaultVersion(table, row, col);
      valRead = readRowColDefaultVersion(table, row, col);
      Assert.assertEquals(null, valRead);
      service.dropTable(tableName);
    }

    // Test put and delete with different versions (i.e. latest version hides old versions etc)
    {
      Assert.assertNull(service.getTableStats().get(tableId));
      service.ensureTableExists(tableName);
      LevelDBTableCore table = new LevelDBTableCore(tableName, service);
      String row = "row-0";
      String col = "col-0";
      String val = "val";
      int numVersion = 10;
      String valRead;

      // Write different values at the same target row and col as different versions from 0 to 9.
      for (int i = 0; i < numVersion; i++) {
        writeRowCol(table, row, col, String.format("%s-%d", val, i), i);
      }

      // Ensure reading the most recent version returns the latest version 9.
      Assert.assertTrue(readRowColLatest(table, row, col).equals(String.format("%s-%d", val, numVersion - 1)));

      // Delete the highest version (i.e. KeyValue.LATEST_TIMESTAMP) should be an noop
      // since there was no value written at that version.
      deleteRowColDefaultVersion(table, row, col);
      Assert.assertTrue(readRowColLatest(table, row, col).equals(String.format("%s-%d", val, numVersion - 1)));

      // Write at the highest version (i.e. KeyValue.LATEST_TIMESTAMP) should hide all old versions.
      writeRowColDefaultVersion(table, row, col, val);
      Assert.assertTrue(readRowColDefaultVersion(table, row, col).equals(val));

      // Delete a specific old version has no impact on the latest version.
      deleteRowCol(table, row, col, numVersion - 1);
      Assert.assertTrue(readRowColLatest(table, row, col).equals(val));

      // Delete the latest version (i.e. KeyValue.LATEST_TIMESTAMP) should unhide old versions.
      deleteRowColDefaultVersion(table, row, col);
      valRead = readRowColLatest(table, row, col);
      Assert.assertTrue(valRead.equals(String.format("%s-%d", val, numVersion - 2)));

      service.dropTable(tableName);
    }
  }

  /**
   * Write the given value as the latest at the target row and col.
   */
  private void writeRowColDefaultVersion(LevelDBTableCore table, String row, String col, String val)
    throws IOException {
    table.putDefaultVersion(row.getBytes(StandardCharsets.UTF_8),
                            col.getBytes(StandardCharsets.UTF_8),
                            val.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Write the given value as specified version at the target row and col.
   */
  private void writeRowCol(LevelDBTableCore table, String row, String col, String val, long version)
    throws IOException {
    table.put(row.getBytes(StandardCharsets.UTF_8),
              col.getBytes(StandardCharsets.UTF_8),
              val.getBytes(StandardCharsets.UTF_8),
              version);
  }

  /**
   * Read the value from the target row and col at default version.
   * Return null if there is no value for the specified row and col.
   */
  @Nullable
  private String readRowColDefaultVersion(LevelDBTableCore table, String row, String col) throws IOException {
    byte[] val = null;
    val = table.getDefaultVersion(row.getBytes(StandardCharsets.UTF_8),
                                  col.getBytes(StandardCharsets.UTF_8));
    if (val == null) {
      return null;
    }
    return new String(val, StandardCharsets.UTF_8);
  }

  @Nullable
  private String readRowCol(LevelDBTableCore table, String row, String col, long version) throws IOException {
    byte[] val = null;
    val = table.get(row.getBytes(StandardCharsets.UTF_8),
                    col.getBytes(StandardCharsets.UTF_8),
                    version);
    if (val == null) {
      return null;
    }
    return new String(val, StandardCharsets.UTF_8);
  }

  @Nullable
  private String readRowColLatest(LevelDBTableCore table, String row, String col) throws IOException {
    byte[] val = null;
    val = table.getLatest(row.getBytes(StandardCharsets.UTF_8),
                          col.getBytes(StandardCharsets.UTF_8),
                          null);
    if (val == null) {
      return null;
    }
    return new String(val, StandardCharsets.UTF_8);
  }


  /**
   * Delete the value at latest version (i.e. KeyValue.LATEST_TIMESTAMP) in the target row and col.
   */
  private void deleteRowColDefaultVersion(LevelDBTableCore table, String row, String col) throws IOException {
    table.deleteDefaultVersion(row.getBytes(StandardCharsets.UTF_8),
                               col.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Delete the value at the specified version in the target row and col.
   */
  private void deleteRowCol(LevelDBTableCore table, String row, String col, long version) throws IOException {
    table.delete(row.getBytes(StandardCharsets.UTF_8),
                 col.getBytes(StandardCharsets.UTF_8),
                 version);
  }

  /**
   * Write a number of rows to the table. There is only one col per row. For each row and col, write a number of
   * values at different versions.
   */
  private void writeData(LevelDBTableCore table, String rowPrefix, long numRows, String col, int valNumBytes,
                         int numVersions) throws IOException {
    Random r = new Random();
    byte[] value = new byte[valNumBytes];
    for (long rowIndex = 0; rowIndex < numRows; rowIndex++) {
      byte[] key = getRowName(rowPrefix, rowIndex).getBytes(StandardCharsets.UTF_8);
      for (long version = 0; version < numVersions; version++) {
        r.nextBytes(value);
        table.put(key, col.getBytes(StandardCharsets.UTF_8), value, version);
      }
    }
  }

  String getRowName(String prefix, long index) {
    return String.format("%s%d", prefix, index);
  }
}
