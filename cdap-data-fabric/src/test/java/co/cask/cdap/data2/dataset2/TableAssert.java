/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import org.junit.Assert;

import java.util.Map;

/**
 * Data set table test helpers.
 */
public class TableAssert {

  public static void assertRow(Row result, byte[] expectedRow, byte[][] columns, byte[][] expected) {
    Assert.assertNotNull(result);
    Assert.assertArrayEquals(expectedRow, result.getRow());
    assertColumns(result, columns, expected);
  }

  public static void assertColumns(Row result, byte[][] columns, byte[][] expected) {
    Assert.assertEquals(columns.length, expected.length);
    Assert.assertNotNull(result);
    Assert.assertFalse(result.isEmpty());
    Map<byte[], byte[]> colsMap = result.getColumns();
    Assert.assertNotNull(colsMap);

    assertColumns(columns, expected, colsMap);
  }

  private static void assertColumns(byte[][] expectedCols, byte[][] expectedVals, Map<byte[], byte[]> toVerify) {
    Assert.assertEquals(expectedCols.length, toVerify.size());
    for (int i = 0; i < expectedCols.length; i++) {
      Assert.assertArrayEquals(expectedVals[i], toVerify.get(expectedCols[i]));
    }
  }

  public static void assertColumns(byte[][] expectedCols, byte[][] expectedVals, Row row) {
    assertColumns(expectedCols, expectedVals, row.getColumns());
  }

  public static void assertColumn(Row result, byte[] column, byte[] expected) {
    assertColumns(result, new byte[][]{column}, new byte[][]{expected});
  }

  public static void assertRow(byte[][] expected, Row row) {
    assertRow(expected, row.getColumns());
  }

  public static void assertRow(byte[][] expected, Map<byte[], byte[]> rowMap) {
    Assert.assertEquals(expected.length / 2, rowMap.size());
    for (int i = 0; i < expected.length; i += 2) {
      byte[] key = expected[i];
      byte[] val = expected[i + 1];
      Assert.assertArrayEquals(val, rowMap.get(key));
    }
  }

  public static void assertScan(byte[][] expectedRows, byte[][][] expectedRowMaps, Table table, Scan scan) {
    assertScan(expectedRows, expectedRowMaps, table.scan(scan));
    if (scan.getFilter() == null) {
      // if only start and stop row are specified, we also want to check the scan(startRow, stopRow) APIs
      assertScan(expectedRows, expectedRowMaps, table.scan(scan.getStartRow(), scan.getStopRow()));
    }
  }

  public static void assertScan(byte[][] expectedRows, byte[][][] expectedRowMaps, Scanner scanner) {
    for (int i = 0; i < expectedRows.length; i++) {
      Row next = scanner.next();
      Assert.assertNotNull(next);
      Assert.assertArrayEquals(expectedRows[i], next.getRow());
      assertRow(expectedRowMaps[i], next.getColumns());
    }

    // nothing is left in scan
    Assert.assertNull(scanner.next());
  }
}
