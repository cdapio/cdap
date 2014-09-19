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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.data2.dataset2.TableTest;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.tephra.TransactionExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Index table.
 */
public class IndexedTableTest extends AbstractDatasetTest {

  private static IndexedTable table;

  static byte[] idxCol = { 'i', 'd', 'x' };
  static byte[] valCol = { 'v', 'a', 'l' };
  static byte[] keyA = { 'a' };
  static byte[] keyAA = { 'a', 'a' };
  static byte[] keyB = { 'b' };
  static byte[] keyC = { 'c' };
  static byte[] valA = { 'a' };
  static byte[] valAA = { 'a', 'a' };
  static byte[] valAB = { 'a', 'b' };
  static byte[] valB = { 'b' };
  static byte[] valBB = { 'b', 'b' };
  static byte[] valC = { 'c' };
  static byte[] idx1 = { '1' };
  static byte[] idx2 = { '2' };
  static byte[] idx3 = { '3' };

  static String idxColString = Bytes.toString(idxCol);
  static byte[][] colIdxVal = { idxCol, valCol };

  @BeforeClass
  public static void beforeClass() throws Exception {
    createInstance("indexedTable", "tab", DatasetProperties.builder()
      .add(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY, idxColString)
      .build());
    table = getInstance("tab");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance("tab");
  }

  @Test
  public void testKeyPrefix() {
    // tests generation of byte key prefixes for index scanning
    // placed here since used by IndexedTable and we lack of a better home for Bytes testing
    byte[] start = { 0x00 };
    byte[] stop = Bytes.incrementPrefix(start);
    Assert.assertArrayEquals(new byte[]{ 0x01 }, stop);
    stop = Bytes.incrementPrefix(stop);
    Assert.assertArrayEquals(new byte[]{ 0x02 }, stop);
    start = new byte[]{ 0x01, (byte) 0xff };
    stop = Bytes.incrementPrefix(start);
    Assert.assertArrayEquals(new byte[]{ 0x02 }, stop);
    start = new byte[]{ (byte) 0xff, (byte) 0xff };
    stop = Bytes.incrementPrefix(start);
    Assert.assertNull(stop);
  }

  @Test
  public void testIndexedOperations() throws Exception {
    TransactionExecutor txnl = newTransactionExecutor(table);

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // add a value c with idx = 1, and b with idx = 2
        table.put(new Put(keyC).add(idxCol, idx1).add(valCol, valC));
        table.put(new Put(keyB).add(idxCol, idx2).add(valCol, valB));
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // read by key c
        Row row = table.get(new Get(keyC, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
        // read by key b
        row = table.get(new Get(keyB, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valB});
        // read by idx 1 -> c
        row = readFirst(table.readByIndex(idxCol, idx1));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
        // read by idx 2 -> b
        row = readFirst(table.readByIndex(idxCol, idx2));
        TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx2, valB });
        // test read over empty index (idx 3)
        row = readFirst(table.readByIndex(idxCol, idx3));
        Assert.assertNull(row);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // add a value a with idx = 1
        table.put(new Put(keyA).add(idxCol, idx1).add(valCol, valA));
      }
    });

    // read by idx 1 -> a
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = readFirst(table.readByIndex(idxCol, idx1));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valA});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // delete value a
        table.delete(new Delete(keyA, colIdxVal));
      }
    });

    // read by idx 1 -> c
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = readFirst(table.readByIndex(idxCol, idx1));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // add a value aa with idx 2
        table.put(new Put(keyAA).add(idxCol, idx2).add(valCol, valAA));
      }
    });

    // read by idx 2 -> aa
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = readFirst(table.readByIndex(idxCol, idx2));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valAA});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap value for aa to ab
        Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valAA, valAB));
      }
    });

    // read by idx 2 -> ab
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = readFirst(table.readByIndex(idxCol, idx2));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valAB});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap value for aa to bb
        Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valAB, valBB));
      }
    });

    // read by idx 2 -> bb (value of key aa)
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = readFirst(table.readByIndex(idxCol, idx2));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valBB});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap value for aa to null
        Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valBB, null));
      }
    });

    // read by idx 2 -> null (value of b)
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = readFirst(table.readByIndex(idxCol, idx2));
        TableTest.verifyColumn(row, idxCol, idx2);
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap idx for c to 3
        Assert.assertTrue(table.compareAndSwap(keyC, idxCol, idx1, idx3));
      }
    });

    // read by idx 1 -> null (no row has that any more)
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertNull(readFirst(table.readByIndex(idxCol, idx1)));
        // read by idx 3 > c
        Row row = readFirst(table.readByIndex(idxCol, idx3));
        TableTest.verifyColumns(row, new byte[][]{idxCol, valCol}, new byte[][]{idx3, valC});
      }
    });
  }

  @Test
  public void testMultipleIndexedColumns() throws Exception {
    createInstance("indexedTable", "multicolumntab", DatasetProperties.builder()
      .add(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY, "idx1,idx2,idx3")
      .build());
    final byte[] idxCol1 = Bytes.toBytes("idx1");
    final byte[] idxCol2 = Bytes.toBytes("idx2");
    final byte[] idxCol3 = Bytes.toBytes("idx3");

    final IndexedTable mcTable = getInstance("multicolumntab");

    try {
      TransactionExecutor tx = newTransactionExecutor(mcTable);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // write multiple rows with two indexed columns
          // every row has idx1 = 1
          // even rows have idx2 = 2
          // every row has idx3 = index mod 3
          for (int i = 1; i < 10; i++) {
            Put put = new Put(Bytes.toBytes("row" + i));
            put.add(idxCol1, idx1);
            if (i % 2 == 0) {
              put.add(idxCol2, idx2);
            }
            put.add(idxCol3, Bytes.toBytes(i % 3));
            put.add(valCol, valA);
            mcTable.put(put);
          }
        }
      });

      final byte[][] allColumns = new byte[][]{ idxCol1, idxCol2, idxCol3, valCol };
      final byte[][] oddColumns = new byte[][]{ idxCol1, idxCol3, valCol };
      final byte[] zero = Bytes.toBytes(0);
      final byte[] one = Bytes.toBytes(1);
      final byte[] two = Bytes.toBytes(2);

      // read by index 1
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Scanner scanner = mcTable.readByIndex(idxCol1, idx1);
          try {
            // should have all rows, all data
            Row row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row1"), oddColumns, new byte[][]{idx1, one, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row3"), oddColumns, new byte[][]{idx1, zero, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2, one, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row5"), oddColumns, new byte[][]{idx1, two, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row7"), oddColumns, new byte[][]{idx1, one, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row9"), oddColumns, new byte[][]{idx1, zero, valA});
            // should be end of rows
            row = scanner.next();
            Assert.assertNull(row);
          } finally {
            scanner.close();
          }
        }
      });

      // read by index 2
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Scanner scanner = mcTable.readByIndex(idxCol2, idx2);
          try {
            // Should have only even rows
            Row row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2, one, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            // should be at the end
            row = scanner.next();
            Assert.assertNull(row);
          } finally {
            scanner.close();
          }
        }
      });

      // read by index 3
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // 0 should have rows 3, 6, 9
          Scanner scanner = mcTable.readByIndex(idxCol3, zero);
          try {
            Row row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row3"), oddColumns, new byte[][]{idx1, zero, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row9"), oddColumns, new byte[][]{idx1, zero, valA});
            // should be end of rows
            row = scanner.next();
            Assert.assertNull(row);
          } finally {
            scanner.close();
          }

          // 1 should have rows 1, 4, 7, 10
          scanner = mcTable.readByIndex(idxCol3, one);
          try {
            Row row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row1"), oddColumns, new byte[][]{idx1, one, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2, one, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row7"), oddColumns, new byte[][]{idx1, one, valA});
            // should be end of rows
            row = scanner.next();
            Assert.assertNull(row);
          } finally {
            scanner.close();
          }

          // 2 should have rows 2, 5, 8
          scanner = mcTable.readByIndex(idxCol3, two);
          try {
            Row row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row5"), oddColumns, new byte[][]{idx1, two, valA});
            row = scanner.next();
            TableTest.verifyRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            // should be end of rows
            row = scanner.next();
            Assert.assertNull(row);
          } finally {
            scanner.close();
          }
        }
      });

    } finally {
      deleteInstance("multicolumntab");
    }
  }

  private Row readFirst(Scanner scanner) {
    Row row = null;
    try {
      row = scanner.next();
    } finally {
      scanner.close();
    }
    return row;
  }
}
