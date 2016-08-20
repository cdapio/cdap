/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.TableAssert;
import co.cask.cdap.proto.Id;
import org.apache.tephra.TransactionExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests for Index table.
 */
public class IndexedTableTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Id.DatasetInstance tabInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "tab");

  private static IndexedTable table;

  static byte[] idxCol = { 'i', 'd', 'x' };
  static byte[] valCol = { 'v', 'a', 'l' };
  static byte[] keyA = { 'a' };
  static byte[] keyAA = { 'a', 'a' };
  static byte[] keyB = { 'b' };
  static byte[] keyC = { 'c' };
  static byte[] keyD = { 'd' };
  static byte[] keyE = { 'e' };
  static byte[] valA = { 'a' };
  static byte[] valAA = { 'a', 'a' };
  static byte[] valAB = { 'a', 'b' };
  static byte[] valB = { 'b' };
  static byte[] valBB = { 'b', 'b' };
  static byte[] valC = { 'c' };
  static byte[] valD = { 'd' };
  static byte[] valE = { 'e' };
  static byte[] idx1 = { '1' };
  static byte[] idx2 = { '2' };
  static byte[] idx3 = { '3' };
  static byte[] idx4 = { '4' };
  static byte[] idx5 = { '5' };

  static String idxColString = Bytes.toString(idxCol);
  static byte[][] colIdxVal = { idxCol, valCol };

  @BeforeClass
  public static void beforeClass() throws Exception {
    dsFrameworkUtil.createInstance("indexedTable", tabInstance, DatasetProperties.builder()
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, idxColString)
      .build());
    table = dsFrameworkUtil.getInstance(tabInstance);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    dsFrameworkUtil.deleteInstance(tabInstance);
  }

  @Test
  public void testKeyPrefix() {
    // tests generation of byte key prefixes for index scanning
    // placed here since used by IndexedTable and we lack of a better home for Bytes testing
    byte[] start = { 0x00 };
    byte[] stop = Bytes.stopKeyForPrefix(start);
    assertArrayEquals(new byte[]{0x01}, stop);
    stop = Bytes.stopKeyForPrefix(stop);
    assertArrayEquals(new byte[]{0x02}, stop);
    start = new byte[]{ 0x01, (byte) 0xff };
    stop = Bytes.stopKeyForPrefix(start);
    assertArrayEquals(new byte[]{0x02}, stop);
    start = new byte[]{ (byte) 0xff, (byte) 0xff };
    stop = Bytes.stopKeyForPrefix(start);
    Assert.assertNull(stop);
  }

  @Test
  public void testIndexedOperations() throws Exception {
    TransactionExecutor txnl = dsFrameworkUtil.newTransactionExecutor(table);

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
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx1, valC});
        // read by key b
        row = table.get(new Get(keyB, colIdxVal));
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx2, valB});
        // read by idx 1 -> c
        row = readFirst(table.readByIndex(idxCol, idx1));
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx1, valC});
        // read by idx 2 -> b
        row = readFirst(table.readByIndex(idxCol, idx2));
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx2, valB});
        // test read over empty index (idx 3)
        assertEmpty(table.readByIndex(idxCol, idx3));
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
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx1, valA});
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
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx1, valC});
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
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx2, valAA});
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
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx2, valAB});
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
        TableAssert.assertColumns(row, colIdxVal, new byte[][]{idx2, valBB});
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
        TableAssert.assertColumn(row, idxCol, idx2);
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
        assertEmpty(table.readByIndex(idxCol, idx1));
        // read by idx 3 > c
        Row row = readFirst(table.readByIndex(idxCol, idx3));
        TableAssert.assertColumns(row, new byte[][]{idxCol, valCol}, new byte[][]{idx3, valC});
      }
    });
  }

  @Test
  public void testIndexedRangeLookups() throws Exception {
    Id.DatasetInstance indexRangedLookupDs =
      Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "rangeLookup");
    dsFrameworkUtil.createInstance("indexedTable", indexRangedLookupDs, DatasetProperties.builder()
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, idxColString)
      .build());
    final IndexedTable iTable = dsFrameworkUtil.getInstance(indexRangedLookupDs);
    TransactionExecutor txnl = dsFrameworkUtil.newTransactionExecutor(iTable);

    try {
      // start a new transaction
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // perform 5 puts, using idx values 1,2,3,4,5
          iTable.put(new Put(keyE).add(idxCol, idx4).add(valCol, valE));
          iTable.put(new Put(keyC).add(idxCol, idx1).add(valCol, valC));
          iTable.put(new Put(keyD).add(idxCol, idx5).add(valCol, valA));
          iTable.put(new Put(keyB).add(idxCol, idx2).add(valCol, valB));
          iTable.put(new Put(keyA).add(idxCol, idx3).add(valCol, valD));
        }
      });

      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // do a scan using idx value range [idx2, idx5). Assert that we retrieve idx2, idx3, idx4.
          Scanner scanner = iTable.scanByIndex(idxCol, idx2, idx5);
          Row next = scanner.next();
          Assert.assertNotNull(next);
          Assert.assertTrue(Bytes.equals(keyB, next.getRow()));
          Assert.assertTrue(Bytes.equals(valB, next.get(valCol)));

          next = scanner.next();
          Assert.assertNotNull(next);
          Assert.assertTrue(Bytes.equals(keyA, next.getRow()));
          Assert.assertTrue(Bytes.equals(valD, next.get(valCol)));

          next = scanner.next();
          Assert.assertNotNull(next);
          Assert.assertTrue(Bytes.equals(keyE, next.getRow()));
          Assert.assertTrue(Bytes.equals(valE, next.get(valCol)));

          assertEmpty(scanner);
        }
      });

      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // do a scan using idx value range [null (first row), idx3). Assert that we retrieve the values corresponding
          // to idx1, idx2.
          Scanner scanner = iTable.scanByIndex(idxCol, null, idx3);
          Row next = scanner.next();
          Assert.assertNotNull(next);
          Assert.assertTrue(Bytes.equals(keyC, next.getRow()));
          Assert.assertTrue(Bytes.equals(valC, next.get(valCol)));

          next = scanner.next();
          Assert.assertNotNull(next);
          Assert.assertTrue(Bytes.equals(keyB, next.getRow()));
          Assert.assertTrue(Bytes.equals(valB, next.get(valCol)));

          assertEmpty(scanner);
        }
      });
    } finally {
      dsFrameworkUtil.deleteInstance(indexRangedLookupDs);
    }
  }

  @Test
  public void testIndexKeyDelimiterAmbiguity() throws Exception {
    final byte[] a = { 'a' };
    final byte[] ab = { 'a', 0, 'b' };
    final byte[] abc = { 'a', 0, 'b', 0, 'c' };
    final byte[] bc = { 'b', 0, 'c' };
    final byte[] bcd = { 'b', 0, 'c', 'd' };
    final byte[] c = { 'c' };
    final byte[] d = { 'd' };
    final byte[] w = { 'w' };
    final byte[] x = { 'x' };
    final byte[] y = { 'y' };
    final byte[] z = { 'z' };
    Id.DatasetInstance delimTabInstance = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "delimtab");
    dsFrameworkUtil.createInstance("indexedTable", delimTabInstance, DatasetProperties.builder()
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, Bytes.toString(a) + "," + Bytes.toString(ab))
      .build());
    final IndexedTable iTable = dsFrameworkUtil.getInstance(delimTabInstance);
    try {
      TransactionExecutor tx = dsFrameworkUtil.newTransactionExecutor(iTable);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          iTable.put(x, a, bc);
          iTable.put(y, ab, c);
          iTable.put(w, a, bcd);
          iTable.put(z, abc, d);
        }
      });

      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // ensure that readByIndex filters teh false positive rows in index
          Scanner scanner = iTable.readByIndex(a, bc);
          try {
            Row row = scanner.next();
            Assert.assertNotNull(row);
            Assert.assertArrayEquals(x, row.getRow());
            Assert.assertArrayEquals(bc, row.get(a));
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = iTable.readByIndex(ab, c);
          try {
            Row row = scanner.next();
            Assert.assertNotNull(row);
            Assert.assertArrayEquals(y, row.getRow());
            Assert.assertArrayEquals(c, row.get(ab));
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          // ensure that scanByIndex filters the false positive rows in index
          scanner = iTable.scanByIndex(a, bcd, null);
          try {
            Row row = scanner.next();
            Assert.assertNotNull(row);
            Assert.assertArrayEquals(w, row.getRow());
            Assert.assertArrayEquals(bcd, row.get(a));
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = iTable.scanByIndex(a, null, bcd);
          try {
            Row row = scanner.next();
            Assert.assertNotNull(row);
            Assert.assertArrayEquals(x, row.getRow());
            Assert.assertArrayEquals(bc, row.get(a));
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }
        }
      });
    } finally {
      dsFrameworkUtil.deleteInstance(delimTabInstance);
    }
  }

  @Test
  public void testMultipleIndexedColumns() throws Exception {
    Id.DatasetInstance multiColumnTabInstance =
      Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "multicolumntab");
    dsFrameworkUtil.createInstance("indexedTable", multiColumnTabInstance, DatasetProperties.builder()
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "idx1,idx2,idx3")
      .build());
    final byte[] idxCol1 = Bytes.toBytes("idx1");
    final byte[] idxCol2 = Bytes.toBytes("idx2");
    final byte[] idxCol3 = Bytes.toBytes("idx3");

    final IndexedTable mcTable = dsFrameworkUtil.getInstance(multiColumnTabInstance);

    try {
      TransactionExecutor tx = dsFrameworkUtil.newTransactionExecutor(mcTable);
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
            TableAssert.assertRow(row, Bytes.toBytes("row1"), oddColumns, new byte[][]{idx1, one, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row3"), oddColumns, new byte[][]{idx1, zero, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2, one, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row5"), oddColumns, new byte[][]{idx1, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row7"), oddColumns, new byte[][]{idx1, one, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row9"), oddColumns, new byte[][]{idx1, zero, valA});
            // should be end of rows
            assertEmpty(scanner);
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
            TableAssert.assertRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2, one, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            // should be at the end
            assertEmpty(scanner);
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
            TableAssert.assertRow(row, Bytes.toBytes("row3"), oddColumns, new byte[][]{idx1, zero, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row9"), oddColumns, new byte[][]{idx1, zero, valA});
            // should be end of rows
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          // 1 should have rows 1, 4, 7
          scanner = mcTable.readByIndex(idxCol3, one);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row1"), oddColumns, new byte[][]{idx1, one, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2, one, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row7"), oddColumns, new byte[][]{idx1, one, valA});
            // should be end of rows
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          // 2 should have rows 2, 5, 8
          scanner = mcTable.readByIndex(idxCol3, two);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row5"), oddColumns, new byte[][]{idx1, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            // should be end of rows
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }
        }
      });

      // update idx2 value for rows 2 & 4
      final byte[] idx2b = new byte[]{ '2', 'b' };
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          mcTable.put(Bytes.toBytes("row2"), idxCol2, idx2b);
          mcTable.put(Bytes.toBytes("row4"), idxCol2, idx2b);
        }
      });

      // only rows 6 & 8 should be returned for idx2 now
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Scanner scanner = mcTable.readByIndex(idxCol2, idx2);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row6"), allColumns, new byte[][]{idx1, idx2, zero, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row8"), allColumns, new byte[][]{idx1, idx2, two, valA});
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = mcTable.readByIndex(idxCol2, idx2b);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row2"), allColumns, new byte[][]{idx1, idx2b, two, valA});
            row = scanner.next();
            TableAssert.assertRow(row, Bytes.toBytes("row4"), allColumns, new byte[][]{idx1, idx2b, one, valA});
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }
        }
      });

      // rows 2 & 4 should be returned for idx2b
    } finally {
      dsFrameworkUtil.deleteInstance(multiColumnTabInstance);
    }
  }

  /**
   * Test conditions where the indexed column name or column value may contain the key delimiter.
   * @throws Exception
   */
  @Test
  public void testIndexKeyDelimiterHandling() throws Exception {
    Id.DatasetInstance delimTabInstance = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "delimtab");
    dsFrameworkUtil.createInstance("indexedTable", delimTabInstance, DatasetProperties.builder()
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, idxColString)
      .build());
    final IndexedTable iTable = dsFrameworkUtil.getInstance(delimTabInstance);
    final byte[] delim = new byte[]{ 0 };
    try {
      final byte[] valueWithDelimiter = Bytes.concat(idx1, delim, idx2);
      TransactionExecutor tx = dsFrameworkUtil.newTransactionExecutor(iTable);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          iTable.put(keyA, idxCol, idx1);
          iTable.put(keyB, idxCol, valueWithDelimiter);
          iTable.put(keyC, idxCol, idx2);
        }
      });

      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Scanner scanner = iTable.readByIndex(idxCol, idx1);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, keyA, new byte[][]{idxCol}, new byte[][]{idx1});
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = iTable.readByIndex(idxCol, idx2);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, keyC, new byte[][]{idxCol}, new byte[][]{idx2});
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = iTable.readByIndex(idxCol, valueWithDelimiter);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, keyB, new byte[][]{idxCol}, new byte[][]{valueWithDelimiter});
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }
        }
      });
    } finally {
      dsFrameworkUtil.deleteInstance(delimTabInstance);
    }
  }

  @Test
  public void testIncrementIndexing() throws Exception {
    Id.DatasetInstance incrTabInstance = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "incrtab");
    dsFrameworkUtil.createInstance("indexedTable", incrTabInstance, DatasetProperties.builder()
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "idx1,idx2,idx3")
      .build());
    final IndexedTable iTable = dsFrameworkUtil.getInstance(incrTabInstance);
    final byte[] idxCol1 = Bytes.toBytes("idx1");
    final byte[] idxCol2 = Bytes.toBytes("idx2");
    final byte[] idxCol3 = Bytes.toBytes("idx3");

    final byte[] row1 = Bytes.toBytes("row1");

    try {
      TransactionExecutor tx = dsFrameworkUtil.newTransactionExecutor(iTable);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          long result = iTable.incrementAndGet(row1, idxCol1, 1);
          assertEquals(1L, result);
        }
      });

      final byte[] oneBytes = Bytes.toBytes(1L);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          try (Scanner scanner = iTable.readByIndex(idxCol1, oneBytes)) {
            Row row = scanner.next();
            TableAssert.assertRow(row, row1, new byte[][]{idxCol1}, new byte[][]{oneBytes});
            assertEmpty(scanner);
          }
        }
      });

      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          long result = iTable.incrementAndGet(row1, idxCol1, 1);
          assertEquals(2L, result);
        }
      });

      final byte[] twoBytes = Bytes.toBytes(2L);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // previous index by value 1 should be gone
          Scanner scanner = iTable.readByIndex(idxCol1, oneBytes);
          try {
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          // should now be indexed by value 2
          scanner = iTable.readByIndex(idxCol1, twoBytes);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, row1, new byte[][]{idxCol1}, new byte[][]{twoBytes});
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }
        }
      });

      final byte[] threeBytes = Bytes.toBytes(3L);
      final byte[][] idxCols = new byte[][]{ idxCol1, idxCol2, idxCol3 };
      final byte[][] expectedValues = new byte[][]{ threeBytes, oneBytes, oneBytes };
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Row result = iTable.incrementAndGet(row1, idxCols, new long[]{ 1, 1, 1 });
          assertNotNull(result);
          TableAssert.assertColumns(result, idxCols, expectedValues);
        }
      });

      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Scanner scanner = iTable.readByIndex(idxCol1, threeBytes);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, row1, idxCols, expectedValues);
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = iTable.readByIndex(idxCol2, oneBytes);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, row1, idxCols, expectedValues);
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }

          scanner = iTable.readByIndex(idxCol3, oneBytes);
          try {
            Row row = scanner.next();
            TableAssert.assertRow(row, row1, idxCols, expectedValues);
            assertEmpty(scanner);
          } finally {
            scanner.close();
          }
        }
      });

      final byte[] row2 = Bytes.toBytes("row2");
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // read-less increment on an indexed column should throw an exception
          try {
            iTable.increment(row2, idxCol1, 1L);
            fail("Expected IllegalArgumentException performing increment on indexed column");
          } catch (IllegalArgumentException iae) {
            // expected
          }

          // read-less increment on a non-indexed column should succeed
          iTable.increment(row2, valCol, 1L);
          byte[] result = iTable.get(row2, valCol);
          assertArrayEquals(oneBytes, result);
        }
      });

      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          iTable.put(row2, valCol, valA);
        }
      });

      // increment against a column with non-long value should fail
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          try {
            iTable.incrementAndGet(row2, valCol, 1L);
            fail("Expected NumberFormatException from increment on a column with non-long value");
          } catch (NumberFormatException nfe) {
            // expected
          }
        }
      });
    } finally {
      dsFrameworkUtil.deleteInstance(incrTabInstance);
    }
  }
  /**
   * Asserts that the given scanner contains no more rows.
   */
  private void assertEmpty(Scanner scanner) {
    Row row = scanner.next();
    Assert.assertNull(row);
  }

  private Row readFirst(Scanner scanner) {
    try {
      return scanner.next();
    } finally {
      scanner.close();
    }
  }
}
