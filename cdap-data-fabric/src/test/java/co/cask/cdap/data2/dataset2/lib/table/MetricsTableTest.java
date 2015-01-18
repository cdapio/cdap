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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * tests metrics table.
 */
public abstract class MetricsTableTest {
  private static final byte ONES = 0x7f;

  protected abstract MetricsTable getTable(String name) throws Exception;

  static final byte[] A = Bytes.toBytes(1L);
  static final byte[] B = Bytes.toBytes(2L);
  static final byte[] C = Bytes.toBytes(3L);
  static final byte[] P = Bytes.toBytes(4L);
  static final byte[] Q = Bytes.toBytes(5L);
  static final byte[] R = Bytes.toBytes(6L);
  static final byte[] X = Bytes.toBytes(7L);
  static final byte[] Y = Bytes.toBytes(8L);
  static final byte[] Z = Bytes.toBytes(9L);

  @Test
  public void testGetPutSwap() throws Exception {
    MetricsTable table = getTable("testGetPutSwap");
    // put two rows
    table.put(ImmutableSortedMap.<byte[], NavigableMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
              .put(A, Bytes.immutableSortedMapOf(P, Bytes.toLong(X), Q, Bytes.toLong(Y)))
              .put(B, Bytes.immutableSortedMapOf(P, Bytes.toLong(X), R, Bytes.toLong(Z))).build());
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(table.get(A, P)));
    Assert.assertEquals(Bytes.toLong(Y), Bytes.toLong(table.get(A, Q)));
    Assert.assertNull(table.get(A, R));
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(table.get(B, P)));
    Assert.assertEquals(Bytes.toLong(Z), Bytes.toLong(table.get(B, R)));
    Assert.assertNull(table.get(B, Q));

    // put one row again, with overlapping key set
    table.put(ImmutableSortedMap.<byte[], NavigableMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
              .put(A, Bytes.immutableSortedMapOf(P, Bytes.toLong(A), R, Bytes.toLong(C))).build());
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(table.get(A, P)));
    Assert.assertEquals(Bytes.toLong(Y), Bytes.toLong(table.get(A, Q)));
    Assert.assertEquals(Bytes.toLong(C), Bytes.toLong(table.get(A, R)));

    // compare and swap an existing value, successfully
    Assert.assertTrue(table.swap(A, P, A, B));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(table.get(A, P)));
    // compare and swap an existing value that does not match
    Assert.assertFalse(table.swap(A, P, A, B));
    Assert.assertArrayEquals(B, table.get(A, P));
    // compare and swap an existing value that does not exist
    Assert.assertFalse(table.swap(B, Q, A, B));
    Assert.assertNull(table.get(B, Q));

    // compare and delete an existing value, successfully
    Assert.assertTrue(table.swap(A, P, B, null));
    Assert.assertNull(table.get(A, P));
    // compare and delete an existing value that does not match
    Assert.assertFalse(table.swap(A, Q, A, null));
    Assert.assertArrayEquals(Y, table.get(A, Q));

    // compare and swap a null value, successfully
    Assert.assertTrue(table.swap(A, P, null, Z));
    Assert.assertArrayEquals(Z, table.get(A, P));
    // compare and swap a null value, successfully
    Assert.assertFalse(table.swap(A, Q, null, Z));
    Assert.assertArrayEquals(Y, table.get(A, Q));
  }

  class IncThread extends Thread {
    final MetricsTable table;
    final byte[] row;
    final Map<byte[], Long> incrememts;
    int rounds;
    IncThread(MetricsTable table, byte[] row, Map<byte[], Long> incrememts, int rounds) {
      this.table = table;
      this.row = row;
      this.incrememts = incrememts;
      this.rounds = rounds;
    }
    public void run() {
      while (rounds-- > 0) {
        try {
          table.increment(row, incrememts);
        } catch (Exception e) {
          System.err.println("exception for increment #" + rounds + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  class IncAndGetThread extends Thread {
    final MetricsTable table;
    final byte[] row;
    final byte[] col;
    final long delta;
    int rounds;
    Long previous = 0L;
    IncAndGetThread(MetricsTable table, byte[] row, byte[] col, long delta, int rounds) {
      this.table = table;
      this.row = row;
      this.col = col;
      this.delta = delta;
      this.rounds = rounds;
    }
    public void run() {
      while (rounds-- > 0) {
        try {
          long value = table.incrementAndGet(row, col, delta);
          Assert.assertTrue(value > previous);
        } catch (Exception e) {
          System.err.println("exception for increment and get #" + rounds + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  @Ignore
  //TODO: CDAP-1186
  public void testConcurrentIncrement() throws Exception {
    final MetricsTable table = getTable("testConcurrentIncrement");
    final int rounds = 500;
    Map<byte[], Long> inc1 = ImmutableMap.of(X, 1L, Y, 2L);
    Map<byte[], Long> inc2 = ImmutableMap.of(Y, 1L, Z, 2L);
    Collection<Thread> threads = ImmutableList.of(new IncThread(table, A, inc1, rounds),
                                                  new IncThread(table, A, inc2, rounds),
                                                  new IncAndGetThread(table, A, Z, 5, rounds),
                                                  new IncAndGetThread(table, A, Z, 2, rounds));
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(rounds + 10L, table.incrementAndGet(A, X, 10L));
    Assert.assertEquals(3 * rounds - 20L, table.incrementAndGet(A, Y, -20L));
    Assert.assertEquals(9 * rounds, table.incrementAndGet(A, Z, 0L));
  }

  class SwapThread extends Thread {
    private final MetricsTable table;
    private final byte[] row;
    private final byte[] col;
    private final AtomicInteger[] counts;
    private final long rounds;

    SwapThread(MetricsTable table, byte[] row, byte[] col, AtomicInteger[] counts, long rounds) {
      this.table = table;
      this.row = row;
      this.col = col;
      this.counts = counts;
      this.rounds = rounds;
    }

    public void run() {
      for (long i = 0; i < rounds; i++) {
        try {
          boolean success = table.swap(row, col, Bytes.toBytes(i), Bytes.toBytes(i + 1));
          counts[success ? 0 : 1].incrementAndGet();
        } catch (Exception e) {
          System.err.println("exception for swap #" + rounds + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testConcurrentSwap() throws Exception {
    MetricsTable table = getTable("testConcurrentSwap");
    final long rounds = 500;
    table.put(ImmutableSortedMap.<byte[], NavigableMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
                .put(A, Bytes.immutableSortedMapOf(B, 0L)).build());
    AtomicInteger[] counts = { new AtomicInteger(), new AtomicInteger() }; // [0] for success, [1] for failures
    Thread t1 = new SwapThread(table, A, B, counts, rounds);
    Thread t2 = new SwapThread(table, A, B, counts, rounds);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    Assert.assertEquals(rounds, Bytes.toLong(table.get(A, B)));
    Assert.assertEquals(rounds, counts[0].get()); // number of successful swaps
    Assert.assertEquals(rounds, counts[1].get()); // number of failed swaps
  }

  @Test
  public void testDelete() throws Exception {
    MetricsTable table = getTable("testDelete");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 1024; i++) {
      writes.put(Bytes.toBytes(i << 22), Bytes.immutableSortedMapOf(A, Bytes.toLong(X)));
    }
    table.put(writes);
    // verify the first and last are there (sanity test for correctness of test logic)
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0x00000000), A));
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0xffc00000), A));

    List<byte[]> toDelete = ImmutableList.of(
      Bytes.toBytes(0xa1000000), Bytes.toBytes(0xb2000000), Bytes.toBytes(0xc3000000));
    // verify these three are there, we will soon delete them
    for (byte[] row : toDelete) {
      Assert.assertArrayEquals(X, table.get(row, A));
    }
    // delete these three
    table.delete(toDelete);
    // verify these three are now gone.
    for (byte[] row : toDelete) {
      Assert.assertNull(table.get(row, A));
    }
    // verify nothing else is gone by counting all entries in a scan
    Assert.assertEquals(1021, countRange(table, null, null));

    // delete a range by prefix
    table.deleteAll(new byte[] { 0x11 });
    // verify that they are gone
    Assert.assertEquals(0, countRange(table, 0x11000000, 0x12000000));
    // verify nothing else is gone by counting all entries in a scan (deleted 4)
    Assert.assertEquals(1017, countRange(table, null, null));

    // delete all with prefix 0xff (it a border case because it has no upper bound)
    table.deleteAll(new byte[] { (byte) 0xff });
    // verify that they are gone
    Assert.assertEquals(0, countRange(table, 0xff000000, null));
    // verify nothing else is gone by counting all entries in a scan (deleted another 4)
    Assert.assertEquals(1013, countRange(table, null, null));

    // delete with empty prefix, should clear table
    // delete all with prefix 0xff (it a border case because it has no upper bound)
    table.deleteAll(new byte[0]);
    // verify everything is gone by counting all entries in a scan
    Assert.assertEquals(0, countRange(table, null, null));
  }

  private static int countRange(MetricsTable table, Integer start, Integer stop) throws Exception {
    Scanner scanner = table.scan(start == null ? null : Bytes.toBytes(start),
                                 stop == null ? null : Bytes.toBytes(stop), null, null);
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }
    return count;
  }

  @Test
  public void testFuzzyScan() throws Exception {
    MetricsTable table = getTable("testFuzzyScan");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, Bytes.immutableSortedMapOf(A, Bytes.toLong(X), B,
                                                                                 Bytes.toLong(Y)));
          }
        }
      }
    }
    table.put(writes);
    // we should have 81 (3^4) rows now
    Assert.assertEquals(81, countRange(table, null, null));

    // now do a fuzzy scan of the table
    FuzzyRowFilter filter = new FuzzyRowFilter(
      ImmutableList.of(ImmutablePair.of(new byte[] { '*', 'b', '*', 'b' }, new byte[] { 0x01, 0x00, 0x01, 0x00 })));
    Scanner scanner = table.scan(null, null, new byte[][] { A }, filter);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      Assert.assertTrue(entry.getRow()[1] == 'b' && entry.getRow()[3] == 'b');
      Assert.assertEquals(1, entry.getColumns().size());
      Assert.assertTrue(entry.getColumns().containsKey(A));
      Assert.assertFalse(entry.getColumns().containsKey(B));
      count++;
    }
    Assert.assertEquals(9, count);
  }

  @Test
  public void testRangeDeleteWithoutFilter() throws Exception {
    MetricsTable table = getTable("rangeDelete");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, Bytes.immutableSortedMapOf(A, Bytes.toLong(X), B,
                                                                                 Bytes.toLong(Y)));
          }
        }
      }
    }
    table.put(writes);
    // we should have 81 (3^4) rows now
    Assert.assertEquals(81, countRange(table, null, null));

    byte[] start = new byte[] { 'a', 0x00, 0x00, 0x00 };
    byte[] end = new byte[] { 'a', ONES, ONES, ONES };
    FuzzyRowFilter filter = new FuzzyRowFilter(
      ImmutableList.of(ImmutablePair.of(new byte[] { '*', 'b', '*', 'b' }, new byte[] { 0x01, 0x00, 0x01, 0x00 })));
    table.deleteRange(start, end, new byte[][] { A }, filter);

    // now do a scan of the table, making sure the cells with row like {a,b,*,b} and column A are gone.
    Scanner scanner = table.scan(null, null, null, null);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      byte[] row = entry.getRow();
      if (row[0] == 'a' && row[1] == 'b' && row[3] == 'b') {
        Assert.assertFalse(entry.getColumns().containsKey(A));
        Assert.assertEquals(1, entry.getColumns().size());
      } else {
        Assert.assertTrue(entry.getColumns().containsKey(A));
        Assert.assertTrue(entry.getColumns().containsKey(B));
        Assert.assertEquals(2, entry.getColumns().size());
      }
      count++;
    }
    Assert.assertEquals(81, count);
  }

  @Test
  public void testRangeDeleteWithFilter() throws Exception {
    MetricsTable table = getTable("rangeDelete");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, Bytes.immutableSortedMapOf(A, Bytes.toLong(X), B,
                                                                                 Bytes.toLong(Y)));
          }
        }
      }
    }
    table.put(writes);
    // we should have 81 (3^4) rows now
    Assert.assertEquals(81, countRange(table, null, null));

    byte[] start = new byte[] { 'a', 0x00, 0x00, 0x00 };
    byte[] end = new byte[] { 'a', ONES, ONES, ONES };
    table.deleteRange(start, end, new byte[][] { A }, null);

    // now do a scan of the table, making sure the cells with row starting with 'a' and column A are gone.
    Scanner scanner = table.scan(null, null, null, null);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      if (entry.getRow()[0] == 'a') {
        Assert.assertFalse(entry.getColumns().containsKey(A));
        Assert.assertEquals(1, entry.getColumns().size());
      } else {
        Assert.assertTrue(entry.getColumns().containsKey(A));
        Assert.assertTrue(entry.getColumns().containsKey(B));
        Assert.assertEquals(2, entry.getColumns().size());
      }
      count++;
    }
    Assert.assertEquals(81, count);
  }

}
