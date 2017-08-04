/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * tests metrics table.
 */
public abstract class MetricsTableTest {
  protected abstract MetricsTable getTable(String name) throws Exception;

  protected static final byte[] A = Bytes.toBytes(1L);
  protected static final byte[] B = Bytes.toBytes(2L);
  protected static final byte[] C = Bytes.toBytes(3L);
  protected static final byte[] P = Bytes.toBytes(4L);
  protected static final byte[] Q = Bytes.toBytes(5L);
  protected static final byte[] R = Bytes.toBytes(6L);
  protected static final byte[] X = Bytes.toBytes(7L);
  protected static final byte[] Y = Bytes.toBytes(8L);
  protected static final byte[] Z = Bytes.toBytes(9L);

  @Test
  public void testGetPutSwap() throws Exception {
    MetricsTable table = getTable("testGetPutSwap");
    // put two rows
    table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
              .put(A, mapOf(P, Bytes.toLong(X), Q, Bytes.toLong(Y)))
              .put(B, mapOf(P, Bytes.toLong(X), R, Bytes.toLong(Z))).build());
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(table.get(A, P)));
    Assert.assertEquals(Bytes.toLong(Y), Bytes.toLong(table.get(A, Q)));
    Assert.assertNull(table.get(A, R));
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(table.get(B, P)));
    Assert.assertEquals(Bytes.toLong(Z), Bytes.toLong(table.get(B, R)));
    Assert.assertNull(table.get(B, Q));

    // put one row again, with overlapping key set
    table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
              .put(A, mapOf(P, Bytes.toLong(A), R, Bytes.toLong(C))).build());
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

  protected class IncThread extends Thread implements Closeable {
    final MetricsTable table;
    final byte[] row;
    final Map<byte[], Long> incrememts;
    int rounds;

    public IncThread(MetricsTable table, byte[] row, Map<byte[], Long> incrememts, int rounds) {
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

    @Override
    public void close() throws IOException {
      table.close();
    }
  }

  protected class IncAndGetThread extends Thread implements Closeable {
    final MetricsTable table;
    final byte[] row;
    final byte[] col;
    final long delta;
    int rounds;
    Long previous = 0L;

    public IncAndGetThread(MetricsTable table, byte[] row, byte[] col, long delta, int rounds) {
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

    @Override
    public void close() throws IOException {
      table.close();
    }
  }

  @Test
  public void testConcurrentIncrement() throws Exception {
    final MetricsTable table = getTable("testConcurrentIncrement");
    final int rounds = 500;
    Map<byte[], Long> inc1 = ImmutableMap.of(X, 1L, Y, 2L);
    Map<byte[], Long> inc2 = ImmutableMap.of(Y, 1L, Z, 2L);
    Collection<? extends Thread> threads = ImmutableList.of(new IncThread(table, A, inc1, rounds),
                                                  new IncThread(table, A, inc2, rounds),
                                                  new IncAndGetThread(table, A, Z, 5, rounds),
                                                  new IncAndGetThread(table, A, Z, 2, rounds));
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
      if (t instanceof Closeable) {
        ((Closeable) t).close();
      }
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
    table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
                .put(A, mapOf(B, 0L)).build());
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
    NavigableMap<byte[], SortedMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 1024; i++) {
      writes.put(Bytes.toBytes(i << 22), mapOf(A, Bytes.toLong(X)));
    }
    table.put(writes);
    // verify the first and last are there (sanity test for correctness of test logic)
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0x00000000), A));
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0xffc00000), A));

    List<byte[]> toDelete = ImmutableList.of(
      Bytes.toBytes(0xa1000000), Bytes.toBytes(0xb2000000), Bytes.toBytes(0xc3000000));
    // verify these three are there, and delete them
    for (byte[] row : toDelete) {
      Assert.assertArrayEquals(X, table.get(row, A));
      table.delete(row, new byte[][] {A});
    }
    // verify these three are now gone.
    for (byte[] row : toDelete) {
      Assert.assertNull(table.get(row, A));
    }
    // verify nothing else is gone by counting all entries in a scan
    Assert.assertEquals(1021, countRange(table, null, null));
  }

  @Test
  public void testDeleteIncrements() throws Exception {
    // note: this is pretty important test case for tables with counters, e.g. metrics
    MetricsTable table = getTable("testDeleteIncrements");
    // delete increment and increment again
    table.increment(A, ImmutableMap.of(B, 5L));
    table.increment(A, ImmutableMap.of(B, 2L));
    Assert.assertEquals(7L, Bytes.toLong(table.get(A, B)));
    table.delete(A, new byte[][]{B});
    Assert.assertNull(table.get(A, B));
    table.increment(A, ImmutableMap.of(B, 3L));
    Assert.assertEquals(3L, Bytes.toLong(table.get(A, B)));
  }

  private static int countRange(MetricsTable table, Integer start, Integer stop) throws Exception {
    Scanner scanner = table.scan(start == null ? null : Bytes.toBytes(start),
                                 stop == null ? null : Bytes.toBytes(stop), null);
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }
    return count;
  }

  @Test
  public void testFuzzyScan() throws Exception {
    MetricsTable table = getTable("testFuzzyScan");
    NavigableMap<byte[], SortedMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, mapOf(A, Bytes.toLong(X)));
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
    Scanner scanner = table.scan(null, null, filter);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      Assert.assertTrue(entry.getRow()[1] == 'b' && entry.getRow()[3] == 'b');
      Assert.assertEquals(1, entry.getColumns().size());
      Assert.assertTrue(entry.getColumns().containsKey(A));
      count++;
    }
    Assert.assertEquals(9, count);
  }

  protected  <T> SortedMap<byte[], T> mapOf(byte[] key, T value) {
    SortedMap<byte[], T> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    map.put(key, value);
    return map;
  }

  protected <T> SortedMap<byte[], T> mapOf(byte[] firstKey, T firstValue, byte[] secondKey, T secondValue) {
    SortedMap<byte[], T> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    map.put(firstKey, firstValue);
    map.put(secondKey, secondValue);
    return map;
  }
}
