package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.table.Scanner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * tests metrics table.
 */
public abstract class MetricsTableTest {
  private static final byte ONES = 0x7f;

  // subclasses must instantiate this in their @BeforeClass method
  protected static DataSetAccessor dsAccessor = null;

  protected MetricsTable getTable(String name) throws Exception {
    dsAccessor.getDataSetManager(MetricsTable.class, DataSetAccessor.Namespace.SYSTEM).create(name);
    return dsAccessor.getDataSetClient(name, MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  static final byte[] A = { 'a' };
  static final byte[] B = { 'b' };
  static final byte[] C = { 'c' };
  static final byte[] P = { 'p' };
  static final byte[] Q = { 'q' };
  static final byte[] R = { 'r' };
  static final byte[] X = { 'x' };
  static final byte[] Y = { 'y' };
  static final byte[] Z = { 'z' };

  @Test
  public void testGetPutSwap() throws Exception {
    MetricsTable table = getTable("testGetPutSwap");
    // put two rows
    table.put(ImmutableMap.of(A, (Map<byte[], byte[]>) ImmutableMap.of(P, X, Q, Y),
                              B, ImmutableMap.of(P, X, R, Z)));
    Assert.assertArrayEquals(X, table.get(A, P).getValue());
    Assert.assertArrayEquals(Y, table.get(A, Q).getValue());
    Assert.assertTrue(table.get(A, R).isEmpty());
    Assert.assertArrayEquals(X, table.get(B, P).getValue());
    Assert.assertArrayEquals(Z, table.get(B, R).getValue());
    Assert.assertTrue(table.get(B, Q).isEmpty());

    // put one row again, with overlapping key set
    table.put(ImmutableMap.of(A, (Map<byte[], byte[]>) ImmutableMap.of(P, A, R, C)));
    Assert.assertArrayEquals(A, table.get(A, P).getValue());
    Assert.assertArrayEquals(Y, table.get(A, Q).getValue());
    Assert.assertArrayEquals(C, table.get(A, R).getValue());

    // compare and swap an existing value, successfully
    Assert.assertTrue(table.swap(A, P, A, B));
    Assert.assertArrayEquals(B, table.get(A, P).getValue());
    // compare and swap an existing value that does not match
    Assert.assertFalse(table.swap(A, P, A, B));
    Assert.assertArrayEquals(B, table.get(A, P).getValue());
    // compare and swap an existing value that does not exist
    Assert.assertFalse(table.swap(B, Q, A, B));
    Assert.assertTrue(table.get(B, Q).isEmpty());

    // compare and delete an existing value, successfully
    Assert.assertTrue(table.swap(A, P, B, null));
    Assert.assertTrue(table.get(A, P).isEmpty());
    // compare and delete an existing value that does not match
    Assert.assertFalse(table.swap(A, Q, A, null));
    Assert.assertArrayEquals(Y, table.get(A, Q).getValue());

    // compare and swap a null value, successfully
    Assert.assertTrue(table.swap(A, P, null, Z));
    Assert.assertArrayEquals(Z, table.get(A, P).getValue());
    // compare and swap a null value, successfully
    Assert.assertFalse(table.swap(A, Q, null, Z));
    Assert.assertArrayEquals(Y, table.get(A, Q).getValue());
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
    private final int rounds;

    SwapThread(MetricsTable table, byte[] row, byte[] col, AtomicInteger[] counts, int rounds) {
      this.table = table;
      this.row = row;
      this.col = col;
      this.counts = counts;
      this.rounds = rounds;
    }

    public void run() {
      for (int i = 0; i < rounds; i++) {
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
    final int rounds = 500;
    table.put(ImmutableMap.of(A, Collections.singletonMap(B, Bytes.toBytes(0))));
    AtomicInteger[] counts = { new AtomicInteger(), new AtomicInteger() }; // [0] for success, [1] for failures
    Thread t1 = new SwapThread(table, A, B, counts, rounds);
    Thread t2 = new SwapThread(table, A, B, counts, rounds);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    Assert.assertArrayEquals(Bytes.toBytes(rounds), table.get(A, B).getValue());
    Assert.assertEquals(rounds, counts[0].get()); // number of successful swaps
    Assert.assertEquals(rounds, counts[1].get()); // number of failed swaps
  }

  @Test
  public void testDelete() throws Exception {
    MetricsTable table = getTable("testDelete");
    Map<byte[], Map<byte[], byte[]>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 1024; i++) {
      writes.put(Bytes.toBytes(i << 22), ImmutableMap.of(A, X));
    }
    table.put(writes);
    // verify the first and last are there (sanity test for correctness of test logic)
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0x00000000), A).getValue());
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0xffc00000), A).getValue());

    List<byte[]> toDelete = ImmutableList.of(
      Bytes.toBytes(0xa1000000), Bytes.toBytes(0xb2000000), Bytes.toBytes(0xc3000000));
    // verify these three are there, we will soon delete them
    for (byte[] row : toDelete) {
      Assert.assertArrayEquals(X, table.get(row, A).getValue());
    }
    // delete these three
    table.delete(toDelete);
    // verify these three are now gone.
    for (byte[] row : toDelete) {
      Assert.assertTrue(table.get(row, A).isEmpty());
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
    Map<byte[], Map<byte[], byte[]>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, ImmutableMap.of(A, X, B, Y));
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
      ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();
      if (entry == null) {
        break;
      }
      Assert.assertTrue(entry.getFirst()[1] == 'b' && entry.getFirst()[3] == 'b');
      Assert.assertEquals(1, entry.getSecond().size());
      Assert.assertTrue(entry.getSecond().containsKey(A));
      Assert.assertFalse(entry.getSecond().containsKey(B));
      count++;
    }
    Assert.assertEquals(9, count);
  }

  @Test
  public void testRangeDeleteWithoutFilter() throws Exception {
    MetricsTable table = getTable("rangeDelete");
    Map<byte[], Map<byte[], byte[]>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, ImmutableMap.of(A, X, B, Y));
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
      ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();
      if (entry == null) {
        break;
      }
      byte[] row = entry.getFirst();
      if (row[0] == 'a' && row[1] == 'b' && row[3] == 'b') {
        Assert.assertFalse(entry.getSecond().containsKey(A));
        Assert.assertEquals(1, entry.getSecond().size());
      } else {
        Assert.assertTrue(entry.getSecond().containsKey(A));
        Assert.assertTrue(entry.getSecond().containsKey(B));
        Assert.assertEquals(2, entry.getSecond().size());
      }
      count++;
    }
    Assert.assertEquals(81, count);
  }

  @Test
  public void testRangeDeleteWithFilter() throws Exception {
    MetricsTable table = getTable("rangeDelete");
    Map<byte[], Map<byte[], byte[]>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, ImmutableMap.of(A, X, B, Y));
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
      ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();
      if (entry == null) {
        break;
      }
      if (entry.getFirst()[0] == 'a') {
        Assert.assertFalse(entry.getSecond().containsKey(A));
        Assert.assertEquals(1, entry.getSecond().size());
      } else {
        Assert.assertTrue(entry.getSecond().containsKey(A));
        Assert.assertTrue(entry.getSecond().containsKey(B));
        Assert.assertEquals(2, entry.getSecond().size());
      }
      count++;
    }
    Assert.assertEquals(81, count);
  }

}
