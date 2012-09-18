package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.log.Log;
import scala.actors.threadpool.Arrays;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

public abstract class OperationExecutorServiceTest extends
    OpexServiceTestBase {

  /** Tests Write, Read, ReadKey */
  @Test
  public void testWriteThenRead() throws Exception {
    final byte[] key = "tWTRkey".getBytes();
    final byte[] key1 = "tWTRkey1".getBytes();
    final byte[] key2 = "tWTRkey2".getBytes();
    byte[] value = { 'v', 'a', 'l', 'u', 'e' };

    // write a key/value with remote
    Write write = new Write(key, value);
    remote.execute(write);
    // read back with remote and compare
    ReadKey readKey = new ReadKey(key);
    Assert.assertArrayEquals(value, remote.execute(readKey).getValue());

    // write one columns with remote
    write = new Write(key1, "col1".getBytes(), "val1".getBytes());
    remote.execute(write);
    // read back with remote and compare
    Read read = new Read(key1, "col1".getBytes());
    Map<byte[], byte[]> columns = remote.execute(read).getValue();
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("val1".getBytes(), columns.get("col1".getBytes()));

    // write two columns with remote
    write = new Write(key2,
        new byte[][] { "col2".getBytes(), "col3".getBytes() },
        new byte[][] { "val2".getBytes(), "val3".getBytes() });
    remote.execute(write);
    // read back with remote and compare
    read = new Read(key2,
        new byte[][] { "col2".getBytes(), "col3".getBytes() });
    columns = remote.execute(read).getValue();
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("val2".getBytes(), columns.get("col2".getBytes()));
    Assert.assertArrayEquals("val3".getBytes(), columns.get("col3".getBytes()));
  }

  /** Tests Increment, Read, ReadKey */
  @Test
  public void testIncrementThenRead() throws Exception {
    final byte[] count = "tITRcount".getBytes();

    // increment a key/value with remote
    Increment increment = new Increment(count, 1);
    remote.execute(increment);
    // read back with remote and verify it is 1
    ReadKey readKey = new ReadKey(count);
    byte[] value = remote.execute(readKey).getValue();
    Assert.assertNotNull(value);
    Assert.assertEquals(8, value.length);
    Assert.assertEquals(1L, ByteBuffer.wrap(value).asLongBuffer().get());

    // increment two columns with remote
    increment = new Increment(count,
        new byte[][] { "a".getBytes(), Operation.KV_COL },
        new long[] { 5L, 10L } );
    remote.execute(increment);
    // read back with remote and verify values
    Read read = new Read(count,
        new byte[][] { "a".getBytes(), Operation.KV_COL });
    Map<byte[], byte[]> columns = remote.execute(read).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertEquals(5L,
        ByteBuffer.wrap(columns.get("a".getBytes())).asLongBuffer().get());
    Assert.assertEquals(11L,
        ByteBuffer.wrap(columns.get(Operation.KV_COL)).asLongBuffer().get());
  }

  /** Tests read for non-existent key */
  @Test
  public void testDeleteThenRead() throws Exception {

    // this is the key we will use
    final byte[] key = "tDTRkey".getBytes();

    // write a key/value
    Write write = new Write(key, "here".getBytes());
    remote.execute(write);

    // delete the row with remote
    Delete delete = new Delete(key);
    remote.execute(delete);

    // read back key with remote and verify null
    ReadKey readKey = new ReadKey(key);
    Assert.assertTrue(remote.execute(readKey).isEmpty());

    // read back row with remote and verify null
    Read read = new Read(key);
    Assert.assertTrue(remote.execute(read).isEmpty());

    // read back one column and verify null
    read = new Read(key, "none".getBytes());
    Assert.assertTrue(remote.execute(read).isEmpty());

    // read back two columns and verify null
    read = new Read(key,
        new byte[][] { "neither".getBytes(), "nor".getBytes() });
    Assert.assertTrue(remote.execute(read).isEmpty());

    // read back column range and verify null
    ReadColumnRange readColumnRange = new ReadColumnRange(
        key,
        "from".getBytes(),
        "to".getBytes());
    Assert.assertTrue(remote.execute(readColumnRange).isEmpty());
  }

   /** Tests Write, ReadColumnRange, Delete */
  @Test
  public void testWriteThenRangeThenDelete() throws Exception {

    final byte[] row = "tWTRTDrow".getBytes();

    // write a bunch of columns with remote
    Write write = new Write(row,
        new byte[][] { "a".getBytes(), "b".getBytes(), "c".getBytes() },
        new byte[][] { "1".getBytes(), "2".getBytes(), "3".getBytes() });
    remote.execute(write);

    // read back all columns with remote (from "" ... "")
    ReadColumnRange readColumnRange =
        new ReadColumnRange(row, null, null);
    Map<byte[], byte[]> columns = remote.execute(readColumnRange).getValue();
    // verify it is complete
    Assert.assertNotNull(columns);
    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back a sub-range (from aa to bb, should only return b)
    readColumnRange =
        new ReadColumnRange(row, "aa".getBytes(), "bb".getBytes());
    columns = remote.execute(readColumnRange).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back all columns after aa, should return b and c
    readColumnRange =
        new ReadColumnRange(row, "aa".getBytes(), null);
    columns = remote.execute(readColumnRange).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back all columns before bb, should return a and b
    readColumnRange =
        new ReadColumnRange(row, null, "bb".getBytes());
    columns = remote.execute(readColumnRange).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back a disjoint column range, verify it is empty by not null
    readColumnRange =
        new ReadColumnRange(row, "d".getBytes(), "e".getBytes());
    Assert.assertTrue(remote.execute(readColumnRange).isEmpty());

    // delete two of the columns with remote
    Delete delete = new Delete(row,
        new byte[][] { "a".getBytes(), "c".getBytes() });
    remote.execute(delete);

    // read back the column range again with remote
    readColumnRange = // reads everything
        new ReadColumnRange(row, null, null);
    columns = remote.execute(readColumnRange).getValue();
    Assert.assertNotNull(columns);
    // verify the two are gone
    Assert.assertEquals(1, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));
  }

  /** Tests Write, CompareAndSwap, Read */
  @Test
  public void testWriteThenSwapThenRead() throws Exception {

    final byte[] key = "tWTSTRkey".getBytes();

    // write a column with a value
    Write write = new Write(key, "x".getBytes(), "1".getBytes());
    remote.execute(write);

    // compareAndSwap with actual value
    CompareAndSwap compareAndSwap = new CompareAndSwap(key,
        "x".getBytes(), "1".getBytes(), "2".getBytes());
    remote.execute(compareAndSwap);

    // read back value and verify it swapped
    Read read = new Read(key, "x".getBytes());
    Map<byte[], byte[]> columns = remote.execute(read).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("2".getBytes(), columns.get("x".getBytes()));

    // compareAndSwap with different value
    compareAndSwap = new CompareAndSwap(key,
        "x".getBytes(), "1".getBytes(), "3".getBytes());
    try {
      remote.execute(compareAndSwap);
      Assert.fail("Expected compare-and-swap to fail.");
    } catch (OperationException e) {
      //expected
    }

    // read back and verify it has not swapped
    columns = remote.execute(read).getValue();
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("2".getBytes(), columns.get("x".getBytes()));

    // delete the row
    Delete delete = new Delete(key, "x".getBytes());
    remote.execute(delete);

    // verify the row is not there any more, actually the read will return
    // a map with an entry for x, but with a null value
    Assert.assertTrue(remote.execute(read).isEmpty());

    // compareAndSwap
    compareAndSwap = new CompareAndSwap(key,
        "x".getBytes(), "2".getBytes(), "3".getBytes());
    try {
      remote.execute(compareAndSwap);
      Assert.fail("Expected compare-and-swap to fail.");
    } catch (OperationException e) {
      //expected
    }

    // verify the row is still not there
    Assert.assertTrue(remote.execute(read).isEmpty());
  }

  /** clear the tables, then write a batch of keys, then readAllKeys */
  @Test
  public void testWriteBatchThenReadAllKeys() throws Exception  {
    // clear all tables, otherwise we will get keys from other tests
    // mingled into the responses for ReadAllKeys
    remote.execute(new ClearFabric(true, false, false));

    // list all keys, verify it is empty (@Before clears the data fabric)
    ReadAllKeys readAllKeys = new ReadAllKeys(0, 1);
    List<byte[]> keys = remote.execute(readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(0, keys.size());
    // write a batch, some k/v, some single column, some multi-column
    List<WriteOperation> writes = Lists.newArrayList();
    writes.add(new Write("a".getBytes(), "1".getBytes()));
    writes.add(new Write("b".getBytes(), "2".getBytes()));
    writes.add(new Write("c".getBytes(), "3".getBytes()));
    writes.add(new Write("d".getBytes(), "x".getBytes(), "4".getBytes()));
    writes.add(new Write("e".getBytes(), "y".getBytes(), "5".getBytes()));
    writes.add(new Write("f".getBytes(), "z".getBytes(), "6".getBytes()));
    writes.add(new Write("g".getBytes(),
        new byte[][] { "x".getBytes(), "y".getBytes(), "z".getBytes() },
        new byte[][] { "7".getBytes(), "8".getBytes(), "9".getBytes() }));
    remote.execute(writes);

    // readAllKeys with > number of writes
    readAllKeys = new ReadAllKeys(0, 10);
    keys = remote.execute(readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(7, keys.size());

    // readAllKeys with < number of writes
    readAllKeys = new ReadAllKeys(0, 5);
    keys = remote.execute(readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(5, keys.size());

    // readAllKeys with offset and returning all
    readAllKeys = new ReadAllKeys(4, 4);
    keys = remote.execute(readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(3, keys.size());

    // readAllKeys with offset not returning all
    readAllKeys = new ReadAllKeys(2, 4);
    keys = remote.execute(readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(4, keys.size());

    // readAllKeys with offset returning none
    readAllKeys = new ReadAllKeys(7, 5);
    keys = remote.execute(readAllKeys).getValue();
    Assert.assertNotNull(keys);
    Assert.assertEquals(0, keys.size());
  }

  /** test batch, one that succeeds and one that fails */
  @Test
  public void testBatchSuccessAndFailure() throws Exception {

    final byte[] keyA = "tBSAF.a".getBytes();
    final byte[] keyB = "tBSAF.b".getBytes();
    final byte[] keyC = "tBSAF.c".getBytes();
    final byte[] keyD = "tBSAF.d".getBytes();
    final byte[] q = "tBSAF.q".getBytes();
    final byte[] qq = "tBSAF.qq".getBytes();

    // write a row for deletion within the batch, and one compareAndSwap
    Write write = new Write(keyB, "0".getBytes());
    remote.execute(write);
    write = new Write(keyD, "0".getBytes());
    remote.execute(write);
    // insert two elements into a queue, and dequeue one to get an ack
    remote.execute(new QueueEnqueue(q, "0".getBytes()));
    remote.execute(new QueueEnqueue(q, "1".getBytes()));
    QueueConsumer consumer = new QueueConsumer(0, 1, 1);
    QueueConfig config =
        new QueueConfig(PartitionerType.RANDOM, true);
    QueueDequeue dequeue = new QueueDequeue(q, consumer, config);
    DequeueResult dequeueResult = remote.execute(dequeue);
    Assert.assertNotNull(dequeueResult);
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("0".getBytes(), dequeueResult.getValue());

    // create a batch of write, delete, increment, enqueue, ack, compareAndSwap
    List<WriteOperation> writes = Lists.newArrayList();
    writes.add(new Write(keyA, "1".getBytes()));
    writes.add(new Delete(keyB));
    writes.add(new Increment(keyC, 5));
    writes.add(new QueueEnqueue(qq, "1".getBytes()));
    writes.add(new QueueAck(
        q, dequeueResult.getEntryPointer(), consumer));
    writes.add(new CompareAndSwap(
        keyD, Operation.KV_COL, "1".getBytes(), "2".getBytes()));

    // execute the writes and verify it failed (compareAndSwap must fail)
    try {
      remote.execute(writes);
      Assert.fail("expected coompare-and-swap conflict");
    } catch (OperationException e) {
      // expected
    }

    // verify that all operations were rolled back
    Assert.assertTrue(remote.execute(new ReadKey(keyA)).isEmpty());
    Assert.assertArrayEquals("0".getBytes(),
        remote.execute(new ReadKey(keyB)).getValue());
    Assert.assertTrue(remote.execute(new ReadKey(keyC)).isEmpty());
    Assert.assertArrayEquals("0".getBytes(),
        remote.execute(new ReadKey(keyD)).getValue());
    Assert.assertTrue(remote.execute(
        new QueueDequeue(qq, consumer, config)).isEmpty());
    // queue should return the same element until it is acked
    dequeueResult = remote.execute(
        new QueueDequeue(q, consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("0".getBytes(), dequeueResult.getValue());

    // set d to 1 to make compareAndSwap succeed
    remote.execute(new Write(keyD, "1".getBytes()));

    // execute the writes again and verify it suceeded
    remote.execute(writes);

    // verify that all operations were performed
    Assert.assertArrayEquals("1".getBytes(),
        remote.execute(new ReadKey(keyA)).getValue());
    Assert.assertTrue(remote.execute(new ReadKey(keyB)).isEmpty());
    Assert.assertArrayEquals(new byte[] { 0,0,0,0,0,0,0,5 },
        remote.execute(new ReadKey(keyC)).getValue());
    Assert.assertArrayEquals("2".getBytes(),
        remote.execute(new ReadKey(keyD)).getValue());
    dequeueResult = remote.execute(
        new QueueDequeue(qq, consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("1".getBytes(), dequeueResult.getValue());
    // queue should return the next element now that the previous one is acked
    dequeueResult = remote.execute(
        new QueueDequeue(q, consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("1".getBytes(), dequeueResult.getValue());
  }


  /** test clearFabric */
  @Test
  public void testClearFabric() throws Exception {
    final byte[] a = "tCFa".getBytes();
    final byte[] x = { 'x' };
    final byte[] q = "queue://tCF/q".getBytes();
    final byte[] s = "stream://tCF/s".getBytes();

    // write to a table, a queue, and a stream
    remote.execute(new Write(a, x));
    remote.execute(new QueueEnqueue(q, x));
    remote.execute(new QueueEnqueue(s, x));

    // clear everything
    remote.execute(new ClearFabric(true, true, true));

    // verify that all is gone
    Assert.assertTrue(remote.execute(new ReadKey(a)).isEmpty());
    QueueConsumer consumer = new QueueConsumer(0, 1, 1);
    QueueConfig config = new QueueConfig(PartitionerType.RANDOM, true);
    Assert.assertTrue(
        remote.execute(new QueueDequeue(q, consumer, config)).isEmpty());
    Assert.assertTrue(
        remote.execute(new QueueDequeue(s, consumer, config)).isEmpty());

    // write back all values
    remote.execute(new Write(a, x));
    remote.execute(new QueueEnqueue(q, x));
    remote.execute(new QueueEnqueue(s, x));

    // clear only the tables
    remote.execute(new ClearFabric(true, false, false));

    // verify that the tables are gone, but queues and streams are there
    Assert.assertTrue(remote.execute(new ReadKey(a)).isEmpty());
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(q, consumer, config)).getValue());
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(s, consumer, config)).getValue());

    // write back to the table
    remote.execute(new Write(a, x));

    // clear only the queues
    remote.execute(new ClearFabric(false, true, false));

    // verify that the queues are gone, but tables and streams are there
    Assert.assertArrayEquals(x, remote.execute(new ReadKey(a)).getValue());
    Assert.assertTrue(
        remote.execute(new QueueDequeue(q, consumer, config)).isEmpty());
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(s, consumer, config)).getValue());

    // write back to the queue
    remote.execute(new QueueEnqueue(q, x));

    // clear only the streams
    remote.execute(new ClearFabric(false, false, true));

    // verify that the streams are gone, but tables and queues are there
    Assert.assertArrayEquals(x, remote.execute(new ReadKey(a)).getValue());
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(q, consumer, config)).getValue());
    Assert.assertTrue(
        remote.execute(new QueueDequeue(s, consumer, config)).isEmpty());
  }

  /** tests enqueue, getGroupId and dequeue with ack for different groups */
  @Test
  public void testEnqueueThenDequeueAndAckWithDifferentGroups() throws
      Exception {
    final byte[] q = "queue://tWTDAAWDG/q".getBytes();

    // enqueue a bunch of entries, each one twice.
    // why twice? with hash partitioner, the same value will go to the same
    // consumer twice. With random partitioner, they go in the order of request
    // insert enough to be sure that even with hash partitioning, none of the
    // consumers will run out of entries to dequeue
    Random rand = new Random(42);
    int prev = 0, i = 0;
    while (i < 100) {
      int next = rand.nextInt(1000);
      if (next == prev) continue;
      byte[] value = Integer.toString(next).getBytes();
      QueueEnqueue enqueue = new QueueEnqueue(q, value);
      remote.execute(enqueue);
      remote.execute(enqueue);
      prev = next;
      i++;
    }
    // get two groupids
    long id1 = remote.execute(new QueueAdmin.GetGroupID(q));
    long id2 = remote.execute(new QueueAdmin.GetGroupID(q));
    Assert.assertFalse(id1 == id2);

    // create 2 consumers for each groupId
    QueueConsumer cons11 = new QueueConsumer(0, id1, 2);
    QueueConsumer cons12 = new QueueConsumer(1, id1, 2);
    QueueConsumer cons21 = new QueueConsumer(0, id2, 2);
    QueueConsumer cons22 = new QueueConsumer(1, id2, 2);

    // creeate two configs, one hash, one random, one single, one multi
    QueueConfig conf1 =
        new QueueConfig(PartitionerType.HASH_ON_VALUE, false);
    QueueConfig conf2 =
        new QueueConfig(PartitionerType.RANDOM, true);

    // dequeue with each consumer
    DequeueResult res11 = remote.execute(new QueueDequeue(q, cons11, conf1));
    DequeueResult res12 = remote.execute(new QueueDequeue(q, cons12, conf1));
    DequeueResult res21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    DequeueResult res22 = remote.execute(new QueueDequeue(q, cons22, conf2));

    // verify that all results are successful
    Assert.assertTrue(res11.isSuccess() && !res11.isEmpty());
    Assert.assertTrue(res12.isSuccess() && !res12.isEmpty());
    Assert.assertTrue(res21.isSuccess() && !res21.isEmpty());
    Assert.assertTrue(res22.isSuccess() && !res22.isEmpty());

    // verify that the values from group 1 are different (hash partitioner)
    Assert.assertFalse(Arrays.equals(res11.getValue(), res12.getValue()));
    // and that the two values for group 2 are equal (random partitioner)
    Assert.assertArrayEquals(res21.getValue(), res22.getValue());

    // verify that group1 (multi-entry config) can dequeue more elements
    DequeueResult next11 = remote.execute(new QueueDequeue(q, cons11, conf1));
    Assert.assertTrue(next11.isSuccess() && !next11.isEmpty());
    // for the second read we expect the same value again (enqueued twice)
    Assert.assertArrayEquals(res11.getValue(), next11.getValue());
    // but if we dequeue again, we should see a different one.
    next11 = remote.execute(new QueueDequeue(q, cons11, conf1));
    Assert.assertTrue(next11.isSuccess() && !next11.isEmpty());
    Assert.assertFalse(Arrays.equals(res11.getValue(), next11.getValue()));

    // verify that group2 (single-entry config) cannot dequeue more elements
    DequeueResult next21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    // other than for group1 above, we would see a different value right
    // away (because the first two, identical value have been dequeued)
    // but this queue is in single-entry mode and requires an ack before
    // the next element can be read. Thus we should see the same value
    Assert.assertArrayEquals(res21.getValue(), next21.getValue());
    // just to be sure, do it again
    next21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertArrayEquals(res21.getValue(), next21.getValue());

    // ack group 1 to verify that it did not affect group 2
    QueueEntryPointer pointer11 = res11.getEntryPointer();
    remote.execute(new QueueAck(q, pointer11, cons11));
    // dequeue group 2 again
    next21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertArrayEquals(res21.getValue(), next21.getValue());
    // just to be sure, do it twice
    next21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertArrayEquals(res21.getValue(), next21.getValue());

    // ack group 2, consumer 1,
    QueueEntryPointer pointer21 = res21.getEntryPointer();
    remote.execute(new QueueAck(q, pointer21, cons21));
    // dequeue group 2 again
    next21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertFalse(Arrays.equals(res21.getValue(), next21.getValue()));

    // verify that consumer 2 of group 2 can still not see new entries
    DequeueResult next22 = remote.execute(new QueueDequeue(q, cons22, conf2));
    Assert.assertTrue(next22.isSuccess() && !next22.isEmpty());
    Assert.assertArrayEquals(res22.getValue(), next22.getValue());

    // For now, disable this, because it is not implemented in native queues
    // TODO reenable after native queues implement getQueueMeta
    // get queue meta with remote and opex, verify they are equal
    /*
    QueueAdmin.GetQueueMeta getQueueMeta = new QueueAdmin.GetQueueMeta(q);
    QueueAdmin.QueueMeta metaLocal = local.execute(getQueueMeta).getValue();
    QueueAdmin.QueueMeta metaRemote = remote.execute(getQueueMeta).getValue();
    Assert.assertNotNull(metaLocal);
    Assert.assertNotNull(metaRemote);
    Assert.assertEquals(metaLocal, metaRemote);
    */
  }

  /*
   * Test that the remot eopex is thread safe:
   * Run many threads that perform reads and writes concurrently.
   * If the opex is not thread-safe, some of them will corrupt each other's
   * network communication.
   */
  @Test
  public void testMultiThreaded() {
    int numThreads = 5;
    int numWritesPerThread = 50;

    OpexThread[] threads = new OpexThread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      OpexThread ti = new OpexThread(i, numWritesPerThread);
      Log.debug("Starting thread " + i);
      ti.start();
      Log.debug("Thread " + i + " is running");
      threads[i] = ti;
    }
    for (int i = 0; i < numThreads; i++) {
      try {
        threads[i].join();
        Assert.assertEquals(numWritesPerThread, threads[i].count);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Assert.fail("join with thread " + i + " was interrupted");
      }
    }
  }

  class OpexThread extends Thread {
    int times;
    int id;
    int count = 0;
    OpexThread(int id, int times) {
      this.id = id;
      this.times = times;
    }
    public void run() {
      try {
        for (int i = 0; i < this.times; i++) {
          byte[] key = (id + "-" + i).getBytes();
          byte[] value = Integer.toString(i).getBytes();
          Log.debug("Thread " + id + " writing #" + i);
          Write write = new Write(key, value);
          remote.execute(write);
          Log.debug("Thread " + id + " reading #" + i);
          ReadKey readKey = new ReadKey(key);
          Assert.assertArrayEquals(value, remote.execute(readKey).getValue());
          count++;
        }
      } catch (Exception e) {
        Assert.fail("Exception in thread " + id + ": " + e.getMessage());
      }
    }
  }
}
