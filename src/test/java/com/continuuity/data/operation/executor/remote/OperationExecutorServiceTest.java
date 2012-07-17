package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.*;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.commons.lang.time.StopWatch;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import scala.actors.threadpool.Arrays;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

public abstract class OperationExecutorServiceTest {

  static OperationExecutor local, remote;
  static InMemoryZookeeper zookeeper;
  static CConfiguration config;
  static OperationExecutorService opexService;

  public static void startService(Injector injector) throws Exception {

    // get an instance of data fabric
    OperationExecutor opex = injector.getInstance(OperationExecutor.class);

    // start an in-memory zookeeper and remember it in a config object
    zookeeper = new InMemoryZookeeper();
    config = CConfiguration.create();
    config.set(Constants.CFG_ZOOKEEPER_ENSEMBLE,
        zookeeper.getConnectionString());

    // find a free port to use for the service
    int port = PortDetector.findFreePort();
    config.setInt(Constants.CFG_DATA_OPEX_SERVER_PORT, port);

    // start an opex service
    opexService = new OperationExecutorService(opex);

    // and start it. Since start is blocking, we have to start async'ly
    new Thread () {
      public void run() {
        try {
          opexService.start(new String[] { }, config);
        } catch (Exception e) {
          System.err.println("Failed to start service: " + e.getMessage());
        }
      }
    }.start();

    // and wait until it has fully initialized
    StopWatch watch = new StopWatch();
    watch.start();
    while(watch.getTime() < 10000) {
      if (opexService.ruok()) break;
    }
    Assert.assertTrue("Operation Executor Service failed to come up within " +
        "10 seconds.", opexService.ruok());

    // now create a remote opex that connects to the service
    remote = new RemoteOperationExecutor(config);
    local = opex;
  }

  @AfterClass
  public static void stopService() throws Exception {

    // shutdown the opex service
    if (opexService != null)
      opexService.stop(true);

    // and shutdown the zookeeper
    if (zookeeper != null) {
      zookeeper.close();
    }
  }

  @Before
  public void clearFabric() {
    // before every test, clear data fabric.
    // otherwise it might see spurious entries from other tests :(
    this.local.execute(new ClearFabric(true, true, true));
  }

  /** Tests Write, Read, ReadKey */
  @Test
  public void testWriteThenRead() throws Exception {
    // write a key/value with remote
    Write write = new Write("key".getBytes(), "value".getBytes());
    Assert.assertTrue(remote.execute(write));
    // read back with remote and compare
    ReadKey readKey = new ReadKey("key".getBytes());
    Assert.assertArrayEquals("value".getBytes(), remote.execute(readKey));

    // write one columns with remote
    write = new Write("key1".getBytes(), "col1".getBytes(), "val1".getBytes());
    Assert.assertTrue(remote.execute(write));
    // read back with remote and compare
    Read read = new Read("key1".getBytes(), "col1".getBytes());
    Map<byte[], byte[]> columns = remote.execute(read);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("val1".getBytes(), columns.get("col1".getBytes()));

    // write two columns with remote
    write = new Write("key2".getBytes(),
        new byte[][] { "col2".getBytes(), "col3".getBytes() },
        new byte[][] { "val2".getBytes(), "val3".getBytes() });
    Assert.assertTrue(remote.execute(write));
    // read back with remote and compare
    read = new Read("key2".getBytes(),
        new byte[][] { "col2".getBytes(), "col3".getBytes() });
    columns = remote.execute(read);
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("val2".getBytes(), columns.get("col2".getBytes()));
    Assert.assertArrayEquals("val3".getBytes(), columns.get("col3".getBytes()));
  }

  /** Tests Increment, Read, ReadKey */
  @Test
  public void testIncrementThenRead() throws Exception {
    // increment a key/value with remote
    Increment increment = new Increment("count".getBytes(), 1);
    Assert.assertTrue(remote.execute(increment));
    // read back with remote and verify it is 1
    ReadKey readKey = new ReadKey("count".getBytes());
    byte[] value = remote.execute(readKey);
    Assert.assertNotNull(value);
    Assert.assertEquals(8, value.length);
    Assert.assertEquals(1L, ByteBuffer.wrap(value).asLongBuffer().get());

    // increment two columns with remote
    increment = new Increment("count".getBytes(),
        new byte[][] { "a".getBytes(), Operation.KV_COL },
        new long[] { 5L, 10L } );
    Assert.assertTrue(remote.execute(increment));
    // read back with remote and verify values
    Read read = new Read("count".getBytes(),
        new byte[][] { "a".getBytes(), Operation.KV_COL });
    Map<byte[], byte[]> columns = remote.execute(read);
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
    byte[] key = "mykey".getBytes();

    // write a key/value
    Write write = new Write(key, "here".getBytes());
    Assert.assertTrue(remote.execute(write));

    // delete the row with remote
    Delete delete = new Delete(key);
    Assert.assertTrue(remote.execute(delete));

    // read back key with remote and verify null
    ReadKey readKey = new ReadKey(key);
    Assert.assertNull(remote.execute(readKey));

    // read back row with remote and verify null
    Read read = new Read(key);
    Map<byte[], byte[]> columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertNull(columns.get(Operation.KV_COL));

    // read back one column and verify null
    read = new Read(key, "none".getBytes());
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertNull(columns.get("none".getBytes()));

    // read back two columns and verify null
    read = new Read(key,
        new byte[][] { "neither".getBytes(), "nor".getBytes() });
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertNull(columns.get("neither".getBytes()));
    Assert.assertNull(columns.get("nor".getBytes()));

    // read back column range and verify null
    ReadColumnRange readColumnRange = new ReadColumnRange(
        key,
        "from".getBytes(),
        "to".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(0, columns.size());
  }

   /** Tests Write, ReadColumnRange, Delete */
  @Test
  public void testWriteThenRangeThenDelete() throws Exception {
    final byte[] row = "row".getBytes();

    // write a bunch of columns with remote
    Write write = new Write(row,
        new byte[][] { "a".getBytes(), "b".getBytes(), "c".getBytes() },
        new byte[][] { "1".getBytes(), "2".getBytes(), "3".getBytes() });
    Assert.assertTrue(remote.execute(write));

    // read back all columns with remote (from "" ... "")
    ReadColumnRange readColumnRange =
        new ReadColumnRange(row, null, null);
    Map<byte[], byte[]> columns = remote.execute(readColumnRange);
    // verify it is complete
    Assert.assertNotNull(columns);
    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back a sub-range (from aa to bb, should only return b)
    readColumnRange =
        new ReadColumnRange(row, "aa".getBytes(), "bb".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back all columns after aa, should return b and c
    readColumnRange =
        new ReadColumnRange(row, "aa".getBytes(), null);
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back all columns before bb, should return a and b
    readColumnRange =
        new ReadColumnRange(row, null, "bb".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back a disjoint column range, verify it is empty by not null
    readColumnRange =
        new ReadColumnRange(row, "d".getBytes(), "e".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(0, columns.size());

    // delete two of the columns with remote
    Delete delete = new Delete(row,
        new byte[][] { "a".getBytes(), "c".getBytes() });
    Assert.assertTrue(remote.execute(delete));

    // read back the column range again with remote
    readColumnRange = // reads everything
        new ReadColumnRange(row, "".getBytes(), null);
    columns = remote.execute(readColumnRange);
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

    // write a column with a value
    Write write = new Write("swap".getBytes(), "x".getBytes(), "1".getBytes());
    Assert.assertTrue(remote.execute(write));

    // compareAndSwap with actual value
    CompareAndSwap compareAndSwap = new CompareAndSwap("swap".getBytes(),
        "x".getBytes(), "1".getBytes(), "2".getBytes());
    Assert.assertTrue(remote.execute(compareAndSwap));

    // read back value and verify it swapped
    Read read = new Read("swap".getBytes(), "x".getBytes());
    Map<byte[], byte[]> columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("2".getBytes(), columns.get("x".getBytes()));

    // compareAndSwap with different value
    compareAndSwap = new CompareAndSwap("swap".getBytes(),
        "x".getBytes(), "1".getBytes(), "3".getBytes());
    Assert.assertFalse(remote.execute(compareAndSwap));

    // read back and verify it has not swapped
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertArrayEquals("2".getBytes(), columns.get("x".getBytes()));

    // delete the row
    Delete delete = new Delete("swap".getBytes(), "x".getBytes());
    Assert.assertTrue(remote.execute(delete));

    // verify the row is not there any more, actually the read will return
    // a map with an entry for x, but with a null value
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertNull(columns.get("x".getBytes()));

    // compareAndSwap
    compareAndSwap = new CompareAndSwap("swap".getBytes(),
        "x".getBytes(), "2".getBytes(), "3".getBytes());
    Assert.assertFalse(remote.execute(compareAndSwap));

    // verify the row is still not there
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertNull(columns.get("x".getBytes()));
  }

  /** clear the tables, then write a batch of keys, then readAllKeys */
  @Test
  public void testWriteBatchThenReadAllKeys() throws Exception  {
    // list all keys, verify it is empty (@Before clears the data fabric)
    ReadAllKeys readAllKeys = new ReadAllKeys(0, 1);
    List<byte[]> keys = remote.execute(readAllKeys);
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
    BatchOperationResult result = remote.execute(writes);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isSuccess());

    // readAllKeys with > number of writes
    readAllKeys = new ReadAllKeys(0, 10);
    keys = remote.execute(readAllKeys);
    Assert.assertNotNull(keys);
    Assert.assertEquals(7, keys.size());

    // readAllKeys with < number of writes
    readAllKeys = new ReadAllKeys(0, 5);
    keys = remote.execute(readAllKeys);
    Assert.assertNotNull(keys);
    Assert.assertEquals(5, keys.size());

    // readAllKeys with offset and returning all
    readAllKeys = new ReadAllKeys(4, 4);
    keys = remote.execute(readAllKeys);
    Assert.assertNotNull(keys);
    Assert.assertEquals(3, keys.size());

    // readAllKeys with offset not returning all
    readAllKeys = new ReadAllKeys(2, 4);
    keys = remote.execute(readAllKeys);
    Assert.assertNotNull(keys);
    Assert.assertEquals(4, keys.size());

    // readAllKeys with offset returning none
    readAllKeys = new ReadAllKeys(7, 5);
    keys = remote.execute(readAllKeys);
    Assert.assertNotNull(keys);
    Assert.assertEquals(0, keys.size());
  }

  /** test batch, one that succeeds and one that fails */
  @Test
  public void testBatchSuccessAndFailure() throws Exception {

    // write a row for deletion within the batch, and one compareAndSwap
    Write write = new Write("b".getBytes(), "0".getBytes());
    Assert.assertTrue(remote.execute(write));
    write = new Write("d".getBytes(), "0".getBytes());
    Assert.assertTrue(remote.execute(write));
    // insert two elements into a queue, and dequeue one to get an ack
    Assert.assertTrue(
        remote.execute(new QueueEnqueue("q".getBytes(), "0".getBytes())));
    Assert.assertTrue(
        remote.execute(new QueueEnqueue("q".getBytes(), "1".getBytes())));
    QueueConsumer consumer = new QueueConsumer(0, 1, 1);
    QueueConfig config =
        new QueueConfig(new QueuePartitioner.RandomPartitioner(), true);
    QueueDequeue dequeue = new QueueDequeue("q".getBytes(), consumer, config);
    DequeueResult dequeueResult = remote.execute(dequeue);
    Assert.assertNotNull(dequeueResult);
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("0".getBytes(), dequeueResult.getValue());

    // create a batch of write, delete, increment, enqueue, ack, compareAndSwap
    List<WriteOperation> writes = Lists.newArrayList();
    writes.add(new Write("a".getBytes(), "1".getBytes()));
    writes.add(new Delete("b".getBytes()));
    writes.add(new Increment("c".getBytes(), 5));
    writes.add(new QueueEnqueue("qq".getBytes(), "1".getBytes()));
    writes.add(new QueueAck(
        "q".getBytes(), dequeueResult.getEntryPointer(), consumer));
    writes.add(new CompareAndSwap(
        "d".getBytes(), Operation.KV_COL, "1".getBytes(), "2".getBytes()));

    // execute the writes and verify it failed (compareAndSwap must fail)
    BatchOperationResult result = remote.execute(writes);
    Assert.assertNotNull(result);
    Assert.assertFalse(result.isSuccess());

    // verify that all operations were rolled back
    Assert.assertNull(remote.execute(new ReadKey("a".getBytes())));
    Assert.assertArrayEquals("0".getBytes(),
        remote.execute(new ReadKey("b".getBytes())));
    Assert.assertNull(remote.execute(new ReadKey("c".getBytes())));
    Assert.assertArrayEquals("0".getBytes(),
        remote.execute(new ReadKey("d".getBytes())));
    Assert.assertTrue(remote.execute(
        new QueueDequeue("qq".getBytes(), consumer, config)).isEmpty());
    // queue should return the same element until it is acked
    dequeueResult = remote.execute(
        new QueueDequeue("q".getBytes(), consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("0".getBytes(), dequeueResult.getValue());

    // set d to 1 to make compareAndSwap succeed
    Assert.assertTrue(
        remote.execute(new Write("d".getBytes(), "1".getBytes())));

    // execute the writes again and verify it suceeded
    result = remote.execute(writes);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isSuccess());

    // verify that all operations were performed
    Assert.assertArrayEquals("1".getBytes(),
        remote.execute(new ReadKey("a".getBytes())));
    Assert.assertNull(remote.execute(new ReadKey("b".getBytes())));
    Assert.assertArrayEquals(new byte[] { 0,0,0,0,0,0,0,5 },
        remote.execute(new ReadKey("c".getBytes())));
    Assert.assertArrayEquals("2".getBytes(),
        remote.execute(new ReadKey("d".getBytes())));
    dequeueResult = remote.execute(
        new QueueDequeue("qq".getBytes(), consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("1".getBytes(), dequeueResult.getValue());
    // queue should return the next element now that the previous one is acked
    dequeueResult = remote.execute(
        new QueueDequeue("q".getBytes(), consumer, config));
    Assert.assertTrue(dequeueResult.isSuccess());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertArrayEquals("1".getBytes(), dequeueResult.getValue());
  }


  /** test clearFabric */
  @Test
  public void testClearFabric() {
    final byte[] a = { 'a' };
    final byte[] x = { 'x' };
    final byte[] q = "queue://q".getBytes();
    final byte[] s = "stream://s".getBytes();

    // write to a table, a queue, and a stream
    Assert.assertTrue(remote.execute(new Write(a, x)));
    Assert.assertTrue(remote.execute(new QueueEnqueue(q, x)));
    Assert.assertTrue(remote.execute(new QueueEnqueue(s, x)));

    // clear everything
    remote.execute(new ClearFabric(true, true, true));

    // verify that all is gone
    Assert.assertNull(remote.execute(new ReadKey(a)));
    QueueConsumer consumer = new QueueConsumer(0, 1, 1);
    QueueConfig config = new
        QueueConfig(new QueuePartitioner.RandomPartitioner(), true);
    Assert.assertTrue(
        remote.execute(new QueueDequeue(q, consumer, config)).isEmpty());
    Assert.assertTrue(
        remote.execute(new QueueDequeue(s, consumer, config)).isEmpty());

    // write back all values
    Assert.assertTrue(remote.execute(new Write(a, x)));
    Assert.assertTrue(remote.execute(new QueueEnqueue(q, x)));
    Assert.assertTrue(remote.execute(new QueueEnqueue(s, x)));

    // clear only the tables
    remote.execute(new ClearFabric(true, false, false));

    // verify that the tables are gone, but queues and streams are there
    Assert.assertNull(remote.execute(new ReadKey(a)));
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(q, consumer, config)).getValue());
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(s, consumer, config)).getValue());

    // write back to the table
    Assert.assertTrue(remote.execute(new Write(a, x)));

    // clear only the queues
    remote.execute(new ClearFabric(false, true, false));

    // verify that the queues are gone, but tables and streams are there
    Assert.assertArrayEquals(x, remote.execute(new ReadKey(a)));
    Assert.assertTrue(
        remote.execute(new QueueDequeue(q, consumer, config)).isEmpty());
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(s, consumer, config)).getValue());

    // write back to the queue
    Assert.assertTrue(remote.execute(new QueueEnqueue(q, x)));

    // clear only the streams
    remote.execute(new ClearFabric(false, false, true));

    // verify that the streams are gone, but tables and queues are there
    Assert.assertArrayEquals(x, remote.execute(new ReadKey(a)));
    Assert.assertArrayEquals(x,
        remote.execute(new QueueDequeue(q, consumer, config)).getValue());
    Assert.assertTrue(
        remote.execute(new QueueDequeue(s, consumer, config)).isEmpty());
  }

  /** tests enqueue, getGroupId and dequeue with ack for different groups */
  @Test
  public void testEnqueueThenDequeueAndAckWithDifferentGroups()  {
    final byte[] q = "queue://q1".getBytes();

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
      Assert.assertTrue(remote.execute(enqueue));
      Assert.assertTrue(remote.execute(enqueue));
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
        new QueueConfig(new QueuePartitioner.HashPartitioner(), false);
    QueueConfig conf2 =
        new QueueConfig(new QueuePartitioner.RandomPartitioner(), true);

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
    Assert.assertTrue(remote.execute(new QueueAck(q, pointer11, cons11)));
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
    Assert.assertTrue(remote.execute(new QueueAck(q, pointer21, cons21)));
    // dequeue group 2 again
    next21 = remote.execute(new QueueDequeue(q, cons21, conf2));
    Assert.assertTrue(next21.isSuccess() && !next21.isEmpty());
    Assert.assertFalse(Arrays.equals(res21.getValue(), next21.getValue()));

    // verify that consumer 2 of group 2 can still not see new entries
    DequeueResult next22 = remote.execute(new QueueDequeue(q, cons22, conf2));
    Assert.assertTrue(next22.isSuccess() && !next22.isEmpty());
    Assert.assertArrayEquals(res22.getValue(), next22.getValue());

    // get queue meta with remote and opex, verify they are equal
    QueueAdmin.GetQueueMeta getQueueMeta = new QueueAdmin.GetQueueMeta(q);
    QueueAdmin.QueueMeta metaLocal = local.execute(getQueueMeta);
    QueueAdmin.QueueMeta metaRemote = remote.execute(getQueueMeta);
    Assert.assertNotNull(metaLocal);
    Assert.assertNotNull(metaRemote);
    Assert.assertEquals(metaLocal, metaRemote);
  }
}
