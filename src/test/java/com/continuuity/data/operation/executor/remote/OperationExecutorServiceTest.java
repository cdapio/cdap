package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang.time.StopWatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class OperationExecutorServiceTest {

  static OperationExecutor opex, remote;
  static InMemoryZookeeper zookeeper;
  static CConfiguration config;
  static OperationExecutorService opexService;

  @BeforeClass
  public static void startService() throws Exception {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules());

    // get an instance of data fabric
    opex = injector.getInstance(OperationExecutor.class);

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
    while(watch.getTime() < 5000) {
      if (opexService.ruok()) break;
    }

    // now create a remote opex that connects to the service
    remote = new RemoteOperationExecutor(config);
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

  /** Tests Write, Read, ReadKey */
  @Test
  public void testWriteThenRead() throws Exception {
    // write a key/value with remote
    Write write = new Write("key".getBytes(), "value".getBytes());
    Assert.assertTrue(remote.execute(write));
    // read back with remote and compare
    ReadKey readKey = new ReadKey("key".getBytes());
    Assert.assertArrayEquals("value".getBytes(), remote.execute(readKey));
    // read back with actual and compare
    Assert.assertArrayEquals("value".getBytes(), opex.execute(readKey));

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

    // write a key/value
    Write write = new Write("deleted".getBytes(), "here".getBytes());
    Assert.assertTrue(remote.execute(write));

    // delete the row with remote
    Delete delete = new Delete("deleted".getBytes());
    Assert.assertTrue(remote.execute(delete));

    // read back key with remote and verify null
    ReadKey readKey = new ReadKey("deleted".getBytes());
    Assert.assertNull(remote.execute(readKey));

    // read back row with remote and verify null
    Read read = new Read("deleted".getBytes());
    Map<byte[], byte[]> columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertTrue(columns.keySet().contains(Operation.KV_COL));
    Assert.assertNull(columns.get(Operation.KV_COL));

    // read back one column and verify null
    read = new Read("deleted".getBytes(), "none".getBytes());
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertTrue(columns.keySet().contains("none".getBytes()));
    Assert.assertNull(columns.get("none".getBytes()));

    // read back two columns and verify null
    read = new Read("deleted".getBytes(),
        new byte[][] { "neither".getBytes(), "nor".getBytes() });
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertTrue(columns.keySet().contains("neither".getBytes()));
    Assert.assertTrue(columns.keySet().contains("nor".getBytes()));
    Assert.assertNull(columns.get("neither".getBytes()));
    Assert.assertNull(columns.get("nor".getBytes()));

    // read back column range and verify null
    ReadColumnRange readColumnRange = new ReadColumnRange(
        "deleted".getBytes(),
        "from".getBytes(),
        "to".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(0, columns.size());
  }

   /** Tests Write, ReadColumnRange, Delete */
  @Test
  public void testWriteThenRangeThenDelete() throws Exception {
    // write a bunch of columns with remote
    Write write = new Write("row".getBytes(),
        new byte[][] { "a".getBytes(), "b".getBytes(), "c".getBytes() },
        new byte[][] { "1".getBytes(), "2".getBytes(), "3".getBytes() });
    Assert.assertTrue(remote.execute(write));

    // read back all columns with remote (from "" ... "")
    ReadColumnRange readColumnRange =
        new ReadColumnRange("row".getBytes(), new byte[] { }, new byte[] { });
    Map<byte[], byte[]> columns = remote.execute(readColumnRange);
    // verify it is complete
    Assert.assertNotNull(columns);
    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back a sub-range (from aa to bb, should only return b)
    readColumnRange =
        new ReadColumnRange("row".getBytes(), "aa".getBytes(), "bb".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back all columns after aa, should return b and c
    readColumnRange =
        new ReadColumnRange("row".getBytes(), "aa".getBytes(), null);
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertNull(columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertArrayEquals("3".getBytes(), columns.get("c".getBytes()));

    // read back all columns before bb, should return a and b
    readColumnRange =
        new ReadColumnRange("row".getBytes(), null, "bb".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(2, columns.size());
    Assert.assertArrayEquals("1".getBytes(), columns.get("a".getBytes()));
    Assert.assertArrayEquals("2".getBytes(), columns.get("b".getBytes()));
    Assert.assertNull(columns.get("c".getBytes()));

    // read back a disjoint column range, verify it is empty by not null
    readColumnRange =
        new ReadColumnRange("row".getBytes(), "d".getBytes(), "e".getBytes());
    columns = remote.execute(readColumnRange);
    Assert.assertNotNull(columns);
    Assert.assertEquals(0, columns.size());

    // delete two of the columns with remote
    Delete delete = new Delete("row".getBytes(),
        new byte[][] { "a".getBytes(), "c".getBytes() });
    Assert.assertTrue(remote.execute(delete));

    // read back the column range again with remote
    readColumnRange = // reads everything
        new ReadColumnRange("row".getBytes(), "".getBytes(), null);
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
    Assert.assertEquals(1, columns.size());
    Assert.assertTrue(columns.containsKey("x".getBytes()));
    Assert.assertNull(columns.get("x".getBytes()));

    // compareAndSwap
    compareAndSwap = new CompareAndSwap("swap".getBytes(),
        "x".getBytes(), "2".getBytes(), "3".getBytes());
    Assert.assertFalse(remote.execute(compareAndSwap));

    // verify the row is still not there
    columns = remote.execute(read);
    Assert.assertNotNull(columns);
    Assert.assertEquals(1, columns.size());
    Assert.assertTrue(columns.containsKey("x".getBytes()));
    Assert.assertNull(columns.get("x".getBytes()));
  }

  /** clear the tables, then write a batch of keys, then readAllKeys */
  @Test
  public void testWriteBatchThenReadAllKeys() throws Exception  {
    // clear the fabric
    ClearFabric clearFabric = new ClearFabric(true, false, false);
    remote.execute(clearFabric);
    // list all keys, verify it is empty
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

  // TODO test batch, one that succeeds and one that fails
  // TODO test clearFabric
  // TODO test readAllKeys


  // TODO test Enqueue
  // TODO test getGroupId
  // TODO test Dequeue, with Hash and with Random
  // TODO test Ack, with multi and single
  // TODO test getQueueMeta

}
