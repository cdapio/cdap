/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.TransactionException;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor.WriteTransactionResult;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.table.Scanner;
import com.continuuity.data.util.OperationUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public abstract class TestOmidTransactionalOperationExecutor {

  private OmidTransactionalOperationExecutor executor;

  /** to get the singleton operation executor, always returns the same. */
  protected abstract OmidTransactionalOperationExecutor getOmidExecutor();

  /** to support testing, return a new executor each time. */
  // this would be needed to simulate multi-node opex, for instance to test
  // that a named table survives a shutdown and a new executor will open it
  // instead of recreating it, also for testing that multiple executors
  // will not create the same table multiple times
  // however, our data fabric modules return singletons.
  //protected abstract OmidTransactionalOperationExecutor getNewExecutor();

  static OperationContext context = OperationUtil.DEFAULT;

  @Before
  public void initialize() {
    executor = getOmidExecutor();
  }

  /**
   * Every subclass should implement this to verify that injection works and uses the correct table type.
   */
  @Test
  public abstract void testInjection();

  @Test
  public void testSimple() throws Exception {

    byte [] key = Bytes.toBytes("keytestSimple");
    byte [] value = Bytes.toBytes("value");

    // start a transaction
    Transaction pointer = executor.startTransaction(true);

    // write to a key
    WriteTransactionResult txResult = executor.write(context, new Write(key, kvcol, value), pointer);
    assertTrue(txResult.success);
    executor.addToTransaction(pointer, txResult.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // commit
    assertTrue(executor.commitTransaction(pointer).isSuccess());

    // read should see the write
    OperationResult<Map<byte [], byte[]>> readValue = executor.execute(context, new Read(key, kvcol));
    assertNotNull(readValue);
    assertFalse(readValue.isEmpty());
    assertArrayEquals(value, readValue.getValue().get(kvcol));
  }

  private static final byte[] kvcol = Operation.KV_COL;

  @Test
  public void testClearFabric() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;
    byte [] dataKey = Bytes.toBytes("dataKey");
    byte [] queueKey = Bytes.toBytes("queue://queueKey");
    byte [] streamKey = Bytes.toBytes("stream://streamKey");

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer qConsumer = new QueueConsumer(0, 0, 1, config);
    QueueConsumer sConsumer = new QueueConsumer(0, 0, 1, config);
    this.executor.execute(context, new QueueConfigure(queueKey, qConsumer));
    this.executor.execute(context, new QueueConfigure(streamKey, sConsumer));

    // insert to all three types
    executor.commit(context, new Write(dataKey, kvcol, dataKey));
    executor.commit(context, new QueueEnqueue(queueKey, new QueueEntry(queueKey)));
    executor.commit(context, new QueueEnqueue(streamKey, new QueueEntry(streamKey)));

    // read data from all three types
    assertTrue(Bytes.equals(dataKey,
        executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol)));
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, qConsumer, config)).getEntry().getData()));
    assertEquals(Bytes.toString(streamKey),
                 Bytes.toString(executor.execute(context,
                                                 new QueueDequeue(streamKey, sConsumer, config)).getEntry().getData()));
//    assertTrue(Bytes.equals(streamKey, executor.execute(context,
//        new QueueDequeue(streamKey, sConsumer, config)).getEntry().getData()));

    // clear data only
    executor.execute(context, new ClearFabric(ClearFabric.ToClear.DATA));

    // data is gone, queues still there
    assertTrue(executor.execute(context, new Read(dataKey, kvcol)).isEmpty());
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, qConsumer, config)).getEntry().getData()));
    assertTrue(Bytes.equals(streamKey, executor.execute(context,
        new QueueDequeue(streamKey, sConsumer, config)).getEntry().getData()));

    // clear queues and streams
    executor.execute(context, new ClearFabric(Arrays.asList(
        ClearFabric.ToClear.QUEUES, ClearFabric.ToClear.STREAMS)));

    // everything is gone
    this.executor.execute(context, new QueueConfigure(queueKey, qConsumer));
    this.executor.execute(context, new QueueConfigure(streamKey, sConsumer));
    assertTrue(executor.execute(context, new Read(dataKey, kvcol)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(queueKey, qConsumer, config)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(streamKey, sConsumer, config)).isEmpty());

    // insert data to all three again
    executor.commit(context, new Write(dataKey, kvcol, dataKey));
    executor.commit(context, new QueueEnqueue(queueKey, new QueueEntry(queueKey)));
    executor.commit(context, new QueueEnqueue(streamKey, new QueueEntry(streamKey)));

    // read data from all three types
    assertArrayEquals(dataKey, executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol));
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, qConsumer, config)).getEntry().getData()));
    assertTrue(Bytes.equals(streamKey, executor.execute(context,
        new QueueDequeue(streamKey, sConsumer, config)).getEntry().getData()));

    // wipe just the streams
    executor.execute(context, new ClearFabric(ClearFabric.ToClear.STREAMS));

    // streams gone, queues and data remain
    this.executor.execute(context, new QueueConfigure(streamKey, sConsumer));
    assertArrayEquals(dataKey,
        executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol));
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, qConsumer, config)).getEntry().getData()));
    assertTrue(executor.execute(context,
        new QueueDequeue(streamKey, sConsumer, config)).isEmpty());

    // wipe data and queues
    executor.execute(context, new ClearFabric(Arrays.asList(
        ClearFabric.ToClear.DATA, ClearFabric.ToClear.QUEUES)));

    // everything is gone
    this.executor.execute(context, new QueueConfigure(queueKey, qConsumer));
    assertTrue(executor.execute(context, new Read(dataKey, kvcol)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(queueKey, qConsumer, config)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(streamKey, sConsumer, config)).isEmpty());

    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void testOverlappingConcurrentWrites() throws Exception {

    byte [] key = Bytes.toBytes("keytestOverlappingConcurrentWrites");
    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");

    // start tx one
    Transaction pointerOne = executor.startTransaction(true);
    // System.out.println("Started transaction one : " + pointerOne);

    // write value one
    WriteTransactionResult txResult1 =
      executor.write(context, new Write(key, kvcol, valueOne), pointerOne);
    assertTrue(txResult1.success);
    executor.addToTransaction(pointerOne, txResult1.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // start tx two
    Transaction pointerTwo = executor.startTransaction(true);
    // System.out.println("Started transaction two : " + pointerTwo);
    assertTrue(pointerTwo.getWriteVersion() > pointerOne.getWriteVersion());

    // write value two
    WriteTransactionResult txResult2 =
      executor.write(context, new Write(key, kvcol, valueTwo), pointerTwo);
    executor.addToTransaction(pointerTwo, txResult2.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // commit tx two, should succeed
    assertTrue(executor.commitTransaction(pointerTwo).isSuccess());

    // even though tx one not committed, we can see two already
    OperationResult<Map<byte[], byte[]>> readValue = executor.execute(context, new Read(key, kvcol));
    assertNotNull(readValue);
    assertFalse(readValue.isEmpty());
    assertArrayEquals(valueTwo, readValue.getValue().get(kvcol));

    // commit tx one, should fail
    assertFalse(executor.commitTransaction(pointerOne).isSuccess());

    // should still see two
    readValue = executor.execute(context, new Read(key, kvcol));
    assertNotNull(readValue);
    assertFalse(readValue.isEmpty());
    assertArrayEquals(valueTwo, readValue.getValue().get(kvcol));
  }

  @Test
  public void testClosedTransactionsThrowExceptions() throws Exception {

    byte [] key = Bytes.toBytes("testClosedTransactionsThrowExceptions");

    // start txwOne
    Transaction pointerOne = executor.startTransaction(true);
    //System.out.println("Started transaction txwOne : " + pointerOne);

    // write and commit
    WriteTransactionResult txResult =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(1)), pointerOne);
    assertTrue(txResult.success);
    executor.addToTransaction(pointerOne, txResult.undos);
    assertTrue(executor.commitTransaction(pointerOne).isSuccess());

    // trying to commit this tx again should throw exception
    try {
      executor.commitTransaction(pointerOne);
      fail("Committing with committed transaction should throw exception");
    } catch (TransactionException te) {
      // correct
    }

    // read should see value 1
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));
  }

  @Test
  public void testOverlappingConcurrentReadersAndWriters() throws Exception {

    byte [] key = Bytes.toBytes("testOverlappingConcurrentReadersAndWriters");

    // start txwOne
    Transaction pointerWOne = executor.startTransaction(true);
    // System.out.println("Started transaction txwOne : " + pointerWOne);

    // write 1
    WriteTransactionResult txResultW1 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(1)), pointerWOne);
    assertTrue(txResultW1.success);
    executor.addToTransaction(pointerWOne, txResultW1.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // commit write 1
    assertTrue(executor.commitTransaction(pointerWOne).isSuccess());

    // read sees 1
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // open long-running read
    Transaction pointerReadOne = executor.startTransaction(true);

    // write 2 and commit immediately
    Transaction pointerWTwo = executor.startTransaction(true);
    // System.out.println("Started transaction txwTwo : " + pointerWTwo);
    WriteTransactionResult txResultW2 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(2)), pointerWTwo);
    assertTrue(txResultW2.success);
    executor.addToTransaction(pointerWTwo, txResultW2.undos);
    assertTrue(executor.commitTransaction(pointerWTwo).isSuccess());

    // read sees 2
    OperationResult<Map<byte[], byte[]>> value = executor.execute(context, new Read(key, kvcol));
    assertNotNull(value);
    assertFalse(value.isEmpty());
    assertArrayEquals(Bytes.toBytes(2), value.getValue().get(kvcol));

    // open long-running read
    Transaction pointerReadTwo = executor.startTransaction(true);

    // write 3 with one transaction but don't commit
    Transaction pointerWThree = executor.startTransaction(true);
    // System.out.println("Started transaction txwThree : " + pointerWThree);
    WriteTransactionResult txResultW3 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(3)), pointerWThree);
    assertTrue(txResultW3.success);
    executor.addToTransaction(pointerWThree, txResultW3.undos);

    // write 4 with another transaction and also don't commit
    Transaction pointerWFour = executor.startTransaction(true);
    // System.out.println("Started transaction txwFour : " + pointerWFour);
    WriteTransactionResult txResultW4 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(4)), pointerWFour);
    assertTrue(txResultW4.success);
    executor.addToTransaction(pointerWFour, txResultW4.undos);

    // read sees 2 still
    assertArrayEquals(Bytes.toBytes(2),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // commit 4, should be successful
    assertTrue(executor.commitTransaction(pointerWFour).isSuccess());

    // read sees 4
    assertArrayEquals(Bytes.toBytes(4),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // commit 3, should fail
    assertFalse(executor.commitTransaction(pointerWThree).isSuccess());

    // read still sees 4
    assertArrayEquals(Bytes.toBytes(4),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // now read with long-running read 1, should see value = 1
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, pointerReadOne, new Read(key, kvcol))
            .getValue().get(kvcol));

    // now do the same thing but in reverse order of conflict

    // write 5 with one transaction but don't commit
    Transaction pointerWFive = executor.startTransaction(true);
    // System.out.println("Started transaction txwFive : " + pointerWFive);
    WriteTransactionResult txResultW5 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(5)), pointerWFive);
    assertTrue(txResultW5.success);
    executor.addToTransaction(pointerWFive, txResultW5.undos);

    // write 6 with another transaction and also don't commit
    Transaction pointerWSix = executor.startTransaction(true);
    // System.out.println("Started transaction txwSix : " + pointerWSix);
    WriteTransactionResult txResultW6 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(6)), pointerWSix);
    assertTrue(txResultW6.success);
    executor.addToTransaction(pointerWSix, txResultW6.undos);

    // read sees 4 still
    assertArrayEquals(Bytes.toBytes(4),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // long running reads should still see their respective values
    assertArrayEquals(Bytes.toBytes(1),
                      executor.execute(context, pointerReadOne, new Read(key, kvcol)).getValue().get(kvcol));
    assertArrayEquals(Bytes.toBytes(2),
                      executor.execute(context, pointerReadTwo, new Read(key, kvcol)).getValue().get(kvcol));

    // commit 5, should be successful
    assertTrue(executor.commitTransaction(pointerWFive).isSuccess());

    // read sees 5
    assertArrayEquals(Bytes.toBytes(5),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // long running reads should still see their respective values
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, pointerReadOne, new Read(key, kvcol)).getValue().get(kvcol));
    assertArrayEquals(Bytes.toBytes(2),
        executor.execute(context, pointerReadTwo, new Read(key, kvcol)).getValue().get(kvcol));

    // commit 6, should fail
    assertFalse(executor.commitTransaction(pointerWSix).isSuccess());

    // read still sees 5
    assertArrayEquals(Bytes.toBytes(5),
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // long running reads should still see their respective values
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, pointerReadOne, new Read(key, kvcol)).getValue().get(kvcol));
    assertArrayEquals(Bytes.toBytes(2),
        executor.execute(context, pointerReadTwo, new Read(key, kvcol)).getValue().get(kvcol));
  }


  @Test
  public void testAbortedOperationsWithQueueAck() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;

    byte [] key = Bytes.toBytes("testAbortedAck");
    byte [] queueName = Bytes.toBytes("testAbortedAckQueue");

    // EnqueuePayload something
    executor.commit(context, batch(new QueueEnqueue(queueName, new QueueEntry(queueName))));

    // DequeuePayload it
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    this.executor.execute(context, new QueueConfigure(queueName, consumer));
    DequeueResult dequeueResult = executor.execute(context, new QueueDequeue(queueName, consumer, config));
    assertTrue(dequeueResult.isSuccess());

    // Start our ack operation
    Transaction ackPointer = executor.startTransaction(true);

    // Start a fake operation that will just conflict with our key
    Transaction fakePointer = executor.startTransaction(true);
    Undo fakeUndo = new UndoWrite(null, key, new byte[][] { new byte[] {'a' } });
    executor.addToTransaction(fakePointer, Collections.singletonList(fakeUndo));

    // Commit fake operation successfully
    assertTrue(executor.commitTransaction(fakePointer).isSuccess());

    // Increment a counter and add our ack
    List<WriteOperation> writes = new ArrayList<WriteOperation>(2);
    writes.add(new Increment(key, kvcol, 3));
    writes.add(new QueueAck(queueName,
        dequeueResult.getEntryPointer(), consumer));

    // execute & commit should return failure
    try {
      executor.commit(context, ackPointer, writes);
      fail("expecting OperationException");
    } catch (OperationException e) {
      // expected
    }

    // Should still be able to dequeue
    dequeueResult = executor.execute(context,
        new QueueDequeue(queueName, consumer, config));
    // THIS FAILS IF ACK NOT REALLY ROLLED BACK!
    assertTrue(dequeueResult.isSuccess());


    // Start new ack operation
    ackPointer = executor.startTransaction(true);

    // Same increment and ack
    writes = new ArrayList<WriteOperation>(2);
    writes.add(new Increment(key, kvcol, 5));
    writes.add(new QueueAck(queueName,
        dequeueResult.getEntryPointer(), consumer));

    // Execute should succeed
    executor.commit(context, ackPointer, writes);


    // DequeuePayload should now return empty
    dequeueResult = executor.execute(context,
        new QueueDequeue(queueName, consumer, config));
    assertTrue(dequeueResult.isEmpty());

    // Incremented value should be 5
    // Mario, look at this one!
    assertEquals(5L, Bytes.toLong(
        executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol)));

    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void testDeletesCanBeTransacted() throws Exception {

    byte [] key = Bytes.toBytes("testDeletesCanBeTransacted");
    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    Transaction dirtyRead = new Transaction(1L, MemoryReadPointer.DIRTY_READ, true);

    List<WriteOperation> ops = new ArrayList<WriteOperation>();
    Delete delete = new Delete(key, kvcol);
    ops.add(delete);

    // Executing in a batch should succeed
    executor.commit(context, ops);

    // Executing singly should also succeed
    executor.commit(context, delete);

    // start tx one
    Transaction pointerOne = executor.startTransaction(true);
    // System.out.println("Started transaction one : " + pointerOne);

    // write value one
    WriteTransactionResult txResultOne =
      executor.write(context, new Write(key, kvcol, valueOne), pointerOne);
    assertTrue(txResultOne.success);
    executor.addToTransaction(pointerOne, txResultOne.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // commit
    assertTrue(executor.commitTransaction(pointerOne).isSuccess());

    // dirty read should see it
    assertArrayEquals(valueOne, executor.execute(context, dirtyRead, new Read(key, kvcol)).getValue().get(kvcol));

    // start tx two
    Transaction pointerTwo = executor.startTransaction(true);
    // System.out.println("Started transaction two : " + pointerTwo);

    // delete value one
    WriteTransactionResult txResultTwo =
      executor.write(context, new Delete(key, kvcol), pointerTwo);
    assertTrue(txResultTwo.success);
    executor.addToTransaction(pointerTwo, txResultTwo.undos);

    // clean read should see it still
    assertArrayEquals(valueOne, executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // dirty read should NOT see it
    assertTrue(executor.execute(context, dirtyRead, new Read(key, kvcol)).isEmpty());

    // commit it
    assertTrue(executor.commitTransaction(pointerTwo).isSuccess());

    // clean read will not see it now
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // write value two
    executor.commit(context, new Write(key, kvcol, valueTwo));

    // clean read sees it
    assertArrayEquals(valueTwo, executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // dirty read sees it
    assertArrayEquals(valueTwo, executor.execute(context, dirtyRead, new Read(key, kvcol)).getValue().get(kvcol));

    // start tx three
    Transaction pointerThree = executor.startTransaction(true);
    // System.out.println("Started transaction three : " + pointerThree);

    // start and commit a fake transaction which will overlap
    Transaction pointerFour = executor.startTransaction(true);
    Undo fakeUndo = new UndoWrite(null, key, new byte[][] { new byte[] {'a' } });
    executor.addToTransaction(pointerFour, Collections.singletonList(fakeUndo));
    assertTrue(executor.commitTransaction(pointerFour).isSuccess());

    // commit the real transaction with a delete, should be aborted
    try {
      executor.commit(context, pointerThree, batch(new Delete(key, kvcol)));
      fail("expecting OperationException");
    } catch (OperationException e) {
      // verify aborted
    }

    // verify clean and dirty reads still see the value (it was undeleted)
    assertArrayEquals(valueTwo, executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));
    assertArrayEquals(valueTwo, executor.execute(context, dirtyRead, new Read(key, kvcol)).getValue().get(kvcol));
  }

  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

  // test table operations on default and named tables: write, read,
  // readkey, readcolumnrange, increment, delete, compareandswap
  @Test
  public void testNamedTableOperations() throws OperationException {
    testNamedTableOperations(null);
    testNamedTableOperations("tableA");
    testNamedTableOperations("tableB");
  }

  private void testNamedTableOperations(String table)
      throws OperationException {

    // clear the fabric
    executor.execute(context, new ClearFabric(ClearFabric.ToClear.ALL));

    // open a table (only if name is not null)
    if (table != null) {
      executor.execute(context, new OpenTable(table));
    }

    // write a few columns
    final byte[] col1 = new byte[] { 1 };
    final byte[] col2 = new byte[] { 2 };
    final byte[] col3 = new byte[] { 3 };
    final byte[] col4 = Operation.KV_COL;
    final byte[] val1 = new byte[] { 'x' };
    final byte[] val2 = new byte[] { 'a' };
    final byte[] val3 = Bytes.toBytes(7L);
    final byte[] val4 = new byte[] { 'x', 'y', 'z'};
    final byte[] rowkey = new byte[] { 'r', 'o', 'w', '4', '2' };
    executor.commit(context, new Write(table, rowkey, new byte[][]{col1, col2, col3, col4}, new byte[][]{val1, val2,
      val3, val4}));

    {
    // read back with single column
    OperationResult<Map<byte[], byte[]>> result1 = executor.execute(context,
        new Read(table, rowkey, col1));
    Assert.assertFalse(result1.isEmpty());
    Assert.assertNotNull(result1.getValue());
    Assert.assertEquals(1, result1.getValue().size());
    Assert.assertArrayEquals(val1, result1.getValue().get(col1));
    }
    {
    // read back with multi column
    OperationResult<Map<byte[], byte[]>> result2 = executor.execute(context,
        new Read(table, rowkey, new byte[][] { col2, col3 }));
    Assert.assertFalse(result2.isEmpty());
    Assert.assertNotNull(result2.getValue());
    Assert.assertEquals(2, result2.getValue().size());
    Assert.assertArrayEquals(val2, result2.getValue().get(col2));
    Assert.assertArrayEquals(val3, result2.getValue().get(col3));
    }
    {
    // read back with read key
    OperationResult<Map<byte[], byte[]>> result3 =
        executor.execute(context, new Read(table, rowkey, kvcol));
    Assert.assertFalse(result3.isEmpty());
    Assert.assertArrayEquals(val4, result3.getValue().get(kvcol));
    }
    {
    // read back with column range
    OperationResult<Map<byte[], byte[]>> result4 = executor.execute(context,
        new ReadColumnRange(table, rowkey, col2, null));
    Assert.assertFalse(result4.isEmpty());
    Assert.assertNotNull(result4.getValue());
    Assert.assertEquals(3, result4.getValue().size());
    Assert.assertArrayEquals(val2, result4.getValue().get(col2));
    Assert.assertArrayEquals(val3, result4.getValue().get(col3));
    Assert.assertArrayEquals(val4, result4.getValue().get(col4));
    }
    // increment one column
    executor.increment(context, new Increment(table, rowkey, col3, 1L));
    {
    // read back with single column
    OperationResult<Map<byte[], byte[]>> result5 = executor.execute(context,
        new Read(table, rowkey, col3));
    Assert.assertFalse(result5.isEmpty());
    Assert.assertNotNull(result5.getValue());
    Assert.assertEquals(1, result5.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(8L), result5.getValue().get(col3));
    }
    // delete one column
    executor.commit(context, new Delete(table, rowkey, col2));
    {
    // verify it's gone
    OperationResult<Map<byte[], byte[]>> result6 = executor.execute(context,
        new Read(table, rowkey, col2));
    Assert.assertTrue(result6.isEmpty());
    Assert.assertEquals(StatusCode.COLUMN_NOT_FOUND, result6.getStatus());
    }
    // compare-and-swap with success
    executor.commit(context, new CompareAndSwap(table, rowkey, col3, Bytes.toBytes(8L), Bytes.toBytes(3L)));
    {
    // verify value has changed
    OperationResult<Map<byte[], byte[]>> result7 = executor.execute(context,
        new Read(table, rowkey, col3));
    Assert.assertFalse(result7.isEmpty());
    Assert.assertNotNull(result7.getValue());
    Assert.assertEquals(1, result7.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(3L), result7.getValue().get(col3));
    }
    // compare-and-swap and fail
    try {
      executor.commit(context, new CompareAndSwap(table, rowkey, col3, Bytes.toBytes(8L), Bytes.toBytes(17L)));
      fail("Write conflict exception expected.");
    } catch (OperationException e) {
      // expected write conflict
      if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
        throw e;
      }
    }
    // verify value is still the same
    {
    OperationResult<Map<byte[], byte[]>> result8 = executor.execute(context,
        new Read(table, rowkey, col3));
    Assert.assertFalse(result8.isEmpty());
    Assert.assertNotNull(result8.getValue());
    Assert.assertEquals(1, result8.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(3L), result8.getValue().get(col3));
    }
  }

  // test that transactions can be rolled back across tables
  @Test
  public void testTransactionsAcrossTables() throws OperationException {

    // some handy constants
    final String tableA = "tableA", tableB = "tableB";
    final byte[] rowX = "rowX".getBytes(), rowY = "rowY".getBytes(),
        rowZ = "rowZ".getBytes();
    final byte[] colX = "colX".getBytes(), colY = "colY".getBytes(),
        colZ = "colZ".getBytes();
    final byte[] valX = "valX".getBytes(), valX1 = "valX1".getBytes(),
        valZ = "valZ".getBytes(), val42 = Bytes.toBytes(42L);

    // open two tables
    executor.execute(context, new OpenTable(tableA));
    executor.execute(context, new OpenTable(tableB));

    // write to default and both tables
    executor.commit(context, new Write(rowX, colX, valX));
    executor.commit(context, new Write(tableA, rowY, colY, val42));
    executor.commit(context, new Write(tableB, rowZ, colZ, valZ));

    // verify the writes went through
    Assert.assertArrayEquals(valX, executor.execute(
        context, new Read(rowX, colX)).getValue().get(colX));
    Assert.assertArrayEquals(val42, executor.execute(
        context, new Read(tableA, rowY, colY)).getValue().get(colY));
    Assert.assertArrayEquals(valZ, executor.execute(
        context, new Read(tableB, rowZ, colZ)).getValue().get(colZ));

    // batch: write to default, increment one, delete from other, c-a-s fails
    List<WriteOperation> writes = Lists.newArrayList();
    writes.add(new Write(rowX, colX, valX1));
    writes.add(new Increment(tableA, rowY, colY, 1L));
    writes.add(new Delete(tableB, rowZ, colZ));
    writes.add(new CompareAndSwap(tableA, rowX, colX, valX, null));
    try {
      executor.commit(context, writes);
      fail("Expected compare-and-swap to fail batch.");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
        throw e; // only write confict is expected
      }
    }

    // verify all was rolled back
    Assert.assertArrayEquals(valX, executor.execute(
        context, new Read(rowX, colX)).getValue().get(colX));
    Assert.assertArrayEquals(val42, executor.execute(
        context, new Read(tableA, rowY, colY)).getValue().get(colY));
    Assert.assertArrayEquals(valZ, executor.execute(
        context, new Read(tableB, rowZ, colZ)).getValue().get(colZ));
  }

  // TODO: test concurrent openTable()
  // how can we test that without opening lots of tables in different threads?

  // TODO: test openTable() works for existing table after shutdown
  // how can we test that if the opex is a singleton? Otherwise we could
  // open a new executor and verify that it finds the existing tables

  // test that repeatedly opening a table has no effect
  @Test
  public void testSubsequentOpenTable() throws OperationException {
    // some handy constants
    final String table = "the-table";
    final byte[] rowX = "rowX".getBytes(), colX = "colX".getBytes(),
        valX = "valX".getBytes(), valX1 = "valX1".getBytes();

    // open a table, and write to the table
    executor.execute(context, new OpenTable(table));
    executor.commit(context, new Write(table, rowX, colX, valX));

    // verify the write can be read
    OperationResult<Map<byte[], byte[]>> result = executor.execute(context, new Read(table, rowX, colX));
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(valX, result.getValue().get(colX));

    // open the table again
    executor.execute(context, new OpenTable(table));

    // verify it can still be read (if open resulted in a new table,
    // then it would be empty, and the read would fail)
    Assert.assertArrayEquals(valX, executor.execute(
        context, new Read(table, rowX, colX)).getValue().get(colX));

    // write to the table
    executor.commit(context, new Write(table, rowX, colX, valX1));
    Assert.assertArrayEquals(valX1, executor.execute(
        context, new Read(table, rowX, colX)).getValue().get(colX));

    // open a again and verify the write can still be read
    executor.execute(context, new OpenTable(table));
    Assert.assertArrayEquals(valX1, executor.execute(
        context, new Read(table, rowX, colX)).getValue().get(colX));
  }

  // test that different op contexts get different tables of same name
  // test that clear fabric deletes all tables for the context and none else
  @Test
  public void testTablesWithDifferentContexts() throws OperationException {
    // some handy constants
    final String table = "the-table";
    final byte[] rowX = "rowX".getBytes(),
        colX = "colX".getBytes(), colY = "colY".getBytes(),
        valX = "valX".getBytes(), valY = "valY".getBytes();

    // create two contexts
    OperationContext context1 = new OperationContext("account1");
    OperationContext context2 = new OperationContext("account2");

    // open a table with the same name for each context
    executor.execute(context1, new OpenTable(table));
    executor.execute(context2, new OpenTable(table));

    // write different values to each table
    executor.commit(context1, new Write(table, rowX, colX, valX));
    executor.commit(context2, new Write(table, rowX, colY, valY));

    // verify each context see its own writes
    Assert.assertArrayEquals(valX, executor.execute(
        context1, new Read(table, rowX, colX)).getValue().get(colX));
    Assert.assertArrayEquals(valY, executor.execute(
        context2, new Read(table, rowX, colY)).getValue().get(colY));

    // verify that each context can not see the others writes
    OperationResult<Map<byte[], byte[]>> result1 =
        executor.execute(context1, new Read(table, rowX, colY));
    OperationResult<Map<byte[], byte[]>> result2 =
        executor.execute(context2, new Read(table, rowX, colX));
    Assert.assertTrue(result1.isEmpty()
        || result1.getValue().get(colY) == null);
    Assert.assertTrue(result2.isEmpty()
                        || result2.getValue().get(colX) == null);

    // clear the tables for one context
    executor.execute(context1, new ClearFabric(ClearFabric.ToClear.TABLES));

    // verify the table is gone for that context
    OperationResult<Map<byte[], byte[]>> result3 =
        executor.execute(context1, new Read(table, rowX, colX));
    Assert.assertTrue(result3.isEmpty()
        || result1.getValue().get(colX) == null);

    // verify the table for the other context is still there
    Assert.assertArrayEquals(valY, executor.execute(
        context2, new Read(table, rowX, colY)).getValue().get(colY));
  }

  // test that transactions work across calls
  @Test
  public void testMultiCallTransaction() throws OperationException {
    final String table = "tMCT";
    final byte[] a = "a".getBytes();
    final byte[] b = "b".getBytes();
    final byte[] c = "c".getBytes();
    final byte[] x = "x".getBytes();
    final byte[] col = "col".getBytes();
    final byte[] me = "me".getBytes();

    // start two transactions, one explicit, one by submitting a batch
    Transaction tx1 = executor.startTransaction(context, true);
    Transaction tx2 = executor.execute(context, null, batch(new Write(table, a, col, me)));
    // increment a counter with both transactions
    executor.execute(context, tx1, batch(new Increment(table, b, x, 1L)));
    executor.execute(context, tx2, batch(new Increment(table, c, x, 5L)));
    // fail the first transaction with a c-a-s
    try {
      executor.execute(context, tx1, batch(new CompareAndSwap(table, x, col, me, me)));
      fail("Compare-andswap should fail");
    } catch (OperationException e) {
      // expected
    }
    // commit the second transaction
    executor.commit(context, tx2);
    // verify the first transaction was rolled back
    Assert.assertTrue(executor.execute(context, new Read(table, b, x)).isEmpty());
    // verify the second transaction was commited
    Assert.assertArrayEquals(me, executor.execute(context, new Read(table, a, col)).getValue().get(col));
    Assert.assertEquals(5L, Bytes.toLong(executor.execute(context, new Read(table, c, x)).getValue().get(x)));
  }

  // test that conflict detection works across calls
  @Test
  public void testMultiCallConflictDetection() throws OperationException {
    final String table = "tMCCD";
    final byte[] a = {'a'};
    final byte[] b = {'b'};
    final byte[] x = {'x'};
    final byte[] y = {'y'};
    final byte[] one = {'1'};
    final byte[] two = {'2'};
    final byte[] three = {'3'};

    // write a value to row a with the first transaction
    Transaction tx1 = executor.startTransaction(context, true);
    executor.execute(context, tx1, batch(new Write(table, a, x, one)));
    // write row b with the second transaction
    Transaction tx2 = executor.startTransaction(context, true);
    executor.execute(context, tx2, batch(new Write(table, b, x, two)));
    // write to a different row x with the third transaction
    Transaction tx3 = executor.startTransaction(context, true);
    executor.execute(context, tx3, batch(new Write(table, x, y, one)));
    // write a value to row b with the first transaction
    executor.execute(context, tx1, batch(new Write(table, b, y, three)));
    // write to row a with with the third transaction
    executor.execute(context, tx3, batch(new Write(table, a, y, two)));
    // commit first transaction
    executor.commit(context, tx1);
    // commit second transaction - should fail because it conflict with row b
    try {
      executor.commit(context, tx2);
      fail("commit of tx2 should have failed dur to conflict on row b");
    } catch (OperationException e) {
      // expected
    }
    // commit third transaction - should fail because it conflicts with row a
    try {
      executor.commit(context, tx3);
      fail("commit of tx3 should have failed dur to conflict on row a");
    } catch (OperationException e) {
      // expected
    }
  }

  // test that read isolation works across calls
  @Test
  public void testMultiCallReadIsolation() throws OperationException {
    final String table = "tMCRI";
    final byte[] row = { 'r', 'o' };
    final byte[] col = { 'c', '1' };

    // start a transaction
    Transaction tx1 = executor.startTransaction(context, true);
    // set a row to 10
    executor.execute(context, tx1, batch(new Write(table, row, col, Bytes.toBytes(10L))));
    // increment the row in another transaction
    Transaction tx2 = executor.startTransaction(context, true);
    // TODO increment is currently broken in HBase (see Jira ENG-2126).
    // TODO For now, when testing HBase, perform a write, not an increment
    // TODO this must be removed as soon as HBase is fixed
    if (this instanceof TestHBaseOmidTransactionalOperationExecutor) {
      executor.execute(context, tx2, batch(new Write(table, row, col, Bytes.toBytes(11L))));
      // increment the row by 10 - must see its own write but not the other one -> 20
      long current = Bytes.toLong(executor.execute(context, tx1, new Read(table, row, col)).getValue().get(col));
      Assert.assertEquals(10L, current);
      executor.execute(context, tx1, batch(new Write(table, row, col, Bytes.toBytes(current + 10L))));
    } else {
      executor.execute(context, tx2, batch(new Increment(table, row, col, 1L)));
      // increment the row by 10 - must see its own write but not the other one -> 20
      executor.execute(context, tx1, batch(new Increment(table, row, col, 10L)));
    }
    // commit the transaction
    executor.commit(context, tx1);
    // verify the value is 20
    Assert.assertEquals(20L, Bytes.toLong(executor.execute(context, new Read(table, row, col)).getValue().get(col)));
    // commit the other transaction - fails
    try {
      executor.commit(context, tx2);
      fail("tx2 should have failed becauseof conflict with row " + new String(row));
    } catch (OperationException e) {
      // expected
    }
  }

  // test that reads are performed within transaction
  @Test
  public void testReadsWithinTransaction() throws OperationException {
    final String table = "tRWT";
    final byte[] f = {'f'};
    final byte[] g = {'g'};
    final byte[] x = {'x'};
    final byte[] one = {'1'};

    // start transaction
    Transaction tx = executor.startTransaction(true);
    // write a value in that tx
    executor.execute(context, tx, batch(new Write(table, f, x, one)));
    // read the value outside the tx -> null
    Assert.assertTrue(executor.execute(context, new Read(table, f, x)).isEmpty());
    // read the value inside the tx -> visible
    Assert.assertArrayEquals(one, executor.execute(context, tx, new Read(table, f, x)).getValue().get(x));
    // write another value outside the tx
    executor.commit(context, batch(new Write(table, g, x, one)));
    // read the value outside the tx -> visible
    Assert.assertArrayEquals(one, executor.execute(context, new Read(table, g, x)).getValue().get(x));
    // read the value inside the tx -> null
    Assert.assertTrue(executor.execute(context, tx, new Read(table, g, x)).isEmpty());
    // commit transaction
    executor.commit(context, tx);
  }

  // test that read results can be sent via queue
  @Test
  public void testReadThenEnqueueAndDeqeue() throws OperationException, IOException {
    final String table = "tRTEAD";
    final byte[] qname = "qRTEAD".getBytes();
    final byte[] f = {'f'};
    final byte[] g = {'g'};
    final byte[] x = {'x'};
    final byte[] one = Bytes.toBytes(1L);
    final byte[] eleven = Bytes.toBytes(11L);

    // start a transaction
    Transaction tx1 = executor.startTransaction(context, true);
    // write a value 1
    executor.execute(context, tx1, batch(new Write(table, f, x, one)));
    // increment the value by 10
    // TODO increment is currently broken in HBase (see Jira ENG-2126).
    // TODO For now, when testing HBase, perform a write, not an increment
    // TODO this must be removed as soon as HBase is fixed
    if (this instanceof TestHBaseOmidTransactionalOperationExecutor) {
      executor.execute(context, tx1, batch(new Write(table, f, x, eleven)));
    } else {
      executor.execute(context, tx1, batch(new Increment(table, f, x, 10L)));
    }
    // read the value back, should be 11
    byte[] value = executor.execute(context, tx1, new Read(table, f, x)).getValue().get(x);
    Assert.assertArrayEquals(eleven, value);
    // enqueue the value and commit
    executor.commit(context, tx1, batch(new QueueEnqueue(qname, new QueueEntry(value))));
    // dequeue
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(PartitionerType.FIFO, true));
    this.executor.execute(context,
                          new QueueConfigure(qname, consumer));
    DequeueResult deqres = executor.execute(
      context, new QueueDequeue(qname, consumer,
                                new QueueConfig(PartitionerType.FIFO, true)));
    // verify the value
    Assert.assertFalse(deqres.isEmpty());
    Assert.assertArrayEquals(value, deqres.getEntry().getData());
    // in a new transaction, write the dequeued value and ack the queue entry
    executor.commit(context, batch(new Write(table, g, x, value),
                                   new QueueAck(qname, deqres.getEntryPointer(),
                                                new QueueConsumer(0, 0, 1,
                                                                  new QueueConfig(PartitionerType.FIFO, true)))));
    // attempt to dequeue again, should be empty
    deqres = executor.execute(
      context, new QueueDequeue(qname, new QueueConsumer(0, 0, 1, new QueueConfig(PartitionerType.FIFO, true)),
                                new QueueConfig(PartitionerType.FIFO, true)));
    // verify the value
    Assert.assertTrue(deqres.isEmpty());
    // verify the write
    Assert.assertArrayEquals(value, executor.execute(context, new Read(table, g, x)).getValue().get(x));
  }

  // test increment with return value
  @Test
  public void testIncrementWithReturn() throws OperationException {
    final String table = "tIWR";
    final byte[] first = {'f', 'r'};
    final byte[] second = {'s', 'r'};
    final byte[] a = {'a'};
    final byte[] b = {'b'};
    final byte[] c = {'c'};
    final byte[][] ab = {a, b};
    final byte[][] abc = {a, b, c};

    // write two out of three columns
    executor.commit(context, new Write(table, first, ab, new byte[][]{Bytes.toBytes(1L), Bytes.toBytes(10L)}));

    // start a transaction
    Transaction tx1 = executor.startTransaction(context, true);
    // increment these columns within the transaction
    Map<byte[], Long> res = executor.
      increment(context, tx1, new Increment(table, first, abc, new long[]{1, 2, 55}));
    // verify return values
    Assert.assertFalse(res.isEmpty());
    Assert.assertEquals(new Long(2L), res.get(a));
    Assert.assertEquals(new Long(12L), res.get(b));
    Assert.assertEquals(new Long(55L), res.get(c));
    // write to a new row and commit transaction
    executor.commit(context, tx1, batch(new Write(table, second, abc, new byte[][] {
      Bytes.toBytes(res.get(a)), Bytes.toBytes(res.get(b)), Bytes.toBytes(res.get(c)) })));

    // increment new row in own tx
    res = executor.increment(context, new Increment(table, second, abc, new long[]{1, 3, 11}));
    // verify return values
    Assert.assertFalse(res.isEmpty());
    Assert.assertEquals(new Long(3L), res.get(a));
    Assert.assertEquals(new Long(15L), res.get(b));
    Assert.assertEquals(new Long(66L), res.get(c));

    // read back values as bytes in own tx
    OperationResult<Map<byte[], byte[]>> res1 = executor.execute(context, new Read(table, second, abc));
    // verify values again as bytes
    Assert.assertFalse(res.isEmpty());
    Assert.assertEquals(3L, Bytes.toLong(res1.getValue().get(a)));
    Assert.assertEquals(15L, Bytes.toLong(res1.getValue().get(b)));
    Assert.assertEquals(66L, Bytes.toLong(res1.getValue().get(c)));
  }

  // test that operation on different tables but with same key do not cause write conflict
  @Test
  public void testNoConflictsIfDifferentTables() throws OperationException {
    final String t2 = "tNCIDT2";
    final String t3 = "tNCIDT3";
    final byte[] a = {'a'};
    final byte[] b = {'b'};

    // start three transactions
    Transaction tx1 = executor.startTransaction(context, true);
    Transaction tx2 = executor.startTransaction(context, true);
    Transaction tx3 = executor.startTransaction(context, true);

    // all transactions write the same row to different tables, one uses default table
    executor.execute(context, tx1, batch(new Write(a, b, b)));
    executor.execute(context, tx2, batch(new Write(t2, a, b, b)));
    executor.execute(context, tx3, batch(new Write(t3, a, b, b)));

    // commit all transactions, none should fail
    executor.commit(context, tx1); // succeeds anyway
    executor.commit(context, tx2); // no conflict between named table and default table
    executor.commit(context, tx3); // no conflict between two named tables
  }

  // test that operations fail if an invalid transaction is passed in
  @Test
  public void testInvalidTransactions() throws OperationException {

    final String table = "tIT";
    final byte[] a = {'a'};
    final byte[] b = {'b'};

    // start a transactions
    Transaction tx1 = executor.startTransaction(context, true);
    Transaction tx2 = executor.startTransaction(context, true);
    // execute some write
    executor.execute(context, tx1, batch(new Write(table, a, b, b)));
    // commit the first transaction, abort the second
    executor.commit(context, tx1);
    executor.abort(context, tx2);

    for (Transaction tx : Lists.newArrayList(tx1, tx2)) {

      // attempt to commit the transaction
      try {
        executor.commit(context, tx);
        fail("commit should fail for " + (tx == tx1 ? "committed" : "aborted") + " transaction");
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.INVALID_TRANSACTION) {
          throw e;
        }
      }

      // attempt to abort transaction
      try {
        executor.abort(context, tx);
        fail("abort should fail for " + (tx == tx1 ? "committed" : "aborted") + " transaction");
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.INVALID_TRANSACTION) {
          throw e;
        }
      }

      // attempt to execute write and commit transaction
      try {
        executor.commit(context, tx, batch(new Write(table, b, b, b)));
        fail("commit should fail for " + (tx == tx1 ? "committed" : "aborted") + " transaction");
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.INVALID_TRANSACTION) {
          throw e;
        }
      }
      // verify the write did not go through
      OperationResult<Map<byte[], byte[]>> result = executor.execute(context, new Read(table, b, b));
      assertTrue(result.isEmpty() || result.getValue().get(b) == null);

      // attempt to execute write with transaction
      try {
        executor.execute(context, tx, batch(new Write(table, b, b, b)));
        fail("write should fail for " + (tx == tx1 ? "committed" : "aborted") + " transaction");
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.INVALID_TRANSACTION) {
          throw e;
        }
      }
      // verify the write did not go through
      result = executor.execute(context, new Read(table, b, b));
      assertTrue(result.isEmpty() || result.getValue().get(b) == null);

      // attempt to execute increment with transaction
      try {
        executor.execute(context, tx, batch(new Increment(table, b, b, 1L)));
        fail("increment should fail for " + (tx == tx1 ? "committed" : "aborted") + " transaction");
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.INVALID_TRANSACTION) {
          throw e;
        }
      }
      // verify the write did not go through
      result = executor.execute(context, new Read(table, b, b));
      assertTrue(result.isEmpty() || result.getValue().get(b) == null);
    }
  }

  @Test
  public void testIncrementIgnoresInProgressXactions() throws OperationException {
    final String table = "tIIIPX";
    final byte[] r1 = { 'r', '1' };
    final byte[] c1 = { 'c', '1' };
    final byte[] one = com.continuuity.api.common.Bytes.toBytes(1L);

    // execute a write in a new xaction
    Transaction tx = executor.execute(context, null, batch(new Write(table, r1, c1, one)));
    // read from outside xaction -> not visible
    OperationResult<Map<byte[], byte[]>> result = executor.execute(context, new Read(table, r1, c1));
    assertTrue(result.isEmpty() || result.getValue().get(c1) == null);
    // increment outside xaction -> increments pre-xaction value
    Map<byte[], Long> iresult = executor.increment(context, new Increment(table, r1, c1, 4L));
    assertEquals(new Long(4L), iresult.get(c1));
    // commit should fail with conflict
    try {
      executor.commit(context, tx);
      fail("commit should fail due to write conflict");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
        throw e;
      }
    }
  }

  @Test
  public void testConflictNotDetectedWithNTCXactions() throws OperationException {
    final String table = "tIIIIPX";
    final byte[] r1 = { 'r', '1' };
    final byte[] c1 = { 'c', '1' };
    final byte[] one = com.continuuity.api.common.Bytes.toBytes(1L);

    // 1) check that NTC tx doesn't fail with conflict
    // 2) check that other tx doesn't fail because it conflicts with NTC xaction

    // execute a write in a new NTC xaction
    Transaction ntc = executor.startTransaction(false);
    executor.execute(context, ntc, batch(new Write(table, r1, c1, one)));
    // read from outside xaction -> not visible
    OperationResult<Map<byte[], byte[]>> result = executor.execute(context, new Read(table, r1, c1));
    assertTrue(result.isEmpty() || result.getValue().get(c1) == null);
    // increment outside xaction -> increments pre-xaction value
    Map<byte[], Long> iresult = executor.increment(context, new Increment(table, r1, c1, 4L));
    assertEquals(new Long(4L), iresult.get(c1));
    // execute a write in a new xaction
    Transaction tx = executor.execute(context, null, batch(new Write(table, r1, c1, one)));
    // commit should NOT fail with conflict
    executor.commit(context, ntc);
    // commit should NOT fail with conflict
    executor.commit(context, tx);
  }

  @Test
  public void testNTCXactionIsNotRolledBackByOpex() throws OperationException {
    final String table = "tIIIIIPX";
    final byte[] r1 = { 'r', '1' };
    final byte[] c1 = { 'c', '1' };
    final byte[] one = com.continuuity.api.common.Bytes.toBytes(1L);

    // execute a write in a new NTC xaction
    Transaction ntc = executor.startTransaction(false);
    // this should fail: no data is there to swap
    boolean failed = false;
    try {
      executor.execute(context, ntc, batch(new CompareAndSwap(table, r1, c1, one, one)));
    } catch (OperationException e) {
      failed = true;
    }
    Assert.assertTrue(failed);

    // but tx should not be aborted, we can work with it after that
    Map<byte[], Long> incResult = executor.increment(context, ntc, new Increment(table, r1, c1, 55L));
    assertEquals(55L, (long) incResult.get(c1));

    // commit should NOT fail
    executor.commit(context, ntc);

    // verifying that writes where committed
    incResult = executor.increment(context, new Increment(table, r1, c1, 1L));
    assertEquals(56L, (long) incResult.get(c1));
  }

  private static final byte[] c = { 'c' }, v = { 'v' };

  @Test
  public void testBatchReads() throws OperationException, InterruptedException {
    final String table = "tBRs";

    // write 1000 random values to the table and remember them in a set
    SortedSet<Long> keysWritten = Sets.newTreeSet();
    List<WriteOperation> ops = Lists.newArrayListWithCapacity(1000);
    Random rand = new Random(451); // this will give us the same "random" sequence reproducibly. but random enough.
    for (int i = 0; i < 1000; i++) {
      long keyLong = rand.nextLong();
      byte[] key = Bytes.toBytes(keyLong);
      ops.add(new Write(table, key,  new byte[][] { c, key }, new byte[][] { key, v }));
      keysWritten.add(keyLong);
    }
    executor.commit(context, ops);

    // get the splits for the table
    OperationResult<List<KeyRange>> result = executor.execute(context, new GetSplits(table));
    Assert.assertFalse(result.isEmpty());
    List<KeyRange> splits = result.getValue();
    // read each split and verify the keys
    SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten);
    verifySplits(table, splits, keysToVerify);

    // get specific number of splits for a subrange
    long start = 0x10000000L, stop = 0x40000000L;
    result = executor.execute(context, new GetSplits(table, 5, Bytes.toBytes(start), Bytes.toBytes(stop)));
    Assert.assertFalse(result.isEmpty());
    splits = result.getValue();
    Assert.assertTrue(splits.size() <= 5);
    keysToVerify = Sets.newTreeSet(keysWritten.subSet(start, stop));
    // read each split and verify the keys
    verifySplits(table, splits, keysToVerify);
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(String t, List<KeyRange> splits, SortedSet<Long> keysToVerify)
    throws OperationException, InterruptedException {
    // read each split and verify the keys, remove all read keys from the set
    for (KeyRange split : splits) {
      Scanner scanner = executor.scan(context, null, new Scan(t, split.getStart(), split.getStop()));
      for (ImmutablePair<byte[], Map<byte[], byte[]>> next = scanner.next(); next != null; next = scanner.next()) {
        byte[] key = next.getFirst();
        Map<byte[], byte[]> row = next.getSecond();
        // verify each row has the two columns written
        Assert.assertArrayEquals(key, row.get(c));
        Assert.assertArrayEquals(v, row.get(key));
        Assert.assertTrue(keysToVerify.remove(com.continuuity.api.common.Bytes.toLong(key)));
      }
      scanner.close();
    }
    // verify all keys have been read
    Assert.assertTrue("Remaining keys: " + keysToVerify.size(), keysToVerify.isEmpty());
  }

}
