/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.StatusCode;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadColumnRange;
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
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.util.TupleMetaDataAnnotator.DequeuePayload;
import com.continuuity.data.util.TupleMetaDataAnnotator.EnqueuePayload;
import com.google.common.collect.Lists;
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
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestOmidTransactionalOperationExecutor {

  private OmidTransactionalOperationExecutor executor;

  /** to get the singleton operation executor, always returns the same */
  protected abstract OmidTransactionalOperationExecutor getOmidExecutor();

  /** to support testing, return a new executor each time */
  // this would be needed to simulate multi-node opex, for instance to test
  // that a named table survives a shutdown and a new executor will open it
  // instead of recreating it, also for testing that multiple executors
  // will not create the same table multiple times
  // however, our data fabric modules return singletons.
  //protected abstract OmidTransactionalOperationExecutor getNewExecutor();

  static OperationContext context = OperationContext.DEFAULT;

  @Before
  public void initialize() {
    executor = getOmidExecutor();
  }

  @Test
  public void testSimple() throws Exception {

    byte [] key = Bytes.toBytes("keytestSimple");
    byte [] value = Bytes.toBytes("value");

    // start a transaction
    Transaction pointer = executor.startTransaction();

    // write to a key
    WriteTransactionResult txResult = executor.write(context, new Write(key, kvcol, value), pointer);
    assertTrue(txResult.success);
    executor.addToTransaction(pointer, txResult.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // commit
    assertTrue(executor.commitTransaction(pointer).isSuccess());

    // read should see the write
    OperationResult<Map<byte [],byte[]>> readValue = executor.execute(context, new Read(key, kvcol));
    assertNotNull(readValue);
    assertFalse(readValue.isEmpty());
    assertArrayEquals(value, readValue.getValue().get(kvcol));
  }

  static final byte[] kvcol = Operation.KV_COL;

  @Test
  public void testClearFabric() throws Exception {
    OmidTransactionalOperationExecutor.DISABLE_QUEUE_PAYLOADS = true;
    byte [] dataKey = Bytes.toBytes("dataKey");
    byte [] queueKey = Bytes.toBytes("queue://queueKey");
    byte [] streamKey = Bytes.toBytes("stream://streamKey");

    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config = new QueueConfig(PartitionerType.RANDOM, true);

    // insert to all three types
    executor.execute(context, new Write(dataKey, kvcol, dataKey));
    executor.execute(context, new QueueEnqueue(queueKey, queueKey));
    executor.execute(context, new QueueEnqueue(streamKey, streamKey));

    // read data from all three types
    assertTrue(Bytes.equals(dataKey,
        executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol)));
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, consumer, config)).getValue()));
    assertTrue(Bytes.equals(streamKey, executor.execute(context,
        new QueueDequeue(streamKey, consumer, config)).getValue()));

    // clear data only
    executor.execute(context, new ClearFabric(ClearFabric.ToClear.DATA));

    // data is gone, queues still there
    assertTrue(executor.execute(context, new Read(dataKey, kvcol)).isEmpty());
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, consumer, config)).getValue()));
    assertTrue(Bytes.equals(streamKey, executor.execute(context,
        new QueueDequeue(streamKey, consumer, config)).getValue()));

    // clear queues and streams
    executor.execute(context, new ClearFabric(Arrays.asList(
        ClearFabric.ToClear.QUEUES, ClearFabric.ToClear.STREAMS)));

    // everything is gone
    assertTrue(executor.execute(context, new Read(dataKey, kvcol)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(queueKey, consumer, config)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(streamKey, consumer, config)).isEmpty());

    // insert data to all three again
    executor.execute(context, new Write(dataKey, kvcol, dataKey));
    executor.execute(context, new QueueEnqueue(queueKey, queueKey));
    executor.execute(context, new QueueEnqueue(streamKey, streamKey));

    // read data from all three types
    assertArrayEquals(dataKey,
        executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol));
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, consumer, config)).getValue()));
    assertTrue(Bytes.equals(streamKey, executor.execute(context,
        new QueueDequeue(streamKey, consumer, config)).getValue()));

    // wipe just the streams
    executor.execute(context, new ClearFabric(ClearFabric.ToClear.STREAMS));

    // streams gone, queues and data remain
    assertArrayEquals(dataKey,
        executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol));
    assertTrue(Bytes.equals(queueKey, executor.execute(context,
        new QueueDequeue(queueKey, consumer, config)).getValue()));
    assertTrue(executor.execute(context,
        new QueueDequeue(streamKey, consumer, config)).isEmpty());

    // wipe data and queues
    executor.execute(context, new ClearFabric(Arrays.asList(
        ClearFabric.ToClear.DATA, ClearFabric.ToClear.QUEUES)));

    // everything is gone
    assertTrue(executor.execute(context, new Read(dataKey, kvcol)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(queueKey, consumer, config)).isEmpty());
    assertTrue(executor.execute(context,
        new QueueDequeue(streamKey, consumer, config)).isEmpty());

    OmidTransactionalOperationExecutor.DISABLE_QUEUE_PAYLOADS = false;
  }

  @Test
  public void testOverlappingConcurrentWrites() throws Exception {

    byte [] key = Bytes.toBytes("keytestOverlappingConcurrentWrites");
    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");

    // start tx one
    Transaction pointerOne = executor.startTransaction();
    // System.out.println("Started transaction one : " + pointerOne);

    // write value one
    WriteTransactionResult txResult1 =
      executor.write(context, new Write(key, kvcol, valueOne), pointerOne);
    assertTrue(txResult1.success);
    executor.addToTransaction(pointerOne, txResult1.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // start tx two
    Transaction pointerTwo = executor.startTransaction();
    // System.out.println("Started transaction two : " + pointerTwo);
    assertTrue(pointerTwo.getTransactionId() > pointerOne.getTransactionId());

    // write value two
    WriteTransactionResult txResult2 =
      executor.write(context, new Write(key, kvcol, valueTwo), pointerTwo);
    executor.addToTransaction(pointerTwo, txResult2.undos);

    // read should see nothing
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // commit tx two, should succeed
    assertTrue(executor.commitTransaction(pointerTwo).isSuccess());

    // even though tx one not committed, we can see two already
    OperationResult<Map<byte[],byte[]>> readValue = executor.execute(context, new Read(key, kvcol));
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
    Transaction pointerOne = executor.startTransaction();
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
    Transaction pointerWOne = executor.startTransaction();
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
    Transaction pointerReadOne = executor.startTransaction();

    // write 2 and commit immediately
    Transaction pointerWTwo = executor.startTransaction();
    // System.out.println("Started transaction txwTwo : " + pointerWTwo);
    WriteTransactionResult txResultW2 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(2)), pointerWTwo);
    assertTrue(txResultW2.success);
    executor.addToTransaction(pointerWTwo, txResultW2.undos);
    assertTrue(executor.commitTransaction(pointerWTwo).isSuccess());

    // read sees 2
    OperationResult<Map<byte[],byte[]>> value = executor.execute(context, new Read(key, kvcol));
    assertNotNull(value);
    assertFalse(value.isEmpty());
    assertArrayEquals(Bytes.toBytes(2), value.getValue().get(kvcol));

    // open long-running read
    Transaction pointerReadTwo = executor.startTransaction();

    // write 3 with one transaction but don't commit
    Transaction pointerWThree = executor.startTransaction();
    // System.out.println("Started transaction txwThree : " + pointerWThree);
    WriteTransactionResult txResultW3 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(3)), pointerWThree);
    assertTrue(txResultW3.success);
    executor.addToTransaction(pointerWThree, txResultW3.undos);

    // write 4 with another transaction and also don't commit
    Transaction pointerWFour = executor.startTransaction();
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
        executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // commit 3, should fail
    assertFalse(executor.commitTransaction(pointerWThree).isSuccess());

    // read still sees 4
    assertArrayEquals(Bytes.toBytes(4),
        executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // now read with long-running read 1, should see value = 1
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, pointerReadOne, new Read(key, kvcol))
            .getValue().get(kvcol));

    // now do the same thing but in reverse order of conflict

    // write 5 with one transaction but don't commit
    Transaction pointerWFive = executor.startTransaction();
    // System.out.println("Started transaction txwFive : " + pointerWFive);
    WriteTransactionResult txResultW5 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(5)), pointerWFive);
    assertTrue(txResultW5.success);
    executor.addToTransaction(pointerWFive, txResultW5.undos);

    // write 6 with another transaction and also don't commit
    Transaction pointerWSix = executor.startTransaction();
    // System.out.println("Started transaction txwSix : " + pointerWSix);
    WriteTransactionResult txResultW6 =
      executor.write(context, new Write(key, kvcol, Bytes.toBytes(6)), pointerWSix);
    assertTrue(txResultW6.success);
    executor.addToTransaction(pointerWSix, txResultW6.undos);

    // read sees 4 still
    assertArrayEquals(Bytes.toBytes(4),
        executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // long running reads should still see their respective values
    assertArrayEquals(Bytes.toBytes(1), executor.execute(context, pointerReadOne, new Read(key,
                                                                                           kvcol)).getValue().get
      (kvcol));
    assertArrayEquals(Bytes.toBytes(2),
        executor.execute(context, pointerReadTwo, new Read(key, kvcol)).getValue().get(kvcol));

    // commit 5, should be successful
    assertTrue(executor.commitTransaction(pointerWFive).isSuccess());

    // read sees 5
    assertArrayEquals(Bytes.toBytes(5),
        executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // long running reads should still see their respective values
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, pointerReadOne, new Read(key, kvcol)).getValue().get(kvcol));
    assertArrayEquals(Bytes.toBytes(2),
        executor.execute(context, pointerReadTwo, new Read(key, kvcol)).getValue().get(kvcol));

    // commit 6, should fail
    assertFalse(executor.commitTransaction(pointerWSix).isSuccess());

    // read still sees 5
    assertArrayEquals(Bytes.toBytes(5),
        executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // long running reads should still see their respective values
    assertArrayEquals(Bytes.toBytes(1),
        executor.execute(context, pointerReadOne, new Read(key, kvcol)).getValue().get(kvcol));
    assertArrayEquals(Bytes.toBytes(2),
        executor.execute(context, pointerReadTwo, new Read(key, kvcol)).getValue().get(kvcol));
  }


  @Test
  public void testAbortedOperationsWithQueueAck() throws Exception {
    OmidTransactionalOperationExecutor.DISABLE_QUEUE_PAYLOADS = true;

    byte [] key = Bytes.toBytes("testAbortedAck");
    byte [] queueName = Bytes.toBytes("testAbortedAckQueue");

    // EnqueuePayload something
    executor.execute(context, batch(new QueueEnqueue(queueName, queueName)));

    // DequeuePayload it
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config = new QueueConfig(PartitionerType.RANDOM, true);
    DequeueResult dequeueResult = executor.execute(context,
        new QueueDequeue(queueName, consumer, config));
    assertTrue(dequeueResult.isSuccess());

    // Start our ack operation
    Transaction ackPointer = executor.startTransaction();

    // Start a fake operation that will just conflict with our key
    Transaction fakePointer = executor.startTransaction();
    Undo fakeUndo = new UndoWrite(null, key, new byte[][] { new byte[] {'a' } } );
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
    ackPointer = executor.startTransaction();

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
        executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol)));

    OmidTransactionalOperationExecutor.DISABLE_QUEUE_PAYLOADS = false;
  }

  @Test
  public void testDeletesCanBeTransacted() throws Exception {

    byte [] key = Bytes.toBytes("testDeletesCanBeTransacted");
    byte [] valueOne = Bytes.toBytes("valueOne");
    byte [] valueTwo = Bytes.toBytes("valueTwo");

    Transaction dirtyRead = new Transaction(1L, MemoryReadPointer.DIRTY_READ);

    List<WriteOperation> ops = new ArrayList<WriteOperation>();
    Delete delete = new Delete(key, kvcol);
    ops.add(delete);

    // Executing in a batch should succeed
    executor.execute(context, ops);

    // Executing singly should also succeed
    executor.execute(context, delete);

    // start tx one
    Transaction pointerOne = executor.startTransaction();
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
    Transaction pointerTwo = executor.startTransaction();
    // System.out.println("Started transaction two : " + pointerTwo);

    // delete value one
    WriteTransactionResult txResultTwo =
      executor.write(context, new Delete(key, kvcol), pointerTwo);
    assertTrue(txResultTwo.success);
    executor.addToTransaction(pointerTwo, txResultTwo.undos);

    // clean read should see it still
    assertArrayEquals(valueOne, executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // dirty read should NOT see it
    assertTrue(executor.execute(context, dirtyRead, new Read(key, kvcol)).isEmpty());

    // commit it
    assertTrue(executor.commitTransaction(pointerTwo).isSuccess());

    // clean read will not see it now
    assertTrue(executor.execute(context, new Read(key, kvcol)).isEmpty());

    // write value two
    executor.execute(context, new Write(key, kvcol, valueTwo));

    // clean read sees it
    assertArrayEquals(valueTwo, executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));

    // dirty read sees it
    assertArrayEquals(valueTwo, executor.execute(context, dirtyRead, new Read(key, kvcol)).getValue().get(kvcol));

    // start tx three
    Transaction pointerThree = executor.startTransaction();
    // System.out.println("Started transaction three : " + pointerThree);

    // start and commit a fake transaction which will overlap
    Transaction pointerFour = executor.startTransaction();
    Undo fakeUndo = new UndoWrite(null, key, new byte[][] { new byte[] {'a' } } );
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
    assertArrayEquals(valueTwo, executor.execute(context, new Read(key,kvcol)).getValue().get(kvcol));
    assertArrayEquals(valueTwo, executor.execute(context, dirtyRead, new Read(key, kvcol)).getValue().get(kvcol));
  }

  @Test
  public void testIncrementPassThru() throws Exception {

    byte [] key = Bytes.toBytes("testIncrementPassThru");

    byte [] columnOne = Bytes.toBytes("colOne");
    byte [] columnTwo = Bytes.toBytes("colTwo");

    byte [] queueOne = Bytes.toBytes("qOne");
    byte [] queueTwo = Bytes.toBytes("qTwo");

    byte [] queueOneData = Bytes.toBytes("queueOneData");
    byte [] queueTwoData = Bytes.toBytes("queueTwoData");

    // Generate a list of write operations that contain two increments and
    // two enqueue operations, one points to one operation, the other points to
    // both operations

    // Increment one (will tie to field "one/ONE" of first and second enqueues)
    Increment incrementOne = new Increment(key, columnOne, 1);

    // Increment two (will tie to field "two" of first enqueue)
    Increment incrementTwo = new Increment(key, columnTwo, 2);

    // Generate first enqueue payload tied to both increments
    Map<String,Long> enqueueOneMap = new TreeMap<String,Long>();
    enqueueOneMap.put("one", incrementOne.getId());
    enqueueOneMap.put("two", incrementTwo.getId());

    // Make the first enqueue operation using an enqueue payload with the map
    QueueEnqueue enqueueOne = new QueueEnqueue(queueOne,
        EnqueuePayload.write(enqueueOneMap, queueOneData));

    // Generate second enqueue payload tied to the first increment
    Map<String,Long> enqueueTwoMap = new TreeMap<String,Long>();
    enqueueTwoMap.put("ONE", incrementOne.getId());

    // Make the second enqueue operation using an enqueue payload with the map
    QueueEnqueue enqueueTwo = new QueueEnqueue(queueTwo,
        EnqueuePayload.write(enqueueTwoMap, queueTwoData));

    // Make a batch of operations, putting enqueues first knowing that these
    // must be reordered to after the increment operations
    List<WriteOperation> batch = new ArrayList<WriteOperation>(4);
    batch.add(enqueueOne);
    batch.add(enqueueTwo);
    batch.add(incrementOne);
    batch.add(incrementTwo);

    // Execute the batch!
    executor.execute(context, batch);

    // Dequeueing from these queues should yield the post increment values
    QueueConsumer consumer = new QueueConsumer(0, 1, 1);
    QueueConfig config = new QueueConfig(PartitionerType.RANDOM, false);

    // Dequeue from queue one, expect two fields, one=1 and two=2
    QueueDequeue dequeueOne = new QueueDequeue(queueOne, consumer, config);
    DequeueResult dequeueOneResult = executor.execute(context, dequeueOne);
    assertTrue(dequeueOneResult.isSuccess());
    assertFalse(dequeueOneResult.isEmpty());
    byte [] dequeueOneData = dequeueOneResult.getValue();
    DequeuePayload dequeueOnePayload = DequeuePayload.read(dequeueOneData);
    Map<String,Long> dequeueOneValues = dequeueOnePayload.getValues();
    assertEquals(2, dequeueOneValues.size());
    assertEquals(new Long(1), dequeueOneValues.get("one"));
    assertEquals(new Long(2), dequeueOneValues.get("two"));
    assertTrue(Bytes.equals(queueOneData,
        dequeueOnePayload.getSerializedTuple()));

    // Dequeue from queue two, expect one field, ONE=1
    QueueDequeue dequeueTwo = new QueueDequeue(queueTwo, consumer, config);
    DequeueResult dequeueTwoResult = executor.execute(context, dequeueTwo);
    assertTrue(dequeueTwoResult.isSuccess());
    assertFalse(dequeueTwoResult.isEmpty());
    byte [] dequeueTwoData = dequeueTwoResult.getValue();
    DequeuePayload dequeueTwoPayload = DequeuePayload.read(dequeueTwoData);
    Map<String,Long> dequeueTwoValues = dequeueTwoPayload.getValues();
    assertEquals(1, dequeueTwoValues.size());
    assertEquals(new Long(1), dequeueTwoValues.get("ONE"));
    assertTrue(Bytes.equals(queueTwoData,
        dequeueTwoPayload.getSerializedTuple()));
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
    executor.execute(context, new Write(table, rowkey,
        new byte[][] { col1, col2, col3, col4 },
        new byte[][] { val1, val2, val3, val4 }));

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
    OperationResult<Map<byte[],byte[]>> result3 =
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
    executor.execute(context, new Increment(table, rowkey, col3, 1L));
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
    executor.execute(context, new Delete(table, rowkey, col2));
    {
    // verify it's gone
    OperationResult<Map<byte[], byte[]>> result6 = executor.execute(context,
        new Read(table, rowkey, col2));
    Assert.assertTrue(result6.isEmpty());
    Assert.assertEquals(StatusCode.COLUMN_NOT_FOUND, result6.getStatus());
    }
    // compare-and-swap with success
    executor.execute(context, new CompareAndSwap(table, rowkey, col3,
        Bytes.toBytes(8L), Bytes.toBytes(3L)));
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
      executor.execute(context, new CompareAndSwap(table, rowkey, col3,
          Bytes.toBytes(8L), Bytes.toBytes(17L)));
      fail("Write conflict exception expected.");
    } catch (OperationException e) {
      // expected write conflict
      if (e.getStatus() != StatusCode.WRITE_CONFLICT)
        throw e;
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
    final String tableA= "tableA", tableB = "tableB";
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
    executor.execute(context, new Write(rowX, colX, valX));
    executor.execute(context, new Write(tableA, rowY, colY, val42));
    executor.execute(context, new Write(tableB, rowZ, colZ, valZ));

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
      executor.execute(context, writes);
      fail("Expected compare-and-swap to fail batch.");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.WRITE_CONFLICT)
        throw e; // only write confict is expected
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
    executor.execute(context, new Write(table, rowX, colX, valX));

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
    executor.execute(context, new Write(table, rowX, colX, valX1));
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
    executor.execute(context1, new Write(table, rowX, colX, valX));
    executor.execute(context2, new Write(table, rowX, colY, valY));

    // verify each context see its own writes
    Assert.assertArrayEquals(valX, executor.execute(
        context1, new Read(table, rowX, colX)).getValue().get(colX));
    Assert.assertArrayEquals(valY, executor.execute(
        context2, new Read(table, rowX, colY)).getValue().get(colY));

    // verify that each context can not see the others writes
    OperationResult<Map<byte[],byte[]>> result1 =
        executor.execute(context1, new Read(table, rowX, colY));
    OperationResult<Map<byte[],byte[]>> result2 =
        executor.execute(context2, new Read(table, rowX, colX));
    Assert.assertTrue(result1.isEmpty()
        || result1.getValue().get(colY) == null);
    Assert.assertTrue(result2.isEmpty()
        || result2.getValue().get(colX) == null);

    // clear the tables for one context
    executor.execute(context1, new ClearFabric(ClearFabric.ToClear.TABLES));

    // verify the table is gone for that context
    OperationResult<Map<byte[],byte[]>> result3 =
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
    Transaction tx1 = executor.startTransaction(context);
    Transaction tx2 = executor.execute(context, null, batch(new Write(table, a, col, me)));
    // increment a counter with both transactions
    executor.execute(context, tx1, batch(new Increment(table, b, x, 1L)));
    executor.execute(context, tx2, batch(new Increment(table, c, x, 5L)));
    // fail the first transaction with a c-a-s
    try {
      executor.execute(context, tx1, batch(new CompareAndSwap(table, x, me, me)));
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
    Transaction tx1 = executor.startTransaction(context);
    executor.execute(context, tx1, batch(new Write(table, a, x, one)));
    // write row b with the second transaction
    Transaction tx2 = executor.startTransaction(context);
    executor.execute(context, tx2, batch(new Write(table, b, x, two)));
    // write to a different row x with the third transaction
    Transaction tx3 = executor.startTransaction(context);
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
    Transaction tx1 = executor.startTransaction(context);
    // set a row to 10
    executor.execute(context, tx1, batch(new Write(table, row, col, Bytes.toBytes(10L))));
    // increment the row in another transaction
    Transaction tx2 = executor.startTransaction(context);
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
    Transaction tx = executor.startTransaction();
    // write a value in that tx
    executor.execute(context, tx, batch(new Write(table, f, x, one)));
    // read the value outside the tx -> null
    Assert.assertTrue(executor.execute(context, new Read(table, f, x)).isEmpty());
    // read the value inside the tx -> visible
    Assert.assertArrayEquals(one, executor.execute(context, tx, new Read(table, f, x)).getValue().get(x));
    // write another value outside the tx
    executor.execute(context, batch(new Write(table, g, x, one)));
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
    Transaction tx1 = executor.startTransaction(context);
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
    executor.commit(context, tx1, batch(new QueueEnqueue(qname, EnqueuePayload.write(new TreeMap<String, Long>(),
                                                                                     value))));
    // dequeue
    DequeueResult deqres = executor.execute(
      context, new QueueDequeue(qname, new QueueConsumer(0, 0, 1), new QueueConfig(PartitionerType.RANDOM, true)));
    // verify the value
    Assert.assertFalse(deqres.isEmpty());
    Assert.assertArrayEquals(value, DequeuePayload.read(deqres.getValue()).getSerializedTuple());
    // in a new transaction, write the dequeued value and ack the queue entry
    executor.execute(context, batch(new Write(table, g, x, value),
                                    new QueueAck(qname, deqres.getEntryPointer(), new QueueConsumer(0, 0, 1))));
    // attempt to dequeue again, should be empty
    deqres = executor.execute(
      context, new QueueDequeue(qname, new QueueConsumer(0, 0, 1), new QueueConfig(PartitionerType.RANDOM, true)));
    // verify the value
    Assert.assertTrue(deqres.isEmpty());
    // verify the write
    Assert.assertArrayEquals(value, executor.execute(context, new Read(table, g, x)).getValue().get(x));
  }

  // test increment with return value
  @Test
  public void testIncrementWithReturn() throws OperationException {
    final String table = "tIWR";
    final byte[] first = {'f','r'};
    final byte[] second = {'s','r'};
    final byte[] a = {'a'};
    final byte[] b = {'b'};
    final byte[] c = {'c'};
    final byte[][] ab = {a,b};
    final byte[][] abc = {a,b,c};

    // write two out of three columns
    executor.execute(context, new Write(table, first, ab, new byte[][] {
      Bytes.toBytes(1L), Bytes.toBytes(10L) }));

    // start a transaction
    Transaction tx1 = executor.startTransaction(context);
    // increment these columns within the transaction
    OperationResult<Map<byte[], Long>> res = executor.
      execute(context, tx1, new Increment(table, first, abc, new long[] { 1, 2, 55 } ));
    // verify return values
    Assert.assertFalse(res.isEmpty());
    Assert.assertEquals(new Long(2L), res.getValue().get(a));
    Assert.assertEquals(new Long(12L), res.getValue().get(b));
    Assert.assertEquals(new Long(55L), res.getValue().get(c));
    // write to a new row and commit transaction
    executor.commit(context, tx1, batch(new Write(table, second, abc, new byte[][] {
      Bytes.toBytes(res.getValue().get(a)),
      Bytes.toBytes(res.getValue().get(b)),
      Bytes.toBytes(res.getValue().get(c)) })));

    // increment new row in own tx
    res = executor.execute(context, new Increment(table, second, abc, new long[]{1, 3, 11}));
    // verify return values
    Assert.assertFalse(res.isEmpty());
    Assert.assertEquals(new Long(3L), res.getValue().get(a));
    Assert.assertEquals(new Long(15L), res.getValue().get(b));
    Assert.assertEquals(new Long(66L), res.getValue().get(c));

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
    Transaction tx1 = executor.startTransaction(context);
    Transaction tx2 = executor.startTransaction(context);
    Transaction tx3 = executor.startTransaction(context);

    // all transactions write the same row to different tables, one uses default table
    executor.execute(context, tx1, batch(new Write(a, b, b)));
    executor.execute(context, tx2, batch(new Write(t2, a, b, b)));
    executor.execute(context, tx3, batch(new Write(t3, a, b, b)));

    // commit all transactions, none should fail
    executor.commit(context, tx1); // succeeds anyway
    executor.commit(context, tx2); // no conflict between named table and default table
    executor.commit(context, tx3); // no conflict between two named tables
  }

}
