package com.continuuity.data.stream;


import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 */
@Ignore
public class TestStreamTable {

  private static final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
  protected static TransactionOracle oracle = injector.getInstance(TransactionOracle.class);
  protected static TimestampOracle timeOracle = injector.getInstance(TimestampOracle.class);

  protected long getDirtyWriteVersion(){
    return TransactionOracle.DIRTY_WRITE_VERSION;
  }

  /**
   * Scenario: Simple read/write test - Write 10 entries and read all the entries written
   * Expected result: No errors while writing, read all the 10 entries written
   * @throws OperationException
   */
  @Test
  public void testSimpleReadWrite() throws OperationException {
    CConfiguration conf = new CConfiguration();

    StreamQueueConsumer consumer = new StreamQueueConsumer(0, 0, 1,
                                                      new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true));

    TTQueueTableOnVCTable table = new TTQueueTableOnVCTable( new MemoryOVCTable(Bytes.toBytes("TestStreamMemoryQueue")),
                                                             oracle,conf);

    OrderedVersionedColumnarTable metaTable = new MemoryOVCTable(Bytes.toBytes("StreamMeta"));
    StreamTable streamTable = new StreamTable(Bytes.toBytes("StreamTest"),table, metaTable, -1);
    MemoryReadPointer readPointer = new MemoryReadPointer(timeOracle.getTimestamp());

    // Write to Stream 10 times
    int countEnqueue = writeNTimesToStream(10, streamTable);
    assertTrue(10 == countEnqueue);

    // Read from the stream and verify if Count of entires is 10
    int countDequeue = readAllEntriesFromStream(streamTable, consumer, readPointer);
    assertTrue(10 == countDequeue);

  }

  /**
   * Scenario: a) Write to Stream b) Read c ) Ack d) Un-Ack e) Read again
   * Expected result: Same entry should be read in both read cycles
   * @throws OperationException
   */
  @Test
  public void testUnack() throws OperationException {

    CConfiguration conf = new CConfiguration();

    StreamQueueConsumer consumer = new StreamQueueConsumer(0, 0, 1,
                                                          new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true));

    TTQueueTableOnVCTable table = new TTQueueTableOnVCTable( new MemoryOVCTable(Bytes.toBytes("TestStreamMemoryQueue")),
                                                             oracle,conf);
    OrderedVersionedColumnarTable metaTable = new MemoryOVCTable(Bytes.toBytes("StreamMeta"));
    StreamTable streamTable = new StreamTable(Bytes.toBytes("StreamTest"),table, metaTable, -1);

    MemoryReadPointer readPointer = new MemoryReadPointer(timeOracle.getTimestamp());

    int countEntriesWritten = writeNTimesToStream(1, streamTable);
    assertTrue(1== countEntriesWritten);

    StreamReadResult readResult = streamTable.read(consumer, readPointer);
    assertTrue(readResult.isSuccess());

    streamTable.ack(StreamEntryPointer.fromQueueEntryPointer(readResult.getEntryPointer()),consumer,readPointer);
    streamTable.unack(StreamEntryPointer.fromQueueEntryPointer(readResult.getEntryPointer()), consumer, readPointer);


    int countEntriesRead  = readAllEntriesFromStream(streamTable, consumer, readPointer);
    assertTrue(1==countEntriesRead);

  }

  /**
   * Scenario: Read from an arbitrary partition in a stream. a) Write 13 Entries b) Set partition to read last 3 entries
   * Expected result: Reading entire stream should just read last 3 entries
   * @throws OperationException
   * @throws InterruptedException
   */
  @Test
  public void testStreamWithPartition() throws OperationException, InterruptedException {
    CConfiguration conf = new CConfiguration();

    StreamQueueConsumer consumer = new StreamQueueConsumer(0, 0, 1,
                                                          new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true));


    TTQueueTableOnVCTable table = new TTQueueTableOnVCTable( new MemoryOVCTable(Bytes.toBytes("TestStreamMemoryQueue")),
                                                             oracle,conf);

    OrderedVersionedColumnarTable metaTable = new MemoryOVCTable(Bytes.toBytes("StreamMeta"));
    StreamTable streamTable = new StreamTable(Bytes.toBytes("StreamTest"),table, metaTable, -1);

    MemoryReadPointer readPointer = new MemoryReadPointer(timeOracle.getTimestamp());

    int countEntriesWritten = writeNTimesToStream(10, streamTable);
    assertTrue(10==countEntriesWritten);

    long partition = System.currentTimeMillis()/1000;
    Thread.sleep(2000);

    int countEntriesWrittenAgain = writeNTimesToStream(3, streamTable);
    assertTrue(3==countEntriesWrittenAgain);

    streamTable.setStartPartition(partition);

    int countEntriesRead = readAllEntriesFromStream(streamTable, consumer, readPointer);
    assertTrue(3==countEntriesRead);

    int countEntriesReadAgain = readAllEntriesFromStream(streamTable, consumer, readPointer);
    assertTrue(0==countEntriesReadAgain);

  }

  /**
   * Scenario: Clear Streams. a) Enqueue n items b) Clear Stream c) Dequeue All
   * Expected Result: Dequeue Count should be 0
   * @throws OperationException
   */
  @Test
  public void testClear() throws OperationException{
    CConfiguration conf = new CConfiguration();

    StreamQueueConsumer consumer = new StreamQueueConsumer(0, 0, 1,
                                                           new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true));

    TTQueueTableOnVCTable table = new TTQueueTableOnVCTable( new MemoryOVCTable(Bytes.toBytes("TestStreamMemoryQueue")),
                                                             oracle,conf);

    OrderedVersionedColumnarTable metaTable = new MemoryOVCTable(Bytes.toBytes("StreamMeta"));
    StreamTable streamTable = new StreamTable(Bytes.toBytes("StreamTest"),table, metaTable, -1);

    MemoryReadPointer readPointer = new MemoryReadPointer(timeOracle.getTimestamp());

    int countEntriesWritten = writeNTimesToStream(10, streamTable);
    assertTrue(10==countEntriesWritten);

    streamTable.clear();

    int countDequeueAll = readAllEntriesFromStream(streamTable,consumer,readPointer);
    assertTrue(0==countDequeueAll);
  }

  private int writeNTimesToStream(int n, StreamTable table) throws OperationException {
    int count=0;
    for(int i=0; i< n; i++) {
      count++;
      StreamWriteResult result=  table.write(new StreamEntry(Bytes.toBytes("test")),getDirtyWriteVersion() );
      assertTrue(result.isSuccess());
    }
    return count;
  }

  private int readAllEntriesFromStream(StreamTable table, StreamQueueConsumer consumer, ReadPointer readPointer)
                                                                                            throws OperationException {
    boolean allRead = false;
    int count =0;
    while (!allRead){
      StreamReadResult readResult = table.read(consumer, readPointer);
      if (readResult.isSuccess()) {
        count++;
        table.ack(StreamEntryPointer.fromQueueEntryPointer(readResult.getEntryPointer()),consumer, readPointer);
      }
      else {
        allRead = true;
      }
    }
    return count;

  }
}