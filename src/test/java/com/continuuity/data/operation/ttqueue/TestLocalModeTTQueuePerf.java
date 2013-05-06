package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

public class TestLocalModeTTQueuePerf {

  //  private static final Properties hsqlProperties = new Properties();

  //  private static final String hsql = "jdbc:hsqldb:file:/db/benchdb";
  //  private static final String hsql = "jdbc:hsqldb:mem:membenchdb";

  private static final DataFabricLocalModule module =
      new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null);
  //  new DataFabricLocalModule();

  private static final Injector injector = Guice.createInjector(module);

  private static final OVCTableHandle handle =
      injector.getInstance(OVCTableHandle.class);

  //  // Configuration for hypersql
  //  static {
  //    // Assume 1K rows and 512MB cache size
  //    hsqlProperties.setProperty("hsqldb.cache_rows", "" + 512000);
  //    hsqlProperties.setProperty("hsqldb.cache_size", "" + 512000);
  //    // Disable logging
  //    hsqlProperties.setProperty("hsqldb.log_data", "false");
  //  }

  //  // Configuration for hypersql bench
  //  private static final BenchConfig config = new BenchConfig();
  //  static {
  //    config.numJustEnqueues = 1000;
  //    config.queueEntrySize = 10;
  //    config.numEnqueuesThenSyncDequeueAckFinalize = 1000;
  //  }


  @Test
  public void test100EnqueuesThenSyncDequeues() throws Exception {
    testNEnqueuesThenSyncDequeues(100);
  }

  @Test @Ignore
  public void test1kEnqueuesThenSyncDequeues() throws Exception {
    testNEnqueuesThenSyncDequeues(1000);
  }

  @Test @Ignore
  public void test10kEnqueuesThenSyncDequeues() throws Exception {
    testNEnqueuesThenSyncDequeues(10000);
  }

  private void testNEnqueuesThenSyncDequeues(int n) throws OperationException {

    byte [] queueName = Bytes.toBytes("queue://qtn_" + n);
    byte [] streamName = Bytes.toBytes("stream://stn_" + n);

    byte [] data = new byte[1024];
    long version = 10L;

    QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(PartitionerType.FIFO, true));
    MemoryReadPointer memoryReadPointer = new MemoryReadPointer(version, version, null);
    Transaction transaction = new Transaction(memoryReadPointer, memoryReadPointer);

    // first test it with the intra-flow queues
    TTQueueTable queueTable = handle.getQueueTable(queueName);
    queueTable.configure(queueName, consumer, TransactionOracle.DIRTY_READ_POINTER);

    // second test it with the stream queues
    TTQueueTable streamTable = handle.getStreamTable(streamName);
    streamTable.configure(streamName, consumer, TransactionOracle.DIRTY_READ_POINTER);

    log("Enqueueing to queue table");
    long start = now();
    long last = start;
    for (int i=0; i<n; i++) {
      queueTable.enqueue(queueName, new QueueEntry(data), transaction);
      last = printStat(i, last, 1000);
    }
    printReport(start, now(), n);
    log("Done enqueueing to queue table");

    log("Dequeueing from queue table");
    start = now();
    last = start;
    for (int i=0; i<n; i++) {
      DequeueResult result = queueTable.dequeue(queueName, consumer, memoryReadPointer);
      queueTable.ack(queueName, result.getEntryPointer(), consumer, transaction);
      queueTable.finalize(queueName, result.getEntryPointers(), consumer, -1, transaction);
      last = printStat(i, last, 1000);
    }
    printReport(start, now(), n);
    log("Done dequeueing from queue table");

    log("Enqueueing to stream table");
    start = now();
    last = start;
    for (int i=0; i<n; i++) {
      streamTable.enqueue(streamName, new QueueEntry(data), transaction);
      last = printStat(i, last, 1000);
    }
    printReport(start, now(), n);
    log("Done enqueueing to stream table");

    log("Dequeueing from stream table");
    start = now();
    last = start;
    for (int i=0; i<n; i++) {
      DequeueResult result =
          streamTable.dequeue(streamName, consumer, memoryReadPointer);
      streamTable.ack(streamName, result.getEntryPointer(), consumer, transaction);
      streamTable.finalize(streamName, result.getEntryPointers(), consumer, -1, transaction);
      last = printStat(i, last, 1000);
    }
    printReport(start, now(), n);
    log("Done dequeueing from stream table");


  }

  private long printStat(int i, long last, int perline) {
    i++;
    if (i % (perline/10) == 0) System.out.print(".");
    if (i % perline == 0) {
      System.out.println(" " + i + " : Last " + perline + " finished in " +
          timeReport(last, now(), perline));
      return now();
    }
    return last;
  }

  private void printReport(long start, long end, int iterations) {
    log("Finished " + iterations + " iterations in " +
        timeReport(start, end, iterations));
  }

  private String timeReport(long start, long end, int iterations) {
    return "" + format(end-start) + " (" +
        format(end-start, iterations) + "/iteration)";
  }

  private String format(long time, int iterations) {
    return "" + (time/(float)iterations) + "ms";
  }

  private String format(long time) {
    if (time < 1000) return "" + time + "ms";
    if (time < 60000) return "" + (time/(float)1000) + "sec";
    long min = time / 60000;
    float sec = (time - (min*60000)) / (float)1000;
    return "" + min + "min " + sec + "sec";
  }

  protected void log(String msg) {
    System.out.println("" + now() + " : " + msg);
  }

  protected long now() {
    return System.currentTimeMillis();
  }

}