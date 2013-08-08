/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.QueueConsumer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class HBaseQueueTest extends HBaseTestBase {

  private static Logger LOG = LoggerFactory.getLogger(HBaseQueueTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static Configuration hConf;
  private static InMemoryZKServer zkServer;
  private static OperationExecutorService opexService;
  private static OperationExecutor opex;

  @Test
  public void testFilter() throws IOException {
    byte[] columnFamily = "c".getBytes(Charsets.UTF_8);
    byte[] column = columnFamily;
    HBaseUtils.createTableIfNotExists(getHBaseAdmin(), "testFilter", columnFamily, 1000);
    HTable hTable = new HTable(getConfiguration(), "testFilter");

    // Insert 10 rows
    for (int i = 0; i < 10; i++) {
      byte[] value = new byte[Ints.BYTES * 2 + 1];
      Bytes.putInt(value, 0, i);
      Bytes.putInt(value, Ints.BYTES, i % 2);

      if (i % 2 == 0) {
        value[value.length - 1] = ConsumerEntryState.CLAIMED.getState();
      } else {
        value[value.length - 1] = ConsumerEntryState.PROCESSED.getState();
      }

      Put put = new Put(Bytes.toBytes(i));
      put.add(columnFamily, "d".getBytes(Charsets.UTF_8), Bytes.toBytes(i));
      put.add(columnFamily, column, value);
      hTable.put(put);
    }
    // Insert 10 rows without state
    for (int i = 10; i < 20; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(columnFamily, "d".getBytes(Charsets.UTF_8), Bytes.toBytes(i));
      hTable.put(put);
    }

    // Scan
    Scan scan = new Scan();
    scan.addFamily(columnFamily);

    // Filter for (writePointer < readPointer)
    int readPointer = 6;
    Filter commitFilter = new SingleColumnValueFilter(columnFamily, column,
                                                      CompareFilter.CompareOp.GREATER,
                                                      new BinaryPrefixComparator(Bytes.toBytes(readPointer)));

    // Filters for excluded list
    FilterList excludedFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    for (int excluded : new int[]{ 1, 4 }) {
      excludedFilter.addFilter(new SingleColumnValueFilter(columnFamily, column,
                                                           CompareFilter.CompareOp.EQUAL,
                                                           new BinaryPrefixComparator(Bytes.toBytes(excluded))));
    }

    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
                                  ImmutableList.of(commitFilter, excludedFilter)));

    ResultScanner scanner = hTable.getScanner(scan);
    Result result;
    while ((result = scanner.next()) != null) {
      System.out.println(Bytes.toInt(result.getRow()));
    }
  }

  @Test
  public void testSingleFifo() throws Exception {
    // Create the queue table
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "out");
    final HBaseQueueClient queueClient = new HBaseQueueClient(HBaseTestBase.getHBaseAdmin(), "queueTest", queueName);

    final int count = 5000;
    LOG.info("Start enqueue {} entries.", count);

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count; i++) {
      Transaction transaction = opex.start();

      try {
        byte[] queueData = Bytes.toBytes(i);
        queueClient.startTx(transaction);
        queueClient.enqueue(new QueueEntry(queueData));

        if (opex.canCommit(transaction, queueClient.getTxChanges()) && queueClient.commitTx()) {
          if (!opex.commit(transaction)) {
            queueClient.rollbackTx();
          }
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Enqueue {} entries in {} ms", count, elapsed);
    LOG.info("Average {} entries per seconds", (double)count * 1000 / elapsed);

    // Try to dequeue
    final int expectedSum = (count / 2 * (count - 1));
    final AtomicInteger valueSum = new AtomicInteger();
    final int consumerSize = 5;
    final CyclicBarrier startBarrier = new CyclicBarrier(consumerSize + 1);
    final CountDownLatch completeLatch = new CountDownLatch(consumerSize);
    ExecutorService executor = Executors.newFixedThreadPool(consumerSize);
    for (int i = 0; i < consumerSize; i++) {
      final int instanceId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startBarrier.await();
            QueueConsumer consumer = queueClient.createConsumer(
              new ConsumerConfig(0, instanceId, consumerSize, DequeueStrategy.FIFO, null));
            TransactionAware txAware = (TransactionAware)consumer;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            int dequeueCount = 0;
            while (valueSum.get() != expectedSum) {
              Transaction transaction = opex.start();
              txAware.startTx(transaction);

              try {
                DequeueResult result = consumer.dequeue();
                if (opex.canCommit(transaction, queueClient.getTxChanges()) && txAware.commitTx()) {
                  if (!opex.commit(transaction)) {
                    txAware.rollbackTx();
                  }
                }
                if (result.isEmpty()) {
                  continue;
                }

                byte[] data = result.getData().iterator().next();
                valueSum.addAndGet(Bytes.toInt(data));
                dequeueCount++;
              } catch (Exception e) {
                opex.abort(transaction);
                throw Throwables.propagate(e);
              }
            }

            long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
            LOG.info("Dequeue {} entries in {} ms", dequeueCount, elapsed);
            LOG.info("Average {} entries per seconds", (double)dequeueCount * 1000 / elapsed);
            completeLatch.countDown();
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
        }
      });
    }

    startBarrier.await();
    Assert.assertTrue(completeLatch.await(40, TimeUnit.SECONDS));
    TimeUnit.SECONDS.sleep(2);

    Assert.assertEquals(expectedSum, valueSum.get());
    executor.shutdownNow();
  }

  @Test
  public void testSingleHash() throws Exception {
    // Create the queue table
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "out");
    HBaseQueueClient queueClient = new HBaseQueueClient(HBaseTestBase.getHBaseAdmin(), "queueTest", queueName);

    int count = 5000;
    LOG.info("Start enqueue {} entries.", count);

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count; i++) {
      Transaction transaction = opex.start();

      try {
        byte[] queueData = ("queue data " + i).getBytes(Charsets.UTF_8);
        queueClient.startTx(transaction);
        queueClient.enqueue(new QueueEntry(queueData));

        if (opex.canCommit(transaction, queueClient.getTxChanges()) && queueClient.commitTx()) {
          if (!opex.commit(transaction)) {
            queueClient.rollbackTx();
          }
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Enqueue {} entries in {} ms", count, elapsed);
    LOG.info("Average {} entries per seconds", (double)count * 1000 / elapsed);

    // Try to dequeue
    QueueConsumer consumer = queueClient.createConsumer(new ConsumerConfig(0, 0, 1, DequeueStrategy.HASH, "key"));
    TransactionAware txAware = (TransactionAware)consumer;

    stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count; i++) {
      Transaction transaction = opex.start();
      txAware.startTx(transaction);

      try {
        DequeueResult result = consumer.dequeue();
        if (opex.canCommit(transaction, queueClient.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
        Assert.assertEquals(("queue data " + i), new String(result.getData().iterator().next(), Charsets.UTF_8));
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Dequeue {} entries in {} ms", count, elapsed);
    LOG.info("Average {} entries per seconds", (double)count * 1000 / elapsed);
  }

  @Test
  public void testBatchHash() throws OperationException, IOException {
    // Create the queue table
    HBaseQueueClient queueClient = new HBaseQueueClient(HBaseTestBase.getHBaseAdmin(), "queueTest",
                                                        QueueName.fromFlowlet("flow", "flowlet", "out"));

    int count = 5000;
    int batchSize = 5;
    byte[] queueData = "queue data".getBytes(Charsets.UTF_8);
    LOG.info("Start enqueue {} entries with batch size {}.", count, batchSize);

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count / batchSize; i++) {
      Transaction transaction = opex.start();

      try {
        queueClient.startTx(transaction);

        for (int j = 0; j < batchSize; j++) {
          queueClient.enqueue(new QueueEntry(queueData));
        }

        if (opex.canCommit(transaction, queueClient.getTxChanges()) && queueClient.commitTx()) {
          if (!opex.commit(transaction)) {
            queueClient.rollbackTx();
          }
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Enqueue {} entries of batch size {} in {} ms", count, batchSize, elapsed);
    LOG.info("Average {} entries per seconds", (double)count * 1000 / elapsed);

    // Try to dequeue
    batchSize = 50;
    QueueConsumer consumer = queueClient.createConsumer(new ConsumerConfig(0, 0, 1, DequeueStrategy.HASH, "key"));
    TransactionAware txAware = (TransactionAware)consumer;

    stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count / batchSize; i++) {
      Transaction transaction = opex.start();
      txAware.startTx(transaction);

      try {
        DequeueResult result = consumer.dequeue(batchSize);
        if (opex.canCommit(transaction, queueClient.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
        int j = 0;
        for (byte[] data : result.getData()) {
          Assert.assertEquals("queue data", new String(data, Charsets.UTF_8));
          j++;
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Dequeue {} entries in {} ms of batch size {}", count, elapsed, batchSize);
    LOG.info("Average {} entries per seconds", (double)count * 1000 / elapsed);
  }

  @BeforeClass
  public static void init() throws Exception {
    // Start ZooKeeper
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    // Start hbase
    HBaseTestBase.startHBase();
    hConf = HBaseTestBase.getConfiguration();

    final DataFabricDistributedModule dataFabricModule = new DataFabricDistributedModule(hConf);

    // Customize test configuration
    final CConfiguration cConf = dataFabricModule.getConfiguration();
    cConf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkServer.getConnectionStr());
    cConf.set(com.continuuity.data.operation.executor.remote.Constants.CFG_DATA_OPEX_SERVER_PORT,
              Integer.toString(Networks.getRandomPort()));

    final Injector injector = Guice.createInjector(dataFabricModule);

    opexService = injector.getInstance(OperationExecutorService.class);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          opexService.start(new String[]{}, cConf);
        } catch (ServerException e) {
          LOG.error("Exception.", e);
        }
      }
    };
    t.start();

    // Get the remote opex
    opex = injector.getInstance(OperationExecutor.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    opexService.stop(true);
    HBaseTestBase.stopHBase();
    zkServer.stopAndWait();
  }
}
