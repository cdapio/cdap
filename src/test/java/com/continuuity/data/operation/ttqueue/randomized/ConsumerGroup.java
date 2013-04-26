package com.continuuity.data.operation.ttqueue.randomized;

import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

/**
*
*/
public class ConsumerGroup implements Runnable {
  private final int id;
  private final TransactionOracle oracle;
  private final ListeningExecutorService listeningExecutorService;
  private final TestConfig testConfig;
  private final TestController testController;
  private final TTQueue queue;
  private final Queue<Integer> groupDequeueList;

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroup.class);

  public ConsumerGroup(int id, TransactionOracle oracle, ListeningExecutorService listeningExecutorService,
                       TestConfig testConfig, TestController testController, TTQueue queue,
                       Queue<Integer> groupDequeueList) {
    this.id = id;
    this.oracle = oracle;
    this.listeningExecutorService = listeningExecutorService;
    this.testConfig = testConfig;
    this.testController = testController;
    this.queue = queue;
    this.groupDequeueList = groupDequeueList;
  }

  @Override
  public void run() {
    final QueuePartitioner.PartitionerType partitionerType = testConfig.getPartitionType();
    LOG.info(getLogMessage(String.format("Partition type=%s, batch", partitionerType)));

    int run = 0;
    try {
      while(true) {
        run++;
        // Create consumers
        final int numConsumers = testConfig.getNumConsumers();
        final int batchSize = testConfig.getConsumerBatchSize();
        final boolean batchReturn = testConfig.shouldBatchReturn();
        final boolean isAsync = testConfig.shouldRunAsync();
        QueueConfig config = new QueueConfig(partitionerType, !isAsync, batchSize, batchReturn);
        LOG.info(getLogMessage(String.format("Run=%d, Num consumers=%d, batchSize=%d, batchReturn=%s, isAsync=%s",
                                             run, numConsumers, batchSize, batchReturn, isAsync)));
        List<ListenableFuture<?>> consumerFutures = Lists.newArrayList();
        ConsumerGroupControl consumerGroupControl = new ConsumerGroupControl(id, numConsumers,
                                                                             testConfig.getNumDequeueRuns());
        Map<Integer, Queue<Integer>> groupMap = Maps.newConcurrentMap();
        for(int i = 0; i < numConsumers; ++i) {
          groupMap.put(i, new ConcurrentLinkedQueue<Integer>());
          ListenableFuture<?> future = listeningExecutorService.submit(
            new Consumer(i, oracle, consumerGroupControl, listeningExecutorService, testConfig, testController,
                         queue, config, groupMap.get(i)));
          consumerFutures.add(future);
        }

        // Wait for all consumers to complete
        final Future<?> compositeConsumerFuture = Futures.allAsList(consumerFutures);
        compositeConsumerFuture.get();

        Iterables.addAll(groupDequeueList, Iterables.concat(groupMap.values()));

//        for(int i = 0; i < numConsumers; ++i) {
//          LOG.info(getLogMessage("Consumer:" + i + " dequeueList=" + groupMap.get(i)));
//        }

        if(consumerGroupControl.getConsumersAtQueueEnd().size() == numConsumers) {
          break;
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogMessage(String message) {
    return String.format("Consumer Group:%d, %s", id, message);
  }
}
