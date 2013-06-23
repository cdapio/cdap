package com.continuuity.data.operation.ttqueue.randomized;

import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
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
  private final int numGroups;
  private final ConsumerGroupControl groupControl;
  private final TransactionOracle oracle;
  private final ListeningExecutorService listeningExecutorService;
  private final TestConfig testConfig;
  private final TestController testController;
  private final TTQueue queue;
  private final Queue<Integer> groupDequeueList;

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroup.class);

  public ConsumerGroup(int id, int numGroups, ConsumerGroupControl groupControl,
                       TransactionOracle oracle, ListeningExecutorService listeningExecutorService,
                       TestConfig testConfig, TestController testController, TTQueue queue,
                       Queue<Integer> groupDequeueList) {
    this.id = id;
    this.numGroups = numGroups;
    this.groupControl = groupControl;
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
      // Wait for setup to be done
      LOG.info(getLogMessage("Waiting for test to start..."));
      testController.waitToStart();
      LOG.info(getLogMessage("Starting."));

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

        ConsumerControl consumerControl = new ConsumerControl(id, numConsumers, testConfig.getNumDequeueRuns());

        List<ListenableFuture<?>> consumerFutures = Lists.newArrayList();
        Map<Integer, Queue<Integer>> groupMap = Maps.newConcurrentMap();
        List<QueueConsumer> consumers = Lists.newArrayListWithCapacity(numConsumers);
        for(int i = 0; i < numConsumers; ++i) {
          QueueConsumer consumer =
            new StatefulQueueConsumer(i, id, numConsumers, "", TTQueueRandomizedTest.HASH_KEY, config);
          consumers.add(consumer);
          queue.configure(consumer, oracle.getReadPointer());
        }

        // Configure done, wait for all groups to finish configuration
        LOG.info(getLogMessage("Config done. Waiting for others to finish configuring"));
        groupControl.getConfigBarrier().await();
        LOG.info(getLogMessage("All config done, starting"));

        // Start consumers
        for(QueueConsumer consumer : consumers) {
          groupMap.put(consumer.getInstanceId(), new ConcurrentLinkedQueue<Integer>());
          ListenableFuture<?> future = listeningExecutorService.submit(
            new Consumer(consumer.getInstanceId(), consumer, oracle, consumerControl, numGroups,
                         listeningExecutorService,
                         testConfig, testController,
                         queue, config, groupMap.get(consumer.getInstanceId())));
          consumerFutures.add(future);
        }

        // Wait for all consumers to complete
        final Future<?> compositeConsumerFuture = Futures.allAsList(consumerFutures);
        compositeConsumerFuture.get();

        Iterables.addAll(groupDequeueList, Iterables.concat(groupMap.values()));

        LOG.info(getLogMessage("Run done. Waiting for others to finish running"));
        groupControl.getRunBarrier().await();
        QueueInfo queueInfo = queue.getQueueInfo();
        LOG.info(getLogMessage(String.format("All run done. QueueInfo=%s", queueInfo.getJSONString())));

//        for(int i = 0; i < numConsumers; ++i) {
//          LOG.info(getLogMessage("Consumer:" + i + " dequeueList=" + groupMap.get(i)));
//        }

        if(consumerControl.getConsumersAtQueueEnd().size() == numConsumers) {
          break;
        }
      }


      // Remove self from barriers
      LOG.info(getLogMessage(
        String.format("To remove self. Waiting for others to finish configuring, current size=%d",
                      groupControl.getConfigBarrier().getParties())));
      groupControl.getConfigBarrier().await();
      // There should be no group waiting on config barrier now!
      groupControl.reduceConfigBarrier();
      LOG.info(getLogMessage(
        String.format("Done removing self from config barrier, new barrier size =%d, ",
                      groupControl.getConfigBarrier().getParties())));

      LOG.info(getLogMessage(
        String.format("To remove self. Waiting for others to finish running, current size=%d",
                      groupControl.getRunBarrier().getParties())));
      groupControl.getRunBarrier().await();
      // There should be no group waiting on run barrier now!
      groupControl.reduceRunBarrier();
      LOG.info(getLogMessage(
        String.format("Done removing self from run barrier, new size =%d",
                      groupControl.getRunBarrier().getParties())));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogMessage(String message) {
    return String.format("Group:%d, %s", id, message);
  }
}
