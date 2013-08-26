package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.google.inject.Inject;

import java.io.IOException;

/**
 *
 */
public class InMemoryQueueClientFactory implements QueueClientFactory {

  private final InMemoryQueueService queueService;

  @Inject
  public InMemoryQueueClientFactory(InMemoryQueueService queueService) {
    this.queueService = queueService;
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    return new InMemoryQueue2Consumer(queueName, consumerConfig, numGroups, queueService);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    return new InMemoryQueue2Producer(queueName, queueService, queueMetrics);
  }
}
