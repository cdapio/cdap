package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;

import java.io.IOException;

/**
 *
 */
public class InMemoryQueueClientFactory implements QueueClientFactory {

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return new InMemoryQueue2Producer(queueName);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig) throws IOException {
    return new InMemoryQueue2Consumer(queueName, consumerConfig);
  }
}
