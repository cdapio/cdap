/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueMetrics;

import java.io.IOException;

/**
 * Factory for creating {@link Queue2Producer} and {@link Queue2Consumer} for different queue.
 */
public interface QueueClientFactory {

  Queue2Producer createProducer(QueueName queueName) throws IOException;

  Queue2Consumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups) throws IOException;

  Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException;
}
