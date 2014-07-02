/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueMetrics;

import java.io.IOException;

/**
 * Factory for creating {@link QueueProducer} and {@link QueueConsumer} for different queue.
 */
public interface QueueClientFactory {

  QueueProducer createProducer(QueueName queueName) throws IOException;

  QueueConsumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups) throws IOException;

  QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException;
}
