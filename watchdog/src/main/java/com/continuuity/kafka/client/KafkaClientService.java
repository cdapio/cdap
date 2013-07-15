/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.google.common.util.concurrent.Service;

/**
 * Represents a kafka client that can publish/subscribe to a kafka server cluster.
 */
public interface KafkaClientService extends Service {

  /**
   * Creates a {@link KafkaPublisher} that is ready for publish.
   * @param ack Type of ack that the publisher would use for all it's publish.
   * @return A {@link KafkaPublisher}.
   */
  KafkaPublisher getPublisher(KafkaPublisher.Ack ack);

  /**
   * Creates a {@link KafkaConsumer} for consuming messages.
   * @return A {@link KafkaConsumer}.
   */
  KafkaConsumer getConsumer();
}
