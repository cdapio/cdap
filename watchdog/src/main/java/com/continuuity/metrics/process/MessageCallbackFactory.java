/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.process;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.kafka.client.KafkaConsumer;

/**
 * Factory to create MessageCallback for the Metrics Processing Service. This factory interface
 * exists for simplifying object injections by guice only.
 */
public interface MessageCallbackFactory {

  KafkaConsumer.MessageCallback create(KafkaConsumerMetaTable metaTable, MetricsScope scope);
}
