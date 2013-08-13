/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.continuuity.weave.common.Cancellable;

import java.util.Iterator;

/**
 *
 */
public interface KafkaConsumer {

  /**
   * Callback for receiving new messages.
   */
  interface MessageCallback {

    /**
     * Invoked when new messages is available.
     * @param messages Iterator of new messages. The {@link FetchedMessage} instance maybe reused in the Iterator
     *                 and across different invocation.
     */
    void onReceived(Iterator<FetchedMessage> messages);

    /**
     * Invoked when message consumption is stopped. When this method is invoked,
     * no more {@link #onReceived(java.util.Iterator)} will get triggered.
     */
    void finished();
  }

  /**
   * A builder preparing a message consumption.
   */
  interface Preparer {

    /**
     * Consume message from a given offset. If the given offset is invalid, it'll start consuming from the
     * latest offset.
     * @param topic Topic to consume from.
     * @param partition Partition in the topic to consume from.
     * @param offset Offset to starts with.
     * @return This {@link Preparer} instance.
     */
    Preparer add(String topic, int partition, long offset);

    /**
     * Consume message from the earliest message available.
     * @param topic Topic to consume from.
     * @param partition Partition in the topic to consume from.
     * @return This {@link Preparer} instance.
     */
    Preparer addFromBeginning(String topic, int partition);

    /**
     * Consume message from the latest message.
     * @param topic Topic to consume from.
     * @param partition Partition in the topic to consume from.
     * @return This {@link Preparer} instance.
     */
    Preparer addLatest(String topic, int partition);

    /**
     * Starts the consumption as being configured by this {@link Preparer}.
     * @param callback
     * @return A {@link Cancellable} for cancelling message consumption.
     */
    Cancellable consume(MessageCallback callback);
  }

  Preparer prepare();
}
