/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;

/**
 * This interface is for publishing data to Kafka.
 */
public interface KafkaPublisher {

  /**
   * A Preparer for preparing to publish messages to a given topic.
   */
  interface Preparer {
    /**
     * Adds the given message to the message set, partitioned with the given partition key.
     * @param message Remaining bytes in the ByteBuffer will be used as message payload. This method would
     *                consume the ByteBuffer, meaning after this method returns, the remaining bytes in the
     *                ByteBuffer would be {@code 0}.
     * @param partitionKey Key for computing the partition Id to publish to. The {@link Object#hashCode()} method
     *                     will be invoke to compute the id.
     * @return
     */
    Preparer add(ByteBuffer message, Object partitionKey);

    /**
     * Sends all the messages being added through the {@link #add} method.
     *
     * @return A {@link ListenableFuture} that will be completed when the send action is done. If publish is succeeded,
     *         it returns number of messages published, otherwise the failure reason will be carried in the future.
     *         The {@link ListenableFuture#cancel(boolean)} method has no effect on the publish action.
     */
    ListenableFuture<Integer> send();
  }

  /**
   * Represents the desired level of publish acknowledgment.
   */
  enum Ack {
    /**
     * Not wait for ack.
     */
    FIRE_AND_FORGET(0),

    /**
     * Waits for the leader received data.
     */
    LEADER_RECEIVED(1),

    /**
     * Waits for all replicas received data.
     */
    ALL_RECEIVED(-1);

    private final int ack;

    private Ack(int ack) {
      this.ack = ack;
    }

    /**
     * Returns the numerical ack number as understand by Kafka server.
     */
    public int getAck() {
      return ack;
    }
  }

  /**
   * Prepares to publish to a given topic.
   *
   * @param topic Name of the topic.
   * @return A {@link Preparer} to prepare for publishing.
   */
  Preparer prepare(String topic);
}
