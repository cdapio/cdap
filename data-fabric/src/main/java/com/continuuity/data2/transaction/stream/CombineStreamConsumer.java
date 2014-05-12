/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StreamConsumer} that combines two {@link StreamConsumer}.
 * It always try to consume from the first consumer until no more event coming from it, then it switch to the
 * second consumer permanently.
 *
 // This is just for compatibility upgrade from pre 2.2.0 to 2.2.0.
 // TODO: Remove usage of this when no longer needed.
 */
public final class CombineStreamConsumer implements StreamConsumer {

  private final StreamConsumer firstConsumer;
  private final StreamConsumer secondConsumer;
  private StreamConsumer activeConsumer;
  private boolean emptyResult;

  /**
   * Constructs a new instance. Both consumers provided should have the same stream name and consumer config.
   *
   * @param firstConsumer Consumer to consume from first.
   * @param secondConsumer Consumer to consume from when the first one is drained.
   *
   * @throws IllegalArgumentException if consumers don't have the same stream name or consumer config.
   */
  public CombineStreamConsumer(StreamConsumer firstConsumer, StreamConsumer secondConsumer) {
    Preconditions.checkArgument(firstConsumer != secondConsumer,
                                "First and second consumers cannot be the same instance");
    Preconditions.checkArgument(firstConsumer.getStreamName().equals(secondConsumer.getStreamName()),
                                "Stream not match between %s and %s", firstConsumer, secondConsumer);
    Preconditions.checkArgument(firstConsumer.getConsumerConfig().equals(secondConsumer.getConsumerConfig()),
                                "Consumer config not match between %s and %s", firstConsumer, secondConsumer);

    this.firstConsumer = firstConsumer;
    this.secondConsumer = secondConsumer;
    this.activeConsumer = firstConsumer;
  }

  @Override
  public QueueName getStreamName() {
    return activeConsumer.getStreamName();
  }

  @Override
  public ConsumerConfig getConsumerConfig() {
    return activeConsumer.getConsumerConfig();
  }

  @Override
  public DequeueResult<StreamEvent> poll(int maxEvents,
                                         long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException {

    DequeueResult<StreamEvent> result = activeConsumer.poll(maxEvents, timeout, timeoutUnit);
    if (activeConsumer == firstConsumer) {
      emptyResult = result.isEmpty();
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    if (activeConsumer == firstConsumer) {
      Closeables.closeQuietly(firstConsumer);
      secondConsumer.close();
    } else {
      activeConsumer.close();
    }
  }

  @Override
  public void startTx(Transaction tx) {
    activeConsumer.startTx(tx);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return activeConsumer.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    return activeConsumer.commitTx();
  }

  @Override
  public void postTxCommit() {
    activeConsumer.postTxCommit();

    // If the first consumer has empty dequeue result in the poll call,
    // it's ok to switch to second consumer permanently in post commit call.
    if (activeConsumer == firstConsumer && emptyResult) {
      activeConsumer = secondConsumer;
      Closeables.closeQuietly(firstConsumer);
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return activeConsumer.rollbackTx();
  }

  @Override
  public String getName() {
    return activeConsumer.getName();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("active", activeConsumer)
      .add("first", firstConsumer)
      .add("second", secondConsumer)
      .toString();
  }
}
