package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public final class QueueInputDatum implements InputDatum {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final QueueConsumer consumer;
  private final DequeueResult dequeueResult;
  private final QueueName queueName;
  private final int numGroups;
  private final AtomicInteger retry;

  public QueueInputDatum(QueueConsumer consumer, QueueName queueName, DequeueResult dequeueResult, int numGroups) {
    this.consumer = consumer;
    this.dequeueResult = dequeueResult;
    this.queueName = queueName;
    this.numGroups = numGroups;
    this.retry = new AtomicInteger(0);
  }

  @Override
  public void submitAck(TransactionAgent txAgent) throws OperationException {
    QueueAck ack;
    if (numGroups > 0) {
      ack = new QueueAck(queueName.toBytes(), dequeueResult.getEntryPointers(), consumer, numGroups);
    } else {
      ack = new QueueAck(queueName.toBytes(), dequeueResult.getEntryPointers(), consumer);
    }
    txAgent.submit(ack);
  }

  @Override
  public boolean needProcess() {
    return !dequeueResult.isEmpty();
  }

  @Override
  public Iterator<ByteBuffer> getData() {
    if (!needProcess()) {
      return Iterators.singletonIterator(EMPTY_BUFFER);
    }

    if (dequeueResult.getEntries().length == 1) {
      return Iterators.singletonIterator(ByteBuffer.wrap(dequeueResult.getEntries()[0].getData()));
    }

    return Iterators.transform(Iterators.forArray(dequeueResult.getEntries()),
                               new Function<QueueEntry, ByteBuffer>() {
                                 @Nullable
                                 @Override
                                 public ByteBuffer apply(@Nullable QueueEntry input) {
                                   return input == null ? EMPTY_BUFFER : ByteBuffer.wrap(input.getData());
                                 }
                               }
    );
  }

  @Override
  public void incrementRetry() {
    retry.incrementAndGet();
  }

  @Override
  public int getRetry() {
    return retry.get();
  }

  @Override
  public InputContext getInputContext() {
    final String name = queueName.getSimpleName();

    return new InputContext() {
      @Override
      public String getOrigin() {
        return name;
      }

      @Override
      public int getRetryCount() {
        return retry.get();
      }
    };
  }

  @Override
  public String toString() {
    if (!dequeueResult.isEmpty()) {
      return String.format("%s, %s, %d, %d",
                           queueName, dequeueResult.getEntryPointer().getEntryId(), numGroups, retry.get());
    } else {
      return String.format("%s, empty, %d, %d", queueName, numGroups, retry.get());
    }
  }
}
