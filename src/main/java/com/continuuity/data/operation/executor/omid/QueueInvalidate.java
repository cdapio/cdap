package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.table.ReadPointer;
import com.google.common.base.Objects;

public abstract class QueueInvalidate {

  protected final byte [] queueName;
  protected final QueueEntryPointer entryPointer;

  protected QueueInvalidate(final byte [] queueName,
      final QueueEntryPointer entryPointer) {
    this.queueName = queueName;
    this.entryPointer = entryPointer;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryPointer", this.entryPointer)
        .toString();
  }
  
  public abstract void execute(TTQueueTable queueTable,
      ImmutablePair<ReadPointer,Long> txPointer) throws OperationException;

  public static class QueueUnenqueue extends QueueInvalidate {
    final QueueProducer producer;
    public QueueUnenqueue(final byte[] queueName,
                          QueueProducer producer,
                          QueueEntryPointer entryPointer) {
      super(queueName, entryPointer);
      this.producer = producer;
    }
    @Override
    public void execute(TTQueueTable queueTable,
        ImmutablePair<ReadPointer,Long> txPointer) throws OperationException {
      queueTable.invalidate(queueName, entryPointer, txPointer.getSecond());
    }
  }

  public static class QueueFinalize extends QueueInvalidate {
    private final QueueConsumer consumer;
    private final int totalNumGroups;
    public QueueFinalize(final byte[] queueName, QueueEntryPointer entryPointer,
        QueueConsumer consumer, int totalNumGroups) {
      super(queueName, entryPointer);
      this.consumer = consumer;
      this.totalNumGroups = totalNumGroups;
    }
    @Override
    public void execute(TTQueueTable queueTable,
        ImmutablePair<ReadPointer,Long> txPointer) throws OperationException {
      queueTable.finalize(queueName, entryPointer, consumer, totalNumGroups);
    }
  }

  public static class QueueUnack extends QueueInvalidate {
    final QueueConsumer consumer;
    public QueueUnack(final byte[] queueName, QueueEntryPointer entryPointer,
        QueueConsumer consumer) {
      super(queueName, entryPointer);
      this.consumer = consumer;
    }
    @Override
    public void execute(TTQueueTable queueTable,
        ImmutablePair<ReadPointer,Long> txPointer) throws OperationException {
      queueTable.unack(queueName, entryPointer, consumer);
    }
  }
}
