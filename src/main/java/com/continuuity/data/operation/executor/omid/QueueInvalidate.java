package com.continuuity.data.operation.executor.omid;

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
      ImmutablePair<ReadPointer,Long> txPointer);

  public static class QueueUnenqueue extends QueueInvalidate {
    public QueueUnenqueue(final byte[] queueName,
        QueueEntryPointer entryPointer) {
      super(queueName, entryPointer);
    }
    @Override
    public void execute(TTQueueTable queueTable,
        ImmutablePair<ReadPointer,Long> txPointer) {
      queueTable.invalidate(queueName, entryPointer, txPointer.getSecond());
    }
  }

  public static class QueueFinalize extends QueueInvalidate {
    private final QueueConsumer consumer;
    public QueueFinalize(final byte[] queueName, QueueEntryPointer entryPointer,
        QueueConsumer consumer) {
      super(queueName, entryPointer);
      this.consumer = consumer;
    }
    @Override
    public void execute(TTQueueTable queueTable,
        ImmutablePair<ReadPointer,Long> txPointer) {
      queueTable.finalize(queueName, entryPointer, consumer);
    }
  }

  public static class QueueUnack extends QueueInvalidate {
    private final QueueConsumer consumer;
    public QueueUnack(final byte[] queueName, QueueEntryPointer entryPointer,
        QueueConsumer consumer) {
      super(queueName, entryPointer);
      this.consumer = consumer;
    }
    @Override
    public void execute(TTQueueTable queueTable,
        ImmutablePair<ReadPointer,Long> txPointer) {
      queueTable.unack(queueName, entryPointer, consumer);
    }
  }
}
