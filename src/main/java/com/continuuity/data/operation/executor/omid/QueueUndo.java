package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class QueueUndo implements Undo {

  protected final byte [] queueName;
  protected final QueueEntryPointer entryPointer;

  protected QueueUndo(final byte[] queueName, final QueueEntryPointer entryPointer) {
    this.queueName = queueName;
    this.entryPointer = entryPointer;
  }

  @Override
  public byte[] getRowKey() {
    // queue operations are excluded from conflict detection
    return null;
  }

  public byte[] getQueueName() {
    return queueName;
  }

  public QueueEntryPointer getEntryPointer() {
    return entryPointer;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryPointer", this.entryPointer)
        .toString();
  }
  
  public abstract void execute(TTQueueTable queueTable, Transaction transaction)
    throws OperationException;

  public static class QueueUnenqueue extends QueueUndo {
    final byte[] data;
    final QueueProducer producer;

    public QueueUnenqueue(final byte[] queueName,
                          final byte[] data,
                          QueueProducer producer,
                          QueueEntryPointer entryPointer) {
      super(queueName, entryPointer);
      this.producer = producer;
      this.data = data;
    }

    @Override
    public void execute(TTQueueTable queueTable, Transaction transaction) throws OperationException {
      queueTable.invalidate(queueName, entryPointer, transaction.getTransactionId());
    }
  }

  public static class QueueUnack extends QueueUndo {
    final QueueConsumer consumer;
    final int numGroups;

    public QueueConsumer getConsumer() {
      return consumer;
    }

    public int getNumGroups() {
      return numGroups;
    }

    public QueueUnack(final byte[] queueName, QueueEntryPointer entryPointer,
        QueueConsumer consumer, int numGroups) {
      super(queueName, entryPointer);
      this.consumer = consumer;
      this.numGroups = numGroups;
    }

    @Override
    public void execute(TTQueueTable queueTable, Transaction transaction) throws OperationException {
      queueTable.unack(queueName, entryPointer, consumer, transaction.getReadPointer());
    }
  }
}
