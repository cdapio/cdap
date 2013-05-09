package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueRunnable;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueStateProxy;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Represents a queue operation to be undone.
 */
public abstract class QueueUndo implements Undo {

  @Override
  public RowSet.Row getRow() {
    // queue operations are excluded from conflict detection
    return null;
  }

  protected final byte [] queueName;
  protected final QueueEntryPointer [] entryPointers;

  public byte[] getQueueName() {
    return queueName;
  }

  public QueueEntryPointer [] getEntryPointers() {
    return this.entryPointers;
  }

  protected QueueUndo(final byte[] queueName, final QueueEntryPointer [] entryPointers) {
    this.queueName = queueName;
    this.entryPointers = entryPointers;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryPointers", Arrays.toString(this.entryPointers))
        .toString();
  }
  /**
   * Perform the undo.
   * @param queueTable the queue table to perform the undo on
   * @param transaction the transaction in which the undo must happen
   * @throws OperationException
   */
  public abstract void execute(QueueStateProxy queueStateProxy, Transaction transaction, TTQueueTable queueTable)
    throws OperationException;

  /**
   * Represents the reverse of an Enqueue operation.
   */
  public static class QueueUnenqueue extends QueueUndo {
    final int sumOfSizes;
    final QueueProducer producer;

    public QueueUnenqueue(final byte[] queueName,
                          final QueueEntry[] entries,
                          QueueProducer producer,
                          QueueEntryPointer[] entryPointers) {
      super(queueName, entryPointers);
      this.producer = producer;
      int size = 0;
      for (QueueEntry entry : entries) {
        size += entry.getData().length;
      }
      this.sumOfSizes = size;
    }

    public int numEntries() {
      return this.entryPointers.length;
    }

    @Override
    public void execute(QueueStateProxy queueStateProxy, Transaction transaction, TTQueueTable queueTable)
      throws OperationException {
      // No need to use queueStateProxy since there is no queue state associated with invalidate
      queueTable.invalidate(queueName, entryPointers, transaction);
    }
  }

  /**
   * Represents the reverse of a Queue ack operation.
   */
  public static class QueueUnack extends QueueUndo {
    final QueueConsumer consumer;
    final int numGroups;

    public QueueConsumer getConsumer() {
      return consumer;
    }

    public int getNumGroups() {
      return numGroups;
    }

    public QueueUnack(final byte[] queueName, QueueEntryPointer [] entryPointers,
                      QueueConsumer consumer, int numGroups) {
      super(queueName, entryPointers);
      this.consumer = consumer;
      this.numGroups = numGroups;
    }

    @Override
    public void execute(QueueStateProxy queueStateProxy, final Transaction transaction, final TTQueueTable queueTable)
      throws OperationException {
      queueStateProxy.run(queueName, consumer,
                                         new QueueRunnable() {
                                           @Override
                                           public void run(StatefulQueueConsumer statefulQueueConsumer)
                                             throws OperationException {
                                             queueTable.unack(queueName, entryPointers, statefulQueueConsumer,
                                                              transaction);
                                           }
                                         });
    }
  }
}
