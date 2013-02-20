package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.OperationBase;
import com.continuuity.data.operation.ReadOperation;
import com.continuuity.data.operation.WriteOperation;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue implements WriteOperation, ReadOperation {

  /** info about the producer */
  private final QueueProducer producer;

  /** Unique id for the operation */
  private final long id;
  private final byte [] queueName;
  private final QueueEntry entry;

  public QueueEnqueue(final byte [] queueName, final QueueEntry entry) {
    this(OperationBase.getId(), queueName, entry);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName, final QueueEntry entry) {
    this(OperationBase.getId(), producer, queueName, entry);
  }

  public QueueEnqueue(final long id, final byte[] queueName, final QueueEntry entry) {
    this(id, null, queueName, entry);
  }

  public QueueEnqueue(final long id, QueueProducer producer, final byte[] queueName, final QueueEntry entry) {
    this.id = id;
    this.producer = producer;
    this.queueName = queueName;
    this.entry = entry;
  }

  /**
   * @deprecated
   */
  public QueueEnqueue(final byte [] queueName, byte[] data) {
    this(OperationBase.getId(), queueName, new QueueEntryImpl(data));
  }

  public QueueEntry getEntry() {
    return this.entry;
  }

  /**
   * @deprecated
  */
  public byte[] getData() {
    return this.entry.getData();
  }

  /**
   * @deprecated
   */
  public void setData(byte[] data) {
    this.entry.setData(data);
  }

  public QueueProducer getProducer() {
    return this.producer;
  }

  @Override
  public byte[] getKey() {
    return this.queueName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryh", this.entry)
        .toString();
  }

  @Override
  public int getPriority() {
    return 2;
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public int getSize() {
    return 0;
  }
}
