package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.WriteOperation;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue extends WriteOperation {

  /** info about the producer */
  private final QueueProducer producer;

  private final byte [] queueName;
  private final QueueEntry entry;

  public QueueEnqueue(final byte [] queueName, final QueueEntry entry) {
    this(null, queueName, entry);
  }

  public QueueEnqueue(final long id, final byte[] queueName, final QueueEntry entry) {
    this(id, null, queueName, entry);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName, final QueueEntry entry) {
    this.producer = producer;
    this.queueName = queueName;
    this.entry = entry;
  }

  public QueueEnqueue(final long id, QueueProducer producer, final byte[] queueName, final QueueEntry entry) {
    super(id);
    Preconditions.checkArgument(entry!=null);
    this.producer = producer;
    this.queueName = queueName;
    this.entry = entry;
  }

  /**
   * @deprecated
   */
  public QueueEnqueue(final byte [] queueName, byte[] data) {
    this(queueName, new QueueEntryImpl(data));
  }

  public QueueEntry getEntry() {
    return this.entry;
  }

  /**
   * @deprecated
   */
  public byte[] getData() {
    return this.entry == null ? null : this.entry.getData();
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
  public int getSize() {
    return 0;
  }
}
