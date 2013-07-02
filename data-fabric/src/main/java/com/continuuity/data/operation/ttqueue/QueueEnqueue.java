package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.WriteOperation;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue extends WriteOperation {

  //info about the producer.
  private final QueueProducer producer;

  private final byte [] queueName;
  private final QueueEntry [] entries;

  public QueueEnqueue(final byte [] queueName, final QueueEntry entry) {
    this(null, queueName, entry);
  }

  public QueueEnqueue(final byte [] queueName, final QueueEntry [] entries) {
    this(null, queueName, entries);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName, final QueueEntry entry) {
    this(producer, queueName, new QueueEntry[] { entry });
    Preconditions.checkArgument(entry != null);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName, final QueueEntry[] entries) {
    Preconditions.checkArgument(entries != null && entries.length > 0);
    this.producer = producer;
    this.queueName = queueName;
    this.entries = entries;
  }

  public QueueEnqueue(final long id, QueueProducer producer, final byte[] queueName, final QueueEntry[] entries) {
    super(id);
    Preconditions.checkArgument(entries != null && entries.length > 0);
    this.producer = producer;
    this.queueName = queueName;
    this.entries = entries;
  }

  public QueueEntry[] getEntries() {
    return this.entries;
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
        .add("entries", Arrays.toString(this.entries))
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
