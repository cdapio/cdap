package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.ReadOperation;
import com.continuuity.api.data.WriteOperation;
import com.google.common.base.Objects;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue implements WriteOperation, ReadOperation {

  /** info about the producer */
  private final QueueProducer producer;

  /** Unique id for the operation */
  private final long id;
  private final byte [] queueName;
  private byte [] data;

  public QueueEnqueue(final byte [] queueName, final byte [] data) {
    this(OperationBase.getId(), queueName, data);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName,
                      final byte[] data) {
    this(OperationBase.getId(), producer, queueName, data);
  }

  public QueueEnqueue(final long id,
                      final byte[] queueName, final byte [] data) {
    this(id, null, queueName, data);
  }

  public QueueEnqueue(final long id, QueueProducer producer,
                      final byte[] queueName, final byte [] data) {
    this.id = id;
    this.producer = producer;
    this.queueName = queueName;
    this.data = data;
  }

  public byte [] getData() {
    return this.data;
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
        .add("data.length", this.data != null ? this.data.length : 0)
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

  /**
   * Sets the data of this enqueue operation to the specified bytes.
   * @param data bytes to set as data payload of this enqueue
   */
  public void setData(byte[] data) {
    this.data = data;
  }
}
