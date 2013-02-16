package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.OperationBase;
import com.continuuity.data.operation.ReadOperation;
import com.continuuity.data.operation.WriteOperation;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

import java.util.Map;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue implements WriteOperation, ReadOperation {

  /** info about the producer */
  private final QueueProducer producer;

  /** Unique id for the operation */
  private final long id;
  private final byte [] queueName;
  private final int headerVersion=0;
  private final Map<String, String> headers; // can be null or empty

  private byte [] data;

  public QueueEnqueue(final byte [] queueName, final byte [] data) {
    this(OperationBase.getId(), queueName, data);
  }

  public QueueEnqueue(final byte [] queueName, final String outputName, final Map<String, String> headers,
                      final byte [] data) {
    this(OperationBase.getId(), queueName, outputName, headers, data);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName, final byte[] data) {
    this(OperationBase.getId(), producer, queueName, data);
  }

  public QueueEnqueue(QueueProducer producer, final byte [] queueName, final Map<String, String> headers,
                      final byte[] data) {
    this(OperationBase.getId(), producer, queueName, headers, data);
  }

  public QueueEnqueue(final long id, final byte[] queueName, final byte [] data) {
    this(id, null, queueName, data);
  }

  public QueueEnqueue(final long id, final byte[] queueName, final String outputName,
                      final Map<String, String> headers, final byte [] data) {
    this(id, null, queueName, headers, data);
  }

  public QueueEnqueue(final long id, QueueProducer producer, final byte[] queueName, final byte [] data) {
    this(id, producer, queueName, null, data);
  }


  public QueueEnqueue(final long id, QueueProducer producer, final byte[] queueName, final Map<String, String> headers,
                      final byte [] data) {
    this.id = id;
    this.producer = producer;
    this.queueName = queueName;
    this.headers=headers;
    this.data = data;
  }

  public Map<String, String> getHeaders() {
    return this.headers;
  }

  public int getHeaderVersion() {
    return headerVersion;
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

  @Override
  public int getSize() {
    return 0;
  }
}
