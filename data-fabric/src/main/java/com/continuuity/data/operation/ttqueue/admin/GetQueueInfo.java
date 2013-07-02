package com.continuuity.data.operation.ttqueue.admin;

import com.continuuity.data.operation.ReadOperation;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
*
*/
public class GetQueueInfo extends ReadOperation {

  //Unique id for the operation.
  private final byte [] queueName;

  public GetQueueInfo(byte[] queueName) {
    this.queueName = queueName;
  }

  public GetQueueInfo(final long id, byte[] queueName) {
    super(id);
    this.queueName = queueName;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queuename", Bytes.toString(this.queueName))
        .toString();
  }
}
