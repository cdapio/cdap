package com.continuuity.data.operation.ttqueue.admin;

import com.continuuity.data.operation.ReadOperation;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Generates and returns a unique group id for the speicified queue.
 */
public class GetGroupID extends ReadOperation {

  private final byte [] queueName;

  public GetGroupID(final byte[] queueName) {
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
