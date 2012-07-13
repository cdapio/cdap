package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.ReadOperation;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.google.common.base.Objects;

public class QueueAdmin {

  /**
   * Generates and returns a unique group id for the speicified queue.
   */
  public static class GetGroupID implements ReadOperation {

    private final byte [] queueName;

    public GetGroupID(final byte [] queueName) {
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

  public static class GetQueueMeta implements ReadOperation {

    private final byte [] queueName;

    public GetQueueMeta(byte [] queueName) {
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

  public static class QueueMeta {
    long globalHeadPointer;
    long currentWritePointer;
    GroupState [] groups;

    public long getGlobalHeadPointer() {
      return this.globalHeadPointer;
    }

    public long getCurrentWritePointer() {
      return this.currentWritePointer;
    }

    public GroupState [] getGroups() {
      return this.groups;
    }

    public QueueMeta() { }

    public QueueMeta(long globalHeadPointer, long currentWritePointer,
                     GroupState[] groups) {
      this.globalHeadPointer = globalHeadPointer;
      this.currentWritePointer = globalHeadPointer;
      this.groups = groups;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("globalHeadPointer", this.globalHeadPointer)
          .add("currentWritePointer", this.currentWritePointer)
          .add("groups", this.groups)
          .toString();
    }
  }
}
