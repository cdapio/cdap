package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.DequeueResult.DequeueStatus;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.operation.ttqueue.internal.ExecutionMode;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.continuuity.hbase.ttqueue.HBQConfig;
import com.continuuity.hbase.ttqueue.HBQConsumer;
import com.continuuity.hbase.ttqueue.HBQDequeueResult;
import com.continuuity.hbase.ttqueue.HBQDequeueResult.HBQDequeueStatus;
import com.continuuity.hbase.ttqueue.HBQEntryPointer;
import com.continuuity.hbase.ttqueue.HBQPartitioner.HBQPartitionerType;
import com.continuuity.hbase.ttqueue.HBQQueueMeta;

/**
 * Converts between internal TTQueue classes and HBQ classes.
 */
public class QueueToHBQ {


  public static ExecutionMode fromHBQ(
      com.continuuity.hbase.ttqueue.internal.ExecutionMode mode) {
    if (mode ==
        com.continuuity.hbase.ttqueue.internal.ExecutionMode.SINGLE_ENTRY) {
      return ExecutionMode.SINGLE_ENTRY;
    }
    return ExecutionMode.MULTI_ENTRY;
  }
    
  public static HBQPartitionerType toHBQ(PartitionerType type) {
    switch (type) {
      case RANDOM: return HBQPartitionerType.RANDOM;
      case HASH_ON_VALUE: return HBQPartitionerType.HASH_ON_VALUE;
      case MODULO_LONG_VALUE: return HBQPartitionerType.MODULO_LONG_VALUE;
      default: return HBQPartitionerType.RANDOM;
    }
  }

  public static DequeueResult fromHBQ(final byte [] queueName,
      final HBQDequeueResult dequeueResult) {
    if (dequeueResult.getStatus() == HBQDequeueStatus.EMPTY) {
      return new DequeueResult(DequeueStatus.EMPTY);
    } else if (dequeueResult.getStatus() == HBQDequeueStatus.SUCCESS) {
      return new DequeueResult(DequeueStatus.SUCCESS, 
          new QueueEntryPointer(queueName,
              dequeueResult.getEntryPointer().getEntryId(),
              dequeueResult.getEntryPointer().getShardId()),
              dequeueResult.getData());
    } else {
      throw new RuntimeException("Invalid state: " + dequeueResult.toString());
    }
  }

  public static QueueMeta fromHBQ(HBQQueueMeta queueMeta) {
    return new QueueMeta(queueMeta.getGlobalHeadPointer(),
        queueMeta.getCurrentWritePointer(),
        convertGroupArray(queueMeta.getGroups()));
  }

  private static GroupState[] convertGroupArray(
      com.continuuity.hbase.ttqueue.internal.GroupState[] groups) {
    GroupState [] convertedGroups = new GroupState[groups.length];
    for (int i=0; i<groups.length; i++) {
      convertedGroups[i] = new GroupState(groups[i].getGroupSize(),
          new EntryPointer(groups[i].getHead().getEntryId(),
              groups[i].getHead().getShardId()),
          QueueToHBQ.fromHBQ(groups[i].getMode()));
    }
    return convertedGroups;
  }

  public static HBQConfig toHBQ(QueueConfig config) {
    return new HBQConfig(toHBQ(config.getPartitionerType()),
        config.isSingleEntry());
  }

  public static HBQEntryPointer toHBQ(QueueEntryPointer pointer) {
    return new HBQEntryPointer(pointer.getEntryId(), pointer.getShardId());
  }

  public static HBQConsumer toHBQ(QueueConsumer consumer) {
    return new HBQConsumer(consumer.getInstanceId(), consumer.getGroupId(),
        consumer.getGroupSize());
  }
}
