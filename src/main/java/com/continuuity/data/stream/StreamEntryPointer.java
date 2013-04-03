package com.continuuity.data.stream;

import com.continuuity.data.operation.ttqueue.QueueEntryPointer;

/**
 * This is implemented based on QueueEntryPointer since the
 * implementation of Streams is done using queues. To genericize later.
 */
public class StreamEntryPointer extends QueueEntryPointer {

  public StreamEntryPointer(byte[] queueName, long entryId, long shardId) {
    super(queueName, entryId, shardId);
  }

  public StreamEntryPointer(byte[] queueName, long entryId) {
    super(queueName, entryId);
  }

  public static StreamEntryPointer fromQueueEntryPointer( QueueEntryPointer queueEntryPointer) {
    return new StreamEntryPointer(queueEntryPointer.getQueueName(),
                                   queueEntryPointer.getEntryId(),queueEntryPointer.getShardId());
  }
}
