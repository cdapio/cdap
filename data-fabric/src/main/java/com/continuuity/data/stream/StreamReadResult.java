package com.continuuity.data.stream;

import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueueState;

/**
 * Result from a stream read operation. It is extending from DequeueResult since Streams implementation are performed
 * using streams. This should be genericized when other implementations of streams are used.
 */
public class StreamReadResult extends DequeueResult {

  public StreamReadResult(DequeueStatus status) {
    super(status);
  }

  public StreamReadResult(DequeueStatus status, QueueEntryPointer pointer, QueueEntry entry) {
    super(status, pointer, entry);
  }

  public StreamReadResult(DequeueStatus status, QueueEntryPointer pointer, QueueEntry entry, QueueState queueState) {
    super(status, pointer, entry, queueState);
  }

  public static StreamReadResult fromDequeueResult(DequeueResult result){
    return new StreamReadResult(result.getStatus(), result.getEntryPointer(), result.getEntry());
  }

}
