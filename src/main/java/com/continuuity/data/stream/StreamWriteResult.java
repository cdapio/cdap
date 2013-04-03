package com.continuuity.data.stream;

import com.continuuity.data.operation.ttqueue.EnqueueResult;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;

/**
 *
 */
public class StreamWriteResult extends EnqueueResult {

  public StreamWriteResult(EnqueueStatus status, QueueEntryPointer entryPointer) {
    super(status, entryPointer);
  }

  public static StreamWriteResult fromEnqueueResult(EnqueueResult result){
    return new  StreamWriteResult(result.getStatus(),result.getEntryPointer());
  }

}
