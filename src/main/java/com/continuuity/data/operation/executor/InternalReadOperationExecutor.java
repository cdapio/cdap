package com.continuuity.data.operation.executor;

import com.continuuity.api.data.SyncReadTimeoutException;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;

public interface InternalReadOperationExecutor {
  
  // TTQueues

  public DequeueResult execute(QueueDequeue dequeue)
      throws SyncReadTimeoutException;

  public long execute(GetGroupID getGroupId)
      throws SyncReadTimeoutException;
}
