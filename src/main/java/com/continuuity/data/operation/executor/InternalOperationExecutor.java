package com.continuuity.data.operation.executor;

import com.continuuity.api.data.SyncReadTimeoutException;
import com.continuuity.data.operation.FormatFabric;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueMeta;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.operation.ttqueue.QueueDequeue;

public interface InternalOperationExecutor {
  
  // TTQueues

  public DequeueResult execute(QueueDequeue dequeue)
      throws SyncReadTimeoutException;

  public long execute(GetGroupID getGroupId)
      throws SyncReadTimeoutException;

  public QueueMeta execute(GetQueueMeta getQueueMeta)
      throws SyncReadTimeoutException;

  // Fabric Administration

  public void execute(FormatFabric formatFabric);
}
