package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueMeta;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.operation.ttqueue.QueueDequeue;

public interface InternalOperationExecutor {

  // TTQueues

  public DequeueResult execute(QueueDequeue dequeue)
      throws OperationException;

  public long execute(GetGroupID getGroupId)
      throws OperationException;

  public OperationResult<QueueMeta> execute(GetQueueMeta getQueueMeta)
      throws OperationException;

  // Fabric Administration

  public void execute(ClearFabric clearFabric)
      throws OperationException;
}
