package com.continuuity.data.operation.executor;

import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueMeta;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.operation.ttqueue.QueueDequeue;

public interface InternalOperationExecutor {

  // TTQueues

  public DequeueResult execute(QueueDequeue dequeue);

  public long execute(GetGroupID getGroupId);

  public QueueMeta execute(GetQueueMeta getQueueMeta);

  // Fabric Administration

  public void execute(ClearFabric clearFabric);
}
