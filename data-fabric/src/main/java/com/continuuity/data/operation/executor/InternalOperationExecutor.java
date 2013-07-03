package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.TruncateTable;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;

/**
 * An executor that can perform operations not exposed to developers.
 */
public interface InternalOperationExecutor {

  // TTQueues

  public DequeueResult execute(OperationContext context,
                               QueueDequeue dequeue)
      throws OperationException;

  public long execute(OperationContext context,
                      GetGroupID getGroupId)
      throws OperationException;

  public OperationResult<QueueInfo>
  execute(OperationContext context, GetQueueInfo getQueueInfo)
      throws OperationException;

  // Fabric Administration

  public void execute(OperationContext context,
                      ClearFabric clearFabric)
      throws OperationException;

  public void execute(OperationContext context,
                      OpenTable openTable)
      throws OperationException;

  public void execute(OperationContext context,
                      TruncateTable truncateTable)
    throws OperationException;
}
