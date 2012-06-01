package com.continuuity.data.operation.executor;

import java.util.Map;

import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.operation.OrderedRead;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadCounter;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.type.ReadOperation;

/**
 * Defines the execution of all supported {@link ReadOperation} types.
 */
public interface ReadOperationExecutor {

  public byte[] execute(Read read) throws SyncReadTimeoutException;

  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException;

  public Map<byte[], byte[]> execute(OrderedRead orderedRead)
      throws SyncReadTimeoutException;
  
  // TTQueues

  public DequeueResult execute(QueueDequeue dequeue)
      throws SyncReadTimeoutException;

  public long execute(GetGroupID getGroupId)
      throws SyncReadTimeoutException;
}
