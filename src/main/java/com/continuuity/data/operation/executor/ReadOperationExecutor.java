package com.continuuity.data.operation.executor;

import java.util.Map;

import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.operation.OrderedRead;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadCounter;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePop;
import com.continuuity.data.operation.type.ReadOperation;

/**
 * Defines the execution of all supported {@link ReadOperation} types.
 */
public interface ReadOperationExecutor {

  public byte[] execute(Read read) throws SyncReadTimeoutException;

  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException;

  public Map<byte[], byte[]> execute(OrderedRead orderedRead) throws SyncReadTimeoutException;

  // Queues

  public QueueEntry execute(QueuePop pop)
  throws SyncReadTimeoutException, InterruptedException;

}
