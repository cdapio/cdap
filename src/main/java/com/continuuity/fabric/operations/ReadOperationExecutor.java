package com.continuuity.fabric.operations;

import java.util.Map;

import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadCounter;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePop;

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
