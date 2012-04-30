package com.continuuity.fabric.operations;

import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.QueuePop;
import com.continuuity.fabric.operations.impl.Read;

/**
 * Defines the execution of all supported {@link ReadOperation} types.
 */
public interface ReadOperationExecutor {

  public byte[] execute(Read read) throws SyncReadTimeoutException;

  public String execute(QueuePop pop) throws SyncReadTimeoutException;

  public String execute(OrderedRead orderedRead) throws SyncReadTimeoutException;
  
}
