package com.continuuity.api.data;

import java.util.List;


/**
 * Defines the execution of all supported {@link ReadOperation} types.
 */
public interface ReadOperationExecutor {

  public byte[] execute(Read read) throws SyncReadTimeoutException;

  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException;

  public List<byte[]> execute(ReadKeys readKeys)
      throws SyncReadTimeoutException;
}
