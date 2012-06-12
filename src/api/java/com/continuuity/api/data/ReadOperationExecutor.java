package com.continuuity.api.data;


/**
 * Defines the execution of all supported {@link ReadOperation} types.
 */
public interface ReadOperationExecutor {

  public byte[] execute(Read read) throws SyncReadTimeoutException;

  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException;
}
