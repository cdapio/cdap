package com.continuuity.api.data;

import java.util.List;
import java.util.Map;


/**
 * Defines the execution of all supported {@link ReadOperation} types.
 */
public interface ReadOperationExecutor {

  /**
   * Executes a {@link ReadKey} operation.
   * @param read
   * @return value stored for specified for read, null if not found
   * @throws SyncReadTimeoutException
   */
  public byte[] execute(ReadKey read) throws SyncReadTimeoutException;

  /**
   * Executes a {@link ReadCounter} operation.
   * @param readCounter
   * @return value of counter, 0 if counter not found
   * @throws SyncReadTimeoutException
   */
  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException;

  /**
   * Executes a {@link ReadColumns} operation.
   * @param read
   * @return map of columns to values, empty map if none found
   * @throws SyncReadTimeoutException
   */
  public Map<byte[], byte[]> execute(ReadColumns read)
      throws SyncReadTimeoutException;
  
  /**
   * Executes a {@link ReadKeys} operation.
   * @param readKeys
   * @return list of keys, empty list if none found
   * @throws SyncReadTimeoutException
   */
  public List<byte[]> execute(ReadKeys readKeys)
      throws SyncReadTimeoutException;
}
