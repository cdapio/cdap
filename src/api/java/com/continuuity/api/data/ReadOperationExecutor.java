package com.continuuity.api.data;

import java.util.List;
import java.util.Map;


/**
 * Executes a {@link ReadOperation}.
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
   * Executes a {@link Read} operation.
   * @param read
   * @return map of columns to values, empty map if none found
   * @throws SyncReadTimeoutException
   */
  public Map<byte[], byte[]> execute(Read read)
      throws SyncReadTimeoutException;

  /**
   * Executes a {@link ReadAllKeys} operation.
   * @param readKeys
   * @return list of keys, empty list if none found
   * @throws SyncReadTimeoutException
   */
  public List<byte[]> execute(ReadAllKeys readKeys)
      throws SyncReadTimeoutException;

  /**
   * Executes a {@link ReadColumnRange} operation.
   * @param readColumnRange
   * @return map of columns to values
   * @throws SyncReadTimeoutException
   */
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange)
      throws SyncReadTimeoutException;
}
