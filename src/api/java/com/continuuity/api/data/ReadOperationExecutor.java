package com.continuuity.api.data;

import java.util.List;
import java.util.Map;


/**
 * Executes a {@link ReadOperation}.
 */
public interface ReadOperationExecutor {

  /**
   * Executes a {@link ReadKey} operation.
   * @param read the operation
   * @return a result object containing the value that was stored for
   *      the requested key. If the key is not found, the result will
   *      be empty and the status will be KEY_NOT_FOUND. If the key is
   *      found but does not have the key column, then the result is empty
   *      and the status is COLUMN_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<byte[]> execute(ReadKey read)
      throws OperationException;

  /**
   * Executes a {@link Read} operation.
   * @param read the operation
   * @return a result object containing a map of columns to values if the key
   *    is found. If the key is not found, the result will be empty and the
   *    status code is KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(Read read)
      throws OperationException;

  /**
   * Executes a {@link ReadAllKeys} operation.
   * @param readKeys the operation
   * @return a result object containing a list of keys if none found. If no
   * keys are found, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<byte[]>> execute(ReadAllKeys readKeys)
      throws OperationException;

  /**
   * Executes a {@link ReadColumnRange} operation.
   * @param readColumnRange the operation
   * @return a result object containing a map of columns to values. If the
   * key is not found, the result will be empty and the status code is
   * KEY_NOT_FOUND. If the key exists but there are no columns the given range,
   * then the result is empty with status code COLUMN_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>>
  execute(ReadColumnRange readColumnRange) throws OperationException;
}
