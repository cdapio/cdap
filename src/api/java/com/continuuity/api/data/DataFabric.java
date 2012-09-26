package com.continuuity.api.data;

import java.util.List;
import java.util.Map;

public interface DataFabric {

  /**
   * Executes a {@link com.continuuity.api.data.ReadKey} operation.
   * @param read the operation
   * @return a result object containing the value that was stored for
   *      the requested key. If the key is not found, the result will
   *      be empty and the status will be KEY_NOT_FOUND. If the key is
   *      found but does not have the key column, then the result is empty
   *      and the status is COLUMN_NOT_FOUND.
   * @throws com.continuuity.api.data.OperationException is something goes wrong
   */
  public OperationResult<byte[]> read(ReadKey read)
      throws OperationException;

  /**
   * Executes a {@link com.continuuity.api.data.Read} operation.
   * @param read the operation
   * @return a result object containing a map of columns to values if the key
   *    is found. If the key is not found, the result will be empty and the
   *    status code is KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> read(Read read)
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
  read(ReadColumnRange readColumnRange) throws OperationException;

  /**
   * Performs a {@link Write} operation.
   * @param write the operation
   * @throws OperationException if execution failed
   */
  public void execute(Write write) throws OperationException;

  /**
   * Performs a {@link Delete} operation.
   * @param delete the operation
   * @throws OperationException if execution failed
   */
  public void execute(Delete delete) throws OperationException;

  /**
   * Performs an {@link Increment} operation.
   * @param inc the operation
   * @throws OperationException if execution failed
   */
  public void execute(Increment inc) throws OperationException;

  /**
   * Performs a {@link CompareAndSwap} operation.
   * @param cas the operation
   * @throws OperationException if execution failed
   */
  public void execute(CompareAndSwap cas) throws OperationException;


  /**
   * Performs the specified writes synchronously and transactionally (the batch
   * appears atomic to readers and the entire batch either completely succeeds
   * or completely fails).
   *
   * Operations may be re-ordered and retriable operations may be automatically
   * retried.
   *
   * If the batch cannot be completed successfully, an exception is thrown.
   * In this case, the operations should be re-generated rather than just
   * re-submitted as retriable operations will already have been retried.
   *
   *
   * @param writes write operations to be performed in a transaction
   * @throws OperationException
   */
  public void execute(List<WriteOperation> writes)
      throws OperationException;

}
