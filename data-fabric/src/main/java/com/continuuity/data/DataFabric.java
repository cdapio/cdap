package com.continuuity.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.weave.filesystem.Location;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * This is the abstract base class for data fabric.
 */
public interface DataFabric {

  // These are to support new TxDs2 system. DataFabric will go away once we fully migrate to it.
  <T> T getDataSetClient(String name, Class<? extends T> type);

  <T> DataSetManager getDataSetManager(Class<? extends T> type);

  /**
   * Executes a {@link com.continuuity.data.operation.Read} operation.
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
   * Performs a {@link com.continuuity.data.operation.CompareAndSwap} operation.
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


  /**
   * Opens a table in the data fabric. The only effect of this is that if the
   * table does not exist yet, it gets created at this time.
   * @param name the name of the table
   */
  public void openTable(String name) throws OperationException;

  // Provides access to filesystem

  /**
   * @param path The path representing the location.
   * @return a {@link Location} object to access given path.
   * @throws java.io.IOException
   */
  Location getLocation(String path) throws IOException;

  /**
   * @param path The path representing the location.
   * @return a {@link Location} object to access given path.
   * @throws IOException
   */
  Location getLocation(URI path) throws IOException;
}
