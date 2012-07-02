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
   */
  public byte[] execute(ReadKey read);

  /**
   * Executes a {@link Read} operation.
   * @param read
   * @return map of columns to values, empty map if none found
   */
  public Map<byte[], byte[]> execute(Read read);

  /**
   * Executes a {@link ReadAllKeys} operation.
   * @param readKeys
   * @return list of keys, empty list if none found
   */
  public List<byte[]> execute(ReadAllKeys readKeys);

  /**
   * Executes a {@link ReadColumnRange} operation.
   * @param readColumnRange
   * @return map of columns to values
   */
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange);
}
