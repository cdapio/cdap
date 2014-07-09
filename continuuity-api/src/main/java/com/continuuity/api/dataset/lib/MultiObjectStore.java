package com.continuuity.api.dataset.lib;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.dataset.Dataset;

import java.util.List;
import java.util.Map;

/**
 * A dataset that stores objects of a particular class into a table; multiple objects can be stored using different
 * column names for each object.
 *
 * <p>
 * Supported types are:
 * </p>
 * <p>
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 * </p>
 * Interfaces and not-static inner classes are not supported.
 * If no column name is specified in read or write operations a default column 'c' will be used.
 *
 * @param <T> the type of objects in the store
 */
@Beta
public interface MultiObjectStore<T>
  extends Dataset, BatchReadable<byte[], Map<byte[], T>>,
  BatchWritable<byte[], Map<byte[], T>> {

  /**
   * Write an object with a given key. Writes the object to the default column 'c'
   * @param key the key of the object
   * @param object the object to be stored
   */
  void write(byte[] key, T object);

  /**
   * Write an object with a given key. Writes the object to the default column 'c'
   * @param key the key of the object
   * @param object the object to be stored
   */
  void write(String key, T object);

  /**
   * Write an object with a given key and a column.
   * @param key the key of the object.
   * @param col column where the object should be written.
   * @param object object to be stored.
   */
  void write(byte[] key, byte[] col, T object);

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(byte[] key);

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @param col to read
   * @return the object if found, or null if not found
   */
  T read(byte[] key, byte[] col);

  /**
   * Delete the object specified with specified key and column.
   * @param key key of the object to be deleted
   * @param col col of the object to be deleted
   */
  void delete(byte[] key, byte[] col);

  /**
   * Delete the object in the default column for the specified key.
   * @param key key of the object to be deleted in the default column
   */
  void delete(byte[] key);

  /**
   * Delete the objects across all the columns for the given key.
   * @param key key of the object to be deleted
   */
  void deleteAll(byte[] key);

  /**
   * Read all the objects with the given key.
   * @param key the key of the object
   * @return Map of column key and Object, null if entry for the key doesn't exist
   */
  Map<byte[], T> readAll(byte[] key);

  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link com.continuuity.api.data.batch.Split}
   */
  @Beta
  List<Split> getSplits(int numSplits, byte[] start, byte[] stop);
}
