package com.continuuity.api.dataset.lib;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.dataset.Dataset;

/**
 * This data set allows to store objects of a particular class into a table. The types that are supported are:
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 * Interfaces and not-static inner classes are not supported.
 * @param <T> the type of objects in the store
 */
@Beta
public interface ObjectStore<T> extends Dataset, BatchReadable<byte[], T>, RowScannable<KeyValue<byte[], T>> {

  /**
   * Write an object with a given key.
   *
   * @param key the key of the object
   * @param object the object to be stored
   */
  void write(String key, T object) throws Exception;

  /**
   * Write an object with a given key.
   *
   * @param key the key of the object
   * @param object the object to be stored
   */
  void write(byte[] key, T object) throws Exception;

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(String key) throws Exception;

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(byte[] key) throws Exception;
}
