/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.table.Delete;

import java.util.Iterator;
import java.util.Map;

/**
 * A dataset that stores objects of a particular class into a table.
 * <p>
 * Supported types are:
 * </p>
 * <p>
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 *</p>
 * Interfaces and not-static inner classes are not supported. An ObjectStore will serialize the entire object and
 * store it in a single column. See {@link ObjectMappedTable} if you want object fields to be mapped to their own
 * columns.
 *
 * @param <T> the type of objects in the store
 */
@Beta
public interface ObjectStore<T> extends Dataset, BatchReadable<byte[], T>, BatchWritable<byte[], T> {

  /**
   * Write an object with a given key.
   *
   * @param key the key of the object
   * @param object the object to be stored
   */
  void write(String key, T object);

  /**
   * Write an object with a given key.
   *
   * @param key the key of the object
   * @param object the object to be stored
   */
  void write(byte[] key, T object);

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(String key);

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(byte[] key);

  /**
   * Scans table.
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return {@link co.cask.cdap.api.dataset.lib.CloseableIterator} over
   * {@link KeyValue KeyValue&lt;byte[], T&gt;}
   */
  CloseableIterator<KeyValue<byte[], T>> scan(byte[] startRow, byte[] stopRow);

  /**
   * Delete the object for the specified key.
   * @param key key of the object to be deleted
   */
  void delete(byte[] key);
}
