/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;

import javax.annotation.Nullable;

/**
 * A Dataset that stores plain java Objects into a table by mapping object fields to table columns.
 * Objects must be flat and can only contain fields of simple type. A simple type is an Integer,
 * int, Long, long, Float, float, Double, double, String, byte[], ByteBuffer, or UUID.
 * The object itself cannot be a simple type.
 *
 * This Dataset is {@link RecordScannable}, which means it can be explored through Hive. The Hive table
 * for this Dataset will contain one column for each object field and one column for the row key.
 * For example, if you are storing an Object of three fields ("id", "name", and "price"), the corresponding
 * Hive table will have four columns: "rowkey", "id", "name", and "price". If you wish to change the name of
 * the "rowkey" column you can do so by setting a property on the Dataset. See {@link ObjectMappedTableProperties}
 * for more information on properties for this Dataset.
 *
 * @param <T> the type of objects in the table
 */
@Beta
public interface ObjectMappedTable<T> extends Dataset, BatchReadable<byte[], T>,
  BatchWritable<byte[], T>, RecordScannable<StructuredRecord> {

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
   *
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(String key);

  /**
   * Read an object with a given key.
   *
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  T read(byte[] key);

  /**
   * Scans table.
   *
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return {@link CloseableIterator} over {@link KeyValue KeyValue&lt;byte[], T&gt;}
   */
  CloseableIterator<KeyValue<byte[], T>> scan(@Nullable String startRow, @Nullable String stopRow);

  /**
   * Scans table.
   *
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return {@link CloseableIterator} over {@link KeyValue KeyValue&lt;byte[], T&gt;}
   */
  CloseableIterator<KeyValue<byte[], T>> scan(@Nullable byte[] startRow, @Nullable byte[] stopRow);

  /**
   * Delete the object for the specified key.
   *
   * @param key key of the object to be deleted
   */
  void delete(String key);

  /**
   * Delete the object for the specified key.
   *
   * @param key key of the object to be deleted
   */
  void delete(byte[] key);
}
