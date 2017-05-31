/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api.dataset.lib.cask360;

import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupType;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataTime;

import com.google.gson.JsonElement;

import java.util.Map;

/**
 * Common interface that links core class {@link Cask360GroupData} with
 * underlying implementations like {@link Cask360GroupDataMap} and
 * {@link Cask360GroupDataTime}.
 */
public interface Cask360GroupDataSpec {

  /**
   * Gets the data type for this group.
   * @return the group type
   */
  Cask360GroupType getType();

  /**
   * Generates and returns the column-to-value map of data that can be
   * written into the persistent table.
   * @param prefix serialized group prefix
   * @return map of bytes to bytes that can be written as columns and values
   */
  Map<byte[], byte[]> getBytesMap(byte[] prefix);

  /**
   * Adds the specified group data to this group data.
   * <p>
   * The group types must match. Data will be joined with the same behavior as
   * {@link java.util.TreeMap#putAll(Map)}.
   * @param data group data of the same type as this group
   */
  void put(Cask360GroupData data);

  /**
   * Adds the group data as specified in the serialized column name and value.
   * @param column serialized column name
   * @param value serialized column value
   */
  void put(byte[] column, byte[] value);

  /**
   * Reads the specified JSON group representation and deserializes all data
   * into this group spec.
   * @param json JSON serialized form of a group data spec
   */
  void readJson(JsonElement json);

  /**
   * Generates and returns the JSON serialized representation of this group.
   * @return JSON seiralized form of this group data spec
   */
  JsonElement toJson();

  /**
   * Calculates and returns the total number of individual data elements in this
   * instance of a {@link Cask360GroupData}.
   * <p>
   * The number of data elements is calculated by adding the number of key-value
   * pairs if this is a map-type group and the number of timestamp-key-value pairs
   * if this is a time-type group.
   * <p>
   * This number should line up with the number of {@link Cask360Record}s
   * generated when running SQL queries on a {@link Cask360Table}.
   * @return total number of data elements in this entity group data instance
   */
  int size();

  @Override
  String toString();
}
