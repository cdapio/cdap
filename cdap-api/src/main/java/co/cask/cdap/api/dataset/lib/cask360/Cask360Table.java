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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.table.Row;

/**
 * The Cask360Table Dataset Interface.
 * <p>
 * Used to store and aggregate data for a set of entities from multiple sources.
 * Each entity and entity update are represented as a {@link Cask360Entity}.
 * <p>
 * Table implements a "wide row" three-dimension data model:
 * <pre>
 *   {
 *     Entity1 --> {
 *                   Group  --> { Key --> Value , Key2 --> Value2 , ... } ,
 *                   Group2 --> { Key3 --> Value3 , ... } ,
 *                   GroupT --> [{'time'-->Timestamp10,'value'-->Value},{'time'-->Timestamp9,'value'-->Value2},...],
 *                   ...
 *                 } ,
 *     Entity2 --> {
 *                   Group  --> { Key --> ValueA , KeyX --> ValueX , ... } ,
 *                   GroupY --> { KeyA --> Value B , ... } ,
 *                   Group4 --> [{'time'-->Timestamp8,'value'-->ValueN},{'time'-->Timestamp3,'value'-->Value3},...],
 *                   ...
 *                 } ,
 *     ...
 *   }
 * </pre>
 * <p>
 * That is, the table stores a set of Entities, uniquely represented by an ID.
 * Each Entity can contain one or more Groups of data. Each Group of data has a
 * name and a type. The following are the types of Groups supported:
 * <ul>
 * <li><b>Map</b> is a sorted map of string keys to string values</li>
 * <li><b>Time</b> is a sorted list of long timestamps to string values</li>
 * </ul>
 * <p>
 * There are no hard limits on the number of entities, groups, key-values. As a
 * general guideline, this table can easily scale to:
 * <ul>
 * <li>Hundreds of Groups per Entity</li>
 * <li>Thousands of Key-Values per Entity</li>
 * <li>Billions of Entities (limited by cluster size)</li>
 * </ul>
 * There are no limits or restrictions about the existence of the same groups in
 * different entities. Each entity could have completely distinct groups,
 * although this is not a common pattern. However, it may be common that some
 * entities may have a group that others do not, which is also permitted. For
 * example, in the above <b>Entity1</b> and <b>Entity2</b> both contain data for
 * <b>Group</b> but one contains data for <b>Group2</b> and the other for
 * <b>GroupY</b>.
 * <p>
 * The SQL Table schema exposed by this dataset flattens the nested structure
 * into a six column schema of <b>(id, group, type, time, key, value)</b> as
 * defined in {@link Cask360Record}.
 */
public interface Cask360Table extends Dataset, RecordScannable<Cask360Record>,
BatchWritable<String, Cask360Entity>, BatchReadable<byte[], Row> {

  /**
   * Writes the specified {@link Cask360Entity} to the table.
   * <p>
   * Operation is in the style of an "upsert", for both the entity and entity
   * data. If the entity does not already exist, the specified entity and entity
   * data will be inserted. If the entity already exists, the specified entity
   * data will be "upserted" into the existing entity data. For groups that do
   * not exist, the entire group will be inserted. For groups that do exist,
   * each key will be upserted. For keys that do not exist, the specified key
   * and value will be inserted. For keys that do exist, the existing value will
   * be updated with the specified value.
   * <p>
   * This operation currently never fails for data modeling reasons and performs
   * no type or consistency checks across groups or entities.
   *
   * @param entity the entity to write
   */
  void write(Cask360Entity entity);

  /**
   * Writes the specified {@link Cask360Entity} to the table. Additional API
   * call to support the {@link BatchWritable} interface.
   * <p>
   * See {@link #write(Cask360Entity)} for more info on the semantics of this
   * operation.
   *
   * @param id
   *          the id of the entity
   * @param entity
   *          the entity to write
   */
  @Override
  void write(String id, Cask360Entity entity);

  /**
   * Reads the {@link Cask360Entity} for the specified ID.
   * <p>
   * If no entity exists for the specified ID, returns null.
   *
   * @param id
   *          entity ID to read
   * @return entity data, or null if entity has no data
   */
  Cask360Entity read(String id);
}
