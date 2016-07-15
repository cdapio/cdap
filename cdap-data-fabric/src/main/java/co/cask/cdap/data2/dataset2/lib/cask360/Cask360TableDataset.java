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
package co.cask.cdap.data2.dataset2.lib.cask360;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupMeta;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupType;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Record;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;

import com.google.common.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The Cask360Table Dataset.
 * <p>
 * Used to store and aggregate data for a set of entities from multiple sources.
 * Each entity and entity update are represented as a {@link Cask360Entity}.
 * <p>
 * Table implements a "wide row" three-dimension data model:
 *
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
 * into a six column schema of <b>(id, group, type, time, key, value)</b>.
 */
public class Cask360TableDataset extends AbstractDataset implements Cask360Table {
  private static final Logger LOG = LoggerFactory.getLogger(Cask360TableDataset.class);

  /** Meta table row key for group name to group number lookup. */
  private static final byte[] META_GROUP_NAME_ROW = new byte[] { 1 };

  /** Meta table row key for group number to group name lookup. */
  private static final byte[] META_GROUP_NAME_ROW_REV = new byte[] { 2 };

  /** Meta table row key for group number counter. */
  private static final byte[] META_GROUP_NAME_COUNTER_ROW = new byte[] { 3 };

  /** Underlying table for entity data. */
  private Table dataTable;

  /** Underlying table for meta data (mapping group names to group meta). */
  private Table metaTable;

  /** Local cache mapping group names and group numbers. */
  private Cask360GroupCache groupCache;

  /**
   * Constructs a new {@link Cask360TableDataset} from the given dataset specification
   * and underlying tables.
   *
   * @param spec
   * @param dataTable
   * @param metaTable
   */
  public Cask360TableDataset(DatasetSpecification spec, @EmbeddedDataset("data") Table dataTable,
      @EmbeddedDataset("meta") Table metaTable) {
    super(spec.getName(), dataTable, metaTable);
    this.dataTable = dataTable;
    this.metaTable = metaTable;
    this.groupCache = new Cask360GroupCache();
  }

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
   * @param entity
   */
  public void write(Cask360Entity entity) {
    write(entity.getID(), entity);
  }

  /**
   * Writes the specified {@link Cask360Entity} to the table. Additional API
   * call to support the {@link BatchWritable} interface.
   * <p>
   * See {@link #write(Cask360Entity)} for more info on the semantics of this
   * operation.
   */
  @Override
  public void write(String id, Cask360Entity entity) {
    List<byte[][][]> dataArrays = new LinkedList<byte[][][]>();
    for (Map.Entry<String, Cask360Group> entry : entity.getGroups().entrySet()) {
      Cask360Group group = entry.getValue();
      Cask360GroupMeta groupMeta = getGroupMeta(Bytes.toBytes(entry.getKey()), group.getType());
      if (group.getType() != groupMeta.getType()) {
        String msg = "Group type mismatch, attempted (" + group.getType() + ") existing (" + groupMeta.getType() + ")";
        LOG.warn(msg);
        throw new IllegalArgumentException(msg);
      }

      dataArrays.add(Cask360TableDataset.mapToArrays(group.getData().getBytesMap(groupMeta.getPrefix())));
    }
    byte[][][] dataArray = Cask360TableDataset.flattenDataArrays(dataArrays);
    this.dataTable.put(Bytes.toBytes(id), dataArray[0], dataArray[1]);
  }

  /**
   * Reads the {@link Cask360Entity} for the specified ID.
   * <p>
   * If no entity exists for the specified ID, returns null.
   *
   * @param id
   *          entity ID to read
   * @return entity data, or null if entity has no data
   */
  public Cask360Entity read(String id) {
    Row row = this.dataTable.get(Bytes.toBytes(id));
    if ((row == null) || row.isEmpty()) {
      return null;
    }
    return new Cask360Entity(id, rowToGroupMap(row));
  }

  // Group Management Internal Methods

  static class Cask360GroupCache {

    /** Local cache mapping group names to group numbers. */
    private Map<byte[], Cask360GroupMeta> groupPrefixes = null;

    /** Local cache mapping group numbers to group names. */
    private Map<byte[], Cask360GroupMeta> groupPrefixesRev = null;

    Cask360GroupCache() {
      this.groupPrefixes = new TreeMap<byte[], Cask360GroupMeta>(Bytes.BYTES_COMPARATOR);
      this.groupPrefixesRev = new TreeMap<byte[], Cask360GroupMeta>(Bytes.BYTES_COMPARATOR);
    }

    Cask360GroupMeta get(byte[] name) {
      return this.groupPrefixes.get(name);
    }

    Cask360GroupMeta get(short number) {
      return this.groupPrefixesRev.get(Cask360GroupMeta.getPrefix(number));
    }

    void put(Cask360GroupMeta meta) {
      this.groupPrefixes.put(meta.getName(), meta);
      this.groupPrefixesRev.put(meta.getPrefix(), meta);
    }
  }

  private synchronized Cask360GroupMeta getGroupMeta(byte[] name, Cask360GroupType type) {
    // Check the local cache
    Cask360GroupMeta groupMeta = this.groupCache.get(name);
    if (groupMeta != null) {
      return groupMeta;
    }
    // Check the meta table
    groupMeta = Cask360GroupMeta.fromBytes(this.metaTable.get(META_GROUP_NAME_ROW, name));
    if (groupMeta != null) {
      this.groupCache.put(groupMeta);
      return groupMeta;
    }
    // Add new group
    long groupNumber = this.metaTable.incrementAndGet(META_GROUP_NAME_COUNTER_ROW, META_GROUP_NAME_COUNTER_ROW, 1L);
    if (groupNumber > Short.MAX_VALUE) {
      throw new RuntimeException("Too many groups added (" + groupNumber + "), maximum is (" + Short.MAX_VALUE + ")");
    }
    groupMeta = new Cask360GroupMeta(name, (short) groupNumber, type);
    byte[] metaBytes = groupMeta.toBytes();
    this.metaTable.put(META_GROUP_NAME_ROW, name, metaBytes);
    this.metaTable.put(META_GROUP_NAME_ROW_REV, groupMeta.getPrefix(), metaBytes);
    this.groupCache.put(groupMeta);
    return groupMeta;
  }

  private synchronized Cask360GroupMeta getGroupMeta(short number) {
    Cask360GroupMeta groupMeta = groupCache.get(number);
    if (groupMeta != null) {
      return groupMeta;
    }
    groupMeta = Cask360GroupMeta
        .fromBytes(this.metaTable.get(META_GROUP_NAME_ROW_REV, Cask360GroupMeta.getPrefix(number)));
    if (groupMeta == null) {
      throw new RuntimeException("Invalid group number, nothing found in meta (" + number + ")");
    }
    this.groupCache.put(groupMeta);
    return groupMeta;
  }

  // Read helpers

  private Map<String, Cask360Group> rowToGroupMap(Row row) {
    Map<String, Cask360Group> ret = new TreeMap<String, Cask360Group>();
    if ((row == null) || row.isEmpty()) {
      return ret;
    }
    short groupNumber = 0;
    Cask360GroupMeta groupMeta = null;
    Cask360Group group = null;
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      byte[] column = entry.getKey();
      byte[] value = entry.getValue();
      short currentGroup = Bytes.toShort(column, 0, Bytes.SIZEOF_SHORT);
      if (currentGroup != groupNumber) {
        groupNumber = currentGroup;
        groupMeta = getGroupMeta(groupNumber);
        group = new Cask360Group(Bytes.toString(groupMeta.getName()), groupMeta.getType());
        ret.put(group.getName(), group);
      }
      group.getData().put(column, value);
    }
    return ret;
  }

  // Utility methods

  private static byte[][][] mapToArrays(Map<byte[], byte[]> data) {
    byte[][][] arrays = new byte[2][][];
    int len = data.size();
    int idx = 0;
    arrays[0] = new byte[len][];
    arrays[1] = new byte[len][];
    for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
      arrays[0][idx] = entry.getKey();
      arrays[1][idx] = entry.getValue();
      idx++;
    }
    return arrays;
  }

  private static byte[][][] flattenDataArrays(List<byte[][][]> srcArrays) {
    int count = 0;
    for (byte[][][] srcArray : srcArrays) {
      count += srcArray[0].length;
    }
    byte[][][] dataArray = new byte[2][][];
    dataArray[0] = new byte[count][];
    dataArray[1] = new byte[count][];
    int dataIdx = 0;
    for (byte[][][] srcArray : srcArrays) {
      int length = srcArray[0].length;
      for (int i = 0; i < length; i++) {
        dataArray[0][dataIdx] = srcArray[0][i];
        dataArray[1][dataIdx] = srcArray[1][i];
        dataIdx++;
      }
    }
    return dataArray;
  }

  // Implementations of RecordScannable
  // Flattens the table into (id, group, key, value) rather than a complex type

  @SuppressWarnings("serial")
  @Override
  public Type getRecordType() {
    return new TypeToken<Cask360Record>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return dataTable.getSplits();
  }

  @Override
  public RecordScanner<Cask360Record> createSplitRecordScanner(Split split) {
    return new RecordScanner<Cask360Record>() {
      private SplitReader<byte[], Row> splitReader;
      private String id;
      private Iterator<KeyValue<byte[], byte[]>> columnIterator;
      private Iterator<Cask360Record> recordIterator;

      @Override
      public void initialize(Split split) throws InterruptedException {
        this.splitReader = dataTable.createSplitReader(split);
        this.splitReader.initialize(split);
      }

      @Override
      public boolean nextRecord() throws InterruptedException {
        // Check if the value iterator is active
        if (recordIterator != null) {
          if (recordIterator.hasNext()) {
            return true;
          } else {
            recordIterator = null;
          }
        }
        if (id == null) {
          // Initial call
          if (!this.splitReader.nextKeyValue()) {
            return false;
          }
          id = Bytes.toString(this.splitReader.getCurrentKey());
          columnIterator = createColumnIterator(this.splitReader.getCurrentValue());
          return columnIterator.hasNext();
        } else if (!columnIterator.hasNext()) {
          // When current iterator is done
          if (!this.splitReader.nextKeyValue()) {
            return false;
          }
          id = Bytes.toString(this.splitReader.getCurrentKey());
          columnIterator = createColumnIterator(this.splitReader.getCurrentValue());
          return columnIterator.hasNext();
        }
        // Current iterator still has columns
        return true;
      }

      private Iterator<KeyValue<byte[], byte[]>> createColumnIterator(Row currentValue) {
        Map<byte[], byte[]> columns = currentValue.getColumns();
        List<KeyValue<byte[], byte[]>> kvList = new ArrayList<KeyValue<byte[], byte[]>>(columns.size());
        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
          kvList.add(new KeyValue<byte[], byte[]>(entry.getKey(), entry.getValue()));
        }
        return kvList.iterator();
      }

      @Override
      public Cask360Record getCurrentRecord() throws InterruptedException {
        if (recordIterator != null) {
          return recordIterator.next();
        }
        KeyValue<byte[], byte[]> kv = columnIterator.next();
        byte[] column = kv.getKey();
        short currentGroup = Bytes.toShort(column, 0, Bytes.SIZEOF_SHORT);
        Cask360GroupMeta meta = getGroupMeta(currentGroup);
        recordIterator = Cask360GroupData.newRecordIterator(id, meta, column, kv.getValue());
        return recordIterator.next();
      }

      @Override
      public void close() {
        this.splitReader.close();
      }
    };
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    return this.dataTable.createSplitReader(split);
  }
}
