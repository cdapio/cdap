/*
 * Copyright 2015 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.timeseries;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.cube.DimensionValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Helper for serde of Fact into columnar format.
 */
public class FactCodec {
  private static final Logger LOG = LoggerFactory.getLogger(FactCodec.class);
  // current version
  private static final byte[] VERSION = new byte[] {0};

  // encoding types
  private static final String TYPE_MEASURE_NAME = "measureName";
  private static final String TYPE_DIMENSIONS_GROUP = "tagsGroup";

  private final EntityTable entityTable;

  private final int resolution;
  private final int rollTimebaseInterval;
  private final int coarseLagFactor;
  private final int coarseRoundFactor;
  // Cache for delta values.
  private final byte[][] deltaCache;

  public FactCodec(EntityTable entityTable, int resolution, int rollTimebaseInterval,
                   int coarseLagFactor, int coarseRoundFactor) {
    this.entityTable = entityTable;
    this.resolution = resolution;
    this.rollTimebaseInterval = rollTimebaseInterval;
    this.coarseLagFactor = coarseLagFactor;
    this.coarseRoundFactor = coarseRoundFactor;
    this.deltaCache = createDeltaCache(rollTimebaseInterval);
  }

  /**
   * Builds row key for write and get operations.
   * @param dimensionValues dimension values
   * @param measureName measure name
   * @param ts timestamp
   * @param now current time in seconds to calculate processing lag
   * @return row key
   */
  public byte[] createRowKey(List<DimensionValue> dimensionValues, String measureName, long ts, long now) {
    return createRowKey(dimensionValues, measureName, ts, now, (name, loader) -> loader.get());
  }
  /**
   * Builds row key for write and get operations.
   * @param dimensionValues dimension values
   * @param measureName measure name
   * @param ts timestamp
   * @param now current time in seconds to calculate processing lag
   * @param fastCache additional thread-local cache function that can be consulted before going to concurrent
   *                  cache or doing database retrieval. May call provided supplier right away if thread-local cache
   *                  is not used.
   * @return row key
   */
  public byte[] createRowKey(List<DimensionValue> dimensionValues, String measureName, long ts, long now,
                             BiFunction<EntityTable.EntityName, Supplier<Long>, Long> fastCache) {
    // "false" would write null in dimension values as "undefined"
    return createRowKey(dimensionValues, measureName, ts, false, false, now, fastCache);
  }

  /**
   * Builds start row key for scan operation.
   * @param dimensionValues dimension values
   * @param measureName measure name
   * @param ts timestamp
   * @param anyAggGroup if true, then scan matches every aggregation group; if false,
   *                    scan matches only aggregation group defined with list of dimension values
   * @return row key
   */
  public byte[] createStartRowKey(List<DimensionValue> dimensionValues, String measureName,
                                  long ts, boolean anyAggGroup) {
    // "false" would write null in dimension values as "undefined"
    return createRowKey(dimensionValues, measureName, ts, false, anyAggGroup, ts);
  }

  /**
   * Builds end row key for scan operation.
   * @param dimensionValues dimension values
   * @param measureName measure name
   * @param ts timestamp
   * @param anyAggGroup if true, then scan matches every aggregation group; if false,
   *                    scan matches only aggregation group defined with list of dimension values
   * @return row key
   */
  public byte[] createEndRowKey(List<DimensionValue> dimensionValues, String measureName,
                                long ts, boolean anyAggGroup) {
    // "false" would write null in dimension values as "undefined"
    return createRowKey(dimensionValues, measureName, ts, true, anyAggGroup, ts);
  }

  /**
   * for the given measureName return the id from entity table
   * @param measureName
   * @return entity id
   */
  public long getMeasureEntityId(String measureName) {
    return entityTable.getId(TYPE_MEASURE_NAME, measureName);
  }

  private byte[] createRowKey(List<DimensionValue> dimensionValues, String measureName, long ts, boolean stopKey,
                              boolean anyAggGroup, long now) {
    return createRowKey(dimensionValues, measureName, ts, stopKey, anyAggGroup, now, (n, cache) -> cache.get());
  }

  private byte[] createRowKey(List<DimensionValue> dimensionValues, String measureName, long ts, boolean stopKey,
                              boolean anyAggGroup, long now,
                              BiFunction<EntityTable.EntityName, Supplier<Long>, Long> fastCache) {
    // Row key format:
    // <version><encoded agg group><time base><encoded dimension1 value>...
    //                                                                 <encoded dimensionN value><encoded measure name>.
    // "+2" is for <encoded agg group> and <encoded measure name>
    byte[] rowKey =
      new byte[VERSION.length + (dimensionValues.size() + 2) * entityTable.getIdSize() + Bytes.SIZEOF_INT];

    int offset = writeVersion(rowKey);

    if (anyAggGroup) {
      offset = writeAnyEncoded(rowKey, offset, stopKey);
    } else {
      offset = writeEncodedAggGroup(dimensionValues, rowKey, offset, fastCache);
    }

    long timestamp = roundToResolution(ts, now);
    int timeBase = getTimeBase(timestamp);
    offset = Bytes.putInt(rowKey, offset, timeBase);

    for (DimensionValue dimensionValue : dimensionValues) {
      if (dimensionValue.getValue() != null) {
        // encoded value is unique within values of the dimension name
        offset = writeEncoded(dimensionValue.getName(), dimensionValue.getValue(), rowKey, offset, fastCache);
      } else {
        // todo: this is only applicable for constructing scan, throw smth if constructing key for writing data
        // writing "ANY" as a value
        offset = writeAnyEncoded(rowKey, offset, stopKey);
      }
    }

    if (measureName != null) {
      writeEncoded(TYPE_MEASURE_NAME, measureName, rowKey, offset, fastCache);
    } else {
      // todo: this is only applicable for constructing scan, throw smth if constructing key for writing data
      // writing "ANY" value
      writeAnyEncoded(rowKey, offset, stopKey);
    }
    return rowKey;
  }

  private static int writeVersion(byte[] rowKey) {
    System.arraycopy(VERSION, 0, rowKey, 0, VERSION.length);
    return VERSION.length;
  }

  /**
   * For the given rowKey, return next rowKey that has different dimensionValue at given position.
   * returns null if no next row key exist
   * @param rowKey given row key
   * @param indexOfDimValueToChange position of the dimension in a given row key to change
   * @return next row key
   */
  public byte[] getNextRowKey(byte[] rowKey, int indexOfDimValueToChange) {
    /*
    * 1) result row key length is determined by the dimensionValues to be included,
    * which is indexOfDimValueToChange plus one: the last dimensionValue will be changing
    * 2) the row key part up to the dimensionValue to be changed remains the same
    * 3) to unchanged part we append incremented value of the key part at position of dimensionValue to be changed.
    * We use Bytes.stopKeyForPrefix to increment that key part.
    * 4) if key part cannot be incremented, then we return null, indicating "no next row key exist
    */
    byte[] newRowKey = new byte[rowKey.length];
    int offset =
      VERSION.length + entityTable.getIdSize() + Bytes.SIZEOF_INT + entityTable.getIdSize() * indexOfDimValueToChange;
    byte[] nextDimValueEncoded = Bytes.stopKeyForPrefix(Arrays.copyOfRange(rowKey,
                                                                           offset, offset + entityTable.getIdSize()));
    if (nextDimValueEncoded == null) {
      return null;
    }
    System.arraycopy(rowKey, 0, newRowKey, 0, offset);
    System.arraycopy(nextDimValueEncoded, 0, newRowKey, offset, nextDimValueEncoded.length);
    return newRowKey;
  }

  public long roundToResolution(long ts, long now) {
    long rounded = (ts / resolution) * resolution;
    if (now - rounded > resolution * coarseLagFactor) {
      int coarseResolution = resolution * coarseRoundFactor;
      return (rounded / coarseResolution) * coarseResolution;
    }
    return rounded;
  }

  /**
   * create fuzzy row mask based on dimension values and measure name.
   * if dimension value/measure name is null it matches any dimension values / measures.
   * @param dimensionValues
   * @param measureName
   * @return fuzzy mask byte array
   */
  public byte[] createFuzzyRowMask(List<DimensionValue> dimensionValues, @Nullable String measureName) {
    // See createRowKey for row format info
    byte[] mask = new byte[VERSION.length + (dimensionValues.size() + 2) * entityTable.getIdSize() + Bytes.SIZEOF_INT];
    int offset = writeVersion(mask);

    // agg group encoded is always provided for fuzzy row filter
    offset = writeEncodedFixedMask(mask, offset);

    // time is defined by start/stop keys when scanning - we never include it in fuzzy filter
    offset = writeFuzzyMask(mask, offset, Bytes.SIZEOF_INT);

    for (DimensionValue dimensionValue : dimensionValues) {
      if (dimensionValue.getValue() != null) {
        offset = writeEncodedFixedMask(mask, offset);
      } else {
        offset = writeEncodedFuzzyMask(mask, offset);
      }
    }

    if (measureName != null) {
      writeEncodedFixedMask(mask, offset);
    } else {
      writeEncodedFuzzyMask(mask, offset);
    }
    return mask;
  }

  public byte[] createColumn(long ts, long now) {
    long timestamp = roundToResolution(ts, now);
    int timeBase = getTimeBase(timestamp);

    return deltaCache[(int) ((timestamp - timeBase) / resolution)];
  }

  public String getMeasureName(byte[] rowKey) {
    // last encoded is measure name
    long encoded = readEncoded(rowKey, rowKey.length - entityTable.getIdSize());
    return entityTable.getName(encoded, TYPE_MEASURE_NAME);
  }

  public List<DimensionValue> getDimensionValues(byte[] rowKey) {
    // todo: in some cases, the client knows the agg group - so to optimize we can accept is as a parameter
    // first encoded is aggregation group
    long encodedAggGroup = readEncoded(rowKey, VERSION.length);
    String aggGroup = entityTable.getName(encodedAggGroup, TYPE_DIMENSIONS_GROUP);
    if (aggGroup == null) {
      // will never happen, unless data in entity table was corrupted or deleted
      LOG.warn("Could not decode agg group: " + encodedAggGroup);
      return Collections.emptyList();
    }
    if (aggGroup.isEmpty()) {
      return Collections.emptyList();
    }

    // aggregation group is defined by list of dimension names concatenated with "." (see writeEncodedAggGroup
    // for details)
    String[] dimensionNames = aggGroup.split("\\.");

    // todo: assert count of dimension values is same as dimension names?
    List<DimensionValue> dimensions = Lists.newArrayListWithCapacity(dimensionNames.length);
    for (int i = 0; i < dimensionNames.length; i++) {
      // dimension values go right after encoded agg group and timebase (encoded as int)
      long encodedDimensionValue =
        readEncoded(rowKey, VERSION.length + entityTable.getIdSize() *  (i + 1) + Bytes.SIZEOF_INT);
      String dimensionValue = entityTable.getName(encodedDimensionValue, dimensionNames[i]);
      dimensions.add(new DimensionValue(dimensionNames[i], dimensionValue));
    }

    return dimensions;
  }

  public long getTimestamp(byte[] rowKey, byte[] column) {
    // timebase is encoded as int after the encoded agg group
    int timebase = Bytes.toInt(rowKey, VERSION.length + entityTable.getIdSize());
    // time leftover is encoded as 2 byte column name
    int leftover = Bytes.toShort(column) * resolution;

    return timebase + leftover;
  }

  static byte[][] getSplits(int aggGroupsCount) {
    // Row key format:
    // <version><encoded agg group><time base>...
    // Version is fixed. We assume agg group name is encoded with 1, ..., <aggGroupsCount>.
    int encodedIdSize = EntityTable.computeSize();
    int rowKeySize = VERSION.length + encodedIdSize;
    // NOTE: we don't need to include first split, it will be added automatically (e.g. by HBase).
    byte[][] splits = new byte[aggGroupsCount - 1][];
    for (int i = 2; i <= aggGroupsCount; i++) {
      byte[] rowKey = new byte[rowKeySize];
      int offset = writeVersion(rowKey);
      writeEncoded(rowKey, offset, i, encodedIdSize);
      splits[i - 2] = rowKey;
    }

    return splits;
  }

  private int writeEncodedAggGroup(List<DimensionValue> dimensionValues, byte[] rowKey, int offset,
                                   BiFunction<EntityTable.EntityName, Supplier<Long>, Long> fastCache) {
    // aggregation group is defined by list of dimension names
    StringBuilder sb = new StringBuilder();
    for (DimensionValue dimensionValue : dimensionValues) {
      sb.append(dimensionValue.getName()).append(".");
    }

    return writeEncoded(TYPE_DIMENSIONS_GROUP, sb.toString(), rowKey, offset, fastCache);
  }

  /**
   * @return incremented offset
   */
  private int writeEncoded(String type, String entity, byte[] destination, int offset,
                           BiFunction<EntityTable.EntityName, Supplier<Long>, Long> fastCache) {
    long id = entityTable.getId(type, entity, fastCache);
    int idSize = entityTable.getIdSize();
    return writeEncoded(destination, offset, id, idSize);
  }

  private static int writeEncoded(byte[] destination, int offset, long id, int idSize) {
    int shift = idSize;
    while (shift != 0) {
      shift--;
      destination[offset + shift] = (byte) (id & 0xff);
      id >>= 8;
    }

    return offset + idSize;
  }

  /**
   * @return incremented offset
   */
  private int writeAnyEncoded(byte[] destination, int offset, boolean stopKey) {
    // all encoded ids start with 1, so all zeroes is special case to say "any" matches
    // todo: all zeroes - should we move to entity table somehow?
    int idSize = entityTable.getIdSize();
    while (idSize != 0) {
      idSize--;
      // 0xff is the biggest byte value (according to lexographical bytes comparator we use)
      destination[offset + idSize] = stopKey ? (byte) 0xff : 0;
    }

    return offset + entityTable.getIdSize();
  }

  private int writeFuzzyMask(byte[] destination, int offset, int length) {
    int count = length;
    while (count != 0) {
      count--;
      destination[offset + count] = 1;
    }

    return offset + length;
  }

  private int writeEncodedFixedMask(byte[] destination, int offset) {
    int idSize = entityTable.getIdSize();
    while (idSize != 0) {
      idSize--;
      destination[offset + idSize] = 0;
    }

    return offset + entityTable.getIdSize();
  }

  private int writeEncodedFuzzyMask(byte[] destination, int offset) {
    int idSize = entityTable.getIdSize();
    while (idSize != 0) {
      idSize--;
      destination[offset + idSize] = 1;
    }

    return offset + entityTable.getIdSize();
  }

  private long readEncoded(byte[] bytes, int offset) {
    long id = 0;
    int idSize = entityTable.getIdSize();
    for (int i = 0; i < idSize; i++) {
      id |= (bytes[offset + i] & 0xff) << ((idSize - i - 1) * 8);
    }
    return id;
  }

  /**
   * Returns timebase computed with the table setting for the given timestamp.
   */
  private int getTimeBase(long time) {
    // We are using 4 bytes timebase for row
    long timeBase = time / rollTimebaseInterval * rollTimebaseInterval;
    Preconditions.checkArgument(timeBase < 0x100000000L, "Timestamp is too large.");
    return (int) timeBase;
  }

  private byte[][] createDeltaCache(int rollTime) {
    byte[][] deltas = new byte[rollTime + 1][];

    for (int i = 0; i <= rollTime; i++) {
      deltas[i] = Bytes.toBytes((short) i);
    }
    return deltas;
  }
}
