package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 */
public abstract class BackedByVersionedStoreOcTableClient extends BufferingOcTableClient {
  protected static final byte[] DELETE_MARKER = new byte[0];

  public BackedByVersionedStoreOcTableClient(String name) {
    super(name);
  }

  protected static NavigableMap<byte[], byte[]> getLatestNotExcluded(
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap,
    long[] excluded) {

    // todo: for some subclasses it is ok to do changes in place...
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : rowMap.entrySet()) {
      // NOTE: versions map already sorted, first comes latest version
      // todo: not cool to rely on external implementation specifics
      for (Map.Entry<Long, byte[]> versionAndValue : column.getValue().entrySet()) {
        // NOTE: we know that excluded versions are ordered
        if (Arrays.binarySearch(excluded, versionAndValue.getKey()) < 0) {
          result.put(column.getKey(), versionAndValue.getValue());
          break;
        }
      }
    }

    return result;
  }

  protected static NavigableMap<byte[], NavigableMap<byte[], byte[]>> getLatestNotExcludedRows(
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rows,
    long[] excluded) {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap : rows.entrySet()) {
      NavigableMap<byte[], byte[]> visibleRowMap = getLatestNotExcluded(rowMap.getValue(), excluded);
      if (visibleRowMap.size() > 0) {
        result.put(rowMap.getKey(), visibleRowMap);
      }
    }

    return result;
  }

  protected static byte[] wrapDeleteIfNeeded(byte[] value) {
    return value == null ? DELETE_MARKER : value;
  }

  protected static byte[] unwrapDeleteIfNeeded(byte[] value) {
    return Arrays.equals(DELETE_MARKER, value) ? null : value;
  }

  // todo: it is in-efficient to copy maps a lot, consider merging with getLatest methods
  protected static NavigableMap<byte[], NavigableMap<byte[], byte[]>> unwrapDeletesForRows(
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows) {

    NavigableMap<byte[], NavigableMap<byte[], byte[]>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : rows.entrySet()) {
      NavigableMap<byte[], byte[]> rowMap = unwrapDeletes(row.getValue());
      if (rowMap.size() > 0) {
        result.put(row.getKey(), rowMap);
      }
    }

    return result;
  }

  // todo: it is in-efficient to copy maps a lot, consider merging with getLatest methods
  protected static NavigableMap<byte[], byte[]> unwrapDeletes(NavigableMap<byte[], byte[]> rowMap) {
    if (rowMap == null || rowMap.isEmpty()) {
      return EMPTY_ROW_MAP;
    }
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], byte[]> keyVal : rowMap.entrySet()) {
      byte[] val = unwrapDeleteIfNeeded(keyVal.getValue());
      if (val != null) {
        result.put(keyVal.getKey(), val);
      }
    }
    return result;
  }

}
