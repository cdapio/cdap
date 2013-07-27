package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 */
public abstract class BackedByVersionedStoreOcTableClient extends BufferringOcTableClient {
  protected static final byte[] DELETE_MARKER = new byte[0];

  public BackedByVersionedStoreOcTableClient(String name) {
    super(name);
  }

  // todo: move to "shared" place: also used by inmemory table
  protected static NavigableMap<byte[], byte[]> getLatestNotExcluded(
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap,
    long[] excluded) {
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : rowMap.entrySet()) {
      // NOTE: versions map already sorted. todo: not cool to rely on external implementation specifics
      for (Map.Entry<Long, byte[]> versionAndValue : column.getValue().entrySet()) {
        // NOTE: we know that excluded versions are ordered
        if (Arrays.binarySearch(excluded, versionAndValue.getKey()) < 0) {
          result.put(column.getKey(), versionAndValue.getValue());
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

  protected byte[] wrapDeleteIfNeeded(byte[] value) {
    return value == null ? DELETE_MARKER : value;
  }

  protected byte[] unwrapDeleteIfNeeded(byte[] value) {
    return Arrays.equals(DELETE_MARKER, value) ? null : value;
  }

  protected NavigableMap<byte[], byte[]> unwrapDeletes(NavigableMap<byte[], byte[]> rowMap) {
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
