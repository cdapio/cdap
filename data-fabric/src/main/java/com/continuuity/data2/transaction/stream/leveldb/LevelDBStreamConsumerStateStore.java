/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class LevelDBStreamConsumerStateStore extends StreamConsumerStateStore {

  private final LevelDBOcTableCore tableCore;

  protected LevelDBStreamConsumerStateStore(StreamConfig streamConfig, LevelDBOcTableCore tableCore) {
    super(streamConfig);
    this.tableCore = tableCore;
  }

  @Override
  protected byte[] fetch(byte[] row, byte[] column) throws IOException {
    return tableCore.getRow(row, new byte[][] { column }, null, null, -1, null).get(column);
  }

  @Override
  protected void fetchAll(byte[] row, Map<byte[], byte[]> result) throws IOException {
    result.putAll(tableCore.getRow(row, null, null, null, -1, null));
  }

  @Override
  protected void fetchAll(byte[] row, byte[] columnPrefix, Map<byte[], byte[]> result) throws IOException {
    // Ignore the column prefix. Parent class would handle it.
    fetchAll(row, result);
  }

  @Override
  protected void store(byte[] row, byte[] column, byte[] value) throws IOException {
    store(row, ImmutableSortedMap.<byte[], byte[]>orderedBy(Bytes.BYTES_COMPARATOR).put(column, value).build());
  }

  @Override
  protected void store(byte[] row, Map<byte[], byte[]> values) throws IOException {
    if (values.isEmpty()) {
      return;
    }
    Map<byte[], Map<byte[], byte[]>> changes =
      ImmutableSortedMap.<byte[], Map<byte[], byte[]>>orderedBy(Bytes.BYTES_COMPARATOR)
      .put(row, values)
      .build();

    tableCore.persist(changes, Long.MAX_VALUE);
  }

  @Override
  protected void delete(byte[] row, Set<byte[]> columns) throws IOException {
    if (columns.isEmpty()) {
      return;
    }
    Map<byte[], byte[]> deleteColumns = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] column : columns) {
      deleteColumns.put(column, Bytes.EMPTY_BYTE_ARRAY);   // Value doesn't matter
    }
    Map<byte[], Map<byte[], byte[]>> undoes =
      ImmutableSortedMap.<byte[], Map<byte[], byte[]>>orderedBy(Bytes.BYTES_COMPARATOR)
      .put(row, deleteColumns)
      .build();

    tableCore.undo(undoes, Long.MAX_VALUE);
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
